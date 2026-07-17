using System.Data.Common;
using System.Text;
using JasperFx;
using JasperFx.Core;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.Oracle;

public class OracleMigrator: Migrator
{
    public OracleMigrator(): base(OracleProvider.Instance.DefaultDatabaseSchemaName)
    {
    }

    public override bool MatchesConnection(DbConnection connection)
    {
        return connection is OracleConnection;
    }

    public override ValueTask ReleaseConnectionPoolAsync(DbConnection connection, CancellationToken ct = default)
    {
        if (connection is OracleConnection oracle)
        {
            OracleConnection.ClearPool(oracle);
        }

        return ValueTask.CompletedTask;
    }

    public override bool IsTransientConnectionFailure(Exception exception)
    {
        foreach (var e in ExceptionChain.Flatten(exception))
        {
            if (e is OracleException oracle && IsTransientConnectionError(oracle.Number)) return true;
        }

        return false;
    }

    /// <summary>
    ///     True for the Oracle errors that mean the instance or its listener would not take a new connection
    ///     right now (weasel#356):
    ///     <list type="bullet">
    ///         <item><c>ORA-00020</c> maximum number of processes exceeded</item>
    ///         <item><c>ORA-12516</c> listener could not find an available handler</item>
    ///         <item><c>ORA-12518</c> listener could not hand off the client connection</item>
    ///         <item><c>ORA-12520</c> listener could not find an available handler for the server type</item>
    ///     </list>
    ///     Excludes <c>ORA-01017</c> (invalid credentials), which never clears on a retry, and deliberately
    ///     excludes <c>ORA-12537</c> (connection closed) and <c>ORA-12570</c> (unexpected packet read
    ///     error): those are raised when an <i>established</i> session is interrupted, which is
    ///     indistinguishable from a migration dropped midway through. Re-running it would replay DDL that
    ///     may already have committed and surface a bogus <c>ORA-00955 name is already used</c> in place of
    ///     the real network error. Pure over the error number so it is unit-testable without constructing an
    ///     <see cref="OracleException" />.
    /// </summary>
    internal static bool IsTransientConnectionError(int number)
    {
        return number is 20 or 12516 or 12518 or 12520;
    }

    public override IDatabaseProvider Provider => OracleProvider.Instance;

    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
    {
        writeStep(this, writer);
    }

    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames)
        {
            writer.WriteLine(CreateSchemaStatementFor(schemaName));
            writer.WriteLine("/"); // SQL*Plus script terminator for PL/SQL blocks
        }
    }

    public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames)
        {
            writer.WriteLine($@"DECLARE
BEGIN
    EXECUTE IMMEDIATE 'DROP USER {schemaName} CASCADE';
END;");
            writer.WriteLine("/");
        }
    }

    protected override async Task executeDelta(
        SchemaMigration migration,
        DbConnection conn,
        AutoCreate autoCreate,
        IMigrationLogger logger,
        CancellationToken ct = default
    )
    {
        await createSchemas(migration, conn, logger, ct).ConfigureAwait(false);

        foreach (var delta in migration.Deltas)
        {
            var writer = new StringWriter();
            WriteUpdate(writer, delta);

            if (writer.ToString().Trim().IsNotEmpty())
            {
                await executeCommand(conn, logger, writer, ct).ConfigureAwait(false);
            }
        }
    }

    public override string ToExecuteScriptLine(string scriptName)
    {
        return $"@{scriptName}";
    }

    public override void AssertValidIdentifier(string name)
    {
        if (name.Length > 128)
        {
            throw new InvalidOperationException($"Oracle identifiers cannot exceed 128 characters. '{name}' is {name.Length} characters.");
        }
    }

    private static async Task createSchemas(
        SchemaMigration migration,
        DbConnection conn,
        IMigrationLogger logger,
        CancellationToken ct = default)
    {
        // Oracle requires each PL/SQL block to be executed separately
        // (not batched like PostgreSQL or SQL Server)
        foreach (var schemaName in migration.Schemas)
        {
            var sql = CreateSchemaStatementFor(schemaName);
            var cmd = conn.CreateCommand(sql);
            logger.SchemaChange(cmd.CommandText);

            try
            {
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                if (logger is DefaultMigrationLogger)
                {
                    throw;
                }

                logger.OnFailure(cmd, e);
            }
        }
    }

    private static async Task executeCommand(DbConnection conn, IMigrationLogger logger, StringWriter writer, CancellationToken ct = default)
    {
        var sql = writer.ToString();

        // Oracle can only execute one statement at a time
        // Split by "/" which is the Oracle statement separator for PL/SQL blocks
        var statements = sql.Split(new[] { "\n/\n", "\n/", "/\n" }, StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .ToArray();

        foreach (var statement in statements)
        {
            var cmd = conn.CreateCommand(statement);
            logger.SchemaChange(cmd.CommandText);

            try
            {
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                if (logger is DefaultMigrationLogger)
                {
                    throw;
                }

                logger.OnFailure(cmd, e);
            }
        }
    }

    public static string CreateSchemaStatementFor(string schemaName)
    {
        // Note: Do not include "/" terminator - it's only for SQL*Plus scripts, not programmatic execution
        return $@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM all_users WHERE username = '{schemaName.ToUpperInvariant()}';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER {schemaName} IDENTIFIED BY ""temp_password"" QUOTA UNLIMITED ON USERS';
        EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO {schemaName}';
    END IF;
END;";
    }

    public override async Task EnsureDatabaseExistsAsync(DbConnection connection, CancellationToken ct = default)
    {
        var builder = new OracleConnectionStringBuilder(connection.ConnectionString);
        var schemaName = builder.UserID;

        if (string.IsNullOrEmpty(schemaName))
        {
            throw new ArgumentException("The connection string does not specify a User ID (schema name).");
        }

        var wasOpen = connection.State == System.Data.ConnectionState.Open;
        if (!wasOpen)
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
        }

        try
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = CreateSchemaStatementFor(schemaName);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            if (!wasOpen)
            {
                await connection.CloseAsync().ConfigureAwait(false);
            }
        }
    }

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
    }

    public override string GenerateDeleteAllSql(IReadOnlyList<DbObjectName> tables, bool resetIdentity = true)
    {
        if (tables.Count == 0)
        {
            return string.Empty;
        }

        var sb = new StringBuilder();

        foreach (var table in tables)
        {
            sb.AppendLine($"DELETE FROM {table.QualifiedName};");
        }

        return sb.ToString();
    }

    public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null)
    {
        if (connection is not OracleConnection)
        {
            throw new ArgumentException("Expected OracleConnection", nameof(connection));
        }

        var builder = new OracleConnectionStringBuilder(connection.ConnectionString);
        return new DatabaseWithTables(identifier ?? builder.UserID ?? "weasel", connection.ConnectionString);
    }
}
