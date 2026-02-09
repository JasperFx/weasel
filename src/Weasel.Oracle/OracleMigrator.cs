using System.Data.Common;
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

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
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
