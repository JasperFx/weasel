using System.Data.Common;
using System.Text;
using JasperFx;
using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.SqlServer;

public class SqlServerMigrator: Migrator
{
    private const string BeginScript = @"DO $$
BEGIN";

    private const string EndScript = @"END
$$;
";

    public SqlServerMigrator(): base(SqlServerProvider.Instance.DefaultDatabaseSchemaName)
    {
    }

    public override bool MatchesConnection(DbConnection connection)
    {
        return connection is SqlConnection;
    }

    public override ValueTask ReleaseConnectionPoolAsync(DbConnection connection, CancellationToken ct = default)
    {
        if (connection is SqlConnection sql)
        {
            SqlConnection.ClearPool(sql);
        }

        return ValueTask.CompletedTask;
    }

    public override bool IsTransientConnectionFailure(Exception exception)
    {
        foreach (var e in ExceptionChain.Flatten(exception))
        {
            if (e is SqlException sql && sql.Errors.Cast<SqlError>().Any(x => IsTransientConnectionError(x.Number)))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    ///     True for the SQL Server error numbers that unambiguously mean "could not give you a connection
    ///     right now, try again" rather than "your migration is wrong" (weasel#356) -- the on-premises
    ///     connection-limit error plus the Azure SQL resource/throttling set:
    ///     <list type="bullet">
    ///         <item><c>17809</c> could not connect: too many user connections (on-premises)</item>
    ///         <item><c>40197</c>, <c>40501</c> service is busy / error processing the request</item>
    ///         <item><c>40613</c> database is currently unavailable</item>
    ///         <item><c>10928</c>, <c>10929</c> resource limits reached</item>
    ///         <item><c>49918</c>, <c>49919</c>, <c>49920</c> not enough resources to process the request</item>
    ///     </list>
    ///     The exclusions matter more than the inclusions here, because a false positive re-runs a
    ///     migration that actually failed:
    ///     <list type="bullet">
    ///         <item>
    ///             <c>-2</c> is <b>not</b> here despite being the classic "timeout expired": SqlClient
    ///             raises it for <i>command</i> timeouts too, so retrying it would silently re-run a DDL
    ///             statement that merely exceeded CommandTimeout (a slow CREATE INDEX on a large table)
    ///             three times over -- exactly what this predicate promises not to do.
    ///         </item>
    ///         <item>
    ///             <c>10053</c>/<c>10054</c>/<c>233</c>/<c>64</c>, likewise: a transport-level drop is
    ///             reported the same way whether it happened while connecting or midway through a
    ///             statement, so they cannot be distinguished from a half-applied migration.
    ///         </item>
    ///         <item><c>1205</c> deadlock is a statement conflict, handled at the statement level.</item>
    ///         <item><c>18456</c> login failed is a credential problem that will never clear.</item>
    ///     </list>
    ///     Erring narrow is the safe direction: a missed code just means no retry, i.e. today's behavior.
    ///     Pure over the error number so it is unit-testable without constructing a
    ///     <see cref="SqlException" />.
    /// </summary>
    internal static bool IsTransientConnectionError(int number)
    {
        return number is 10928 or 10929 or 17809 or 40197 or 40501 or 40613 or 49918 or 49919 or 49920;
    }

    public override IDatabaseProvider Provider => SqlServerProvider.Instance;

    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
    {
        writeStep(this, writer);
    }

    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames) writer.WriteLine(CreateSchemaStatementFor(schemaName));
    }

    public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames)
        {
            writer.WriteLine($@"IF EXISTS ( SELECT  *
                    FROM    sys.schemas
                    WHERE   name = N'{schemaName}' )
        EXEC('DROP SCHEMA [{schemaName}]');
");
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
        return $":r {scriptName}";
    }

    public override void AssertValidIdentifier(string name)
    {
        // Nothing yet
    }

    private static async Task createSchemas(
        SchemaMigration migration,
        DbConnection conn,
        IMigrationLogger logger,
        CancellationToken ct = default)
    {
        var writer = new StringWriter();

        if (migration.Schemas.Any())
        {
            new SqlServerMigrator().WriteSchemaCreationSql(migration.Schemas, writer);
            if (writer.ToString().Trim().IsNotEmpty()) // Cheesy way of knowing if there is any delta
            {
                await executeCommand(conn, logger, writer, ct).ConfigureAwait(false);
            }
        }
    }

    private static async Task executeCommand(DbConnection conn, IMigrationLogger logger, StringWriter writer, CancellationToken ct = default)
    {
        var cmd = conn.CreateCommand(writer.ToString());
        logger.SchemaChange(cmd.CommandText);

        try
        {
            await cmd
                .ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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

    public static string CreateSchemaStatementFor(string schemaName)
    {
        return $@"
IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'{schemaName}' )
    EXEC('CREATE SCHEMA [{schemaName}]');

";
    }

    public override async Task EnsureDatabaseExistsAsync(DbConnection connection, CancellationToken ct = default)
    {
        var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
        var databaseName = builder.InitialCatalog;

        if (string.IsNullOrEmpty(databaseName))
        {
            throw new ArgumentException("The connection string does not specify a database name (Initial Catalog).");
        }

        builder.InitialCatalog = "master";
        await using var adminConn = new SqlConnection(builder.ConnectionString);
        await adminConn.OpenAsync(ct).ConfigureAwait(false);

        var cmd = adminConn.CreateCommand();
        cmd.CommandText = "SELECT DB_ID(@name)";
        var param = cmd.CreateParameter();
        param.ParameterName = "@name";
        param.Value = databaseName;
        cmd.Parameters.Add(param);

        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);

        if (result is null or DBNull)
        {
            var createCmd = adminConn.CreateCommand();
            createCmd.CommandText = $"CREATE DATABASE [{databaseName}]";
            await createCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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

        if (resetIdentity)
        {
            foreach (var table in tables)
            {
                sb.AppendLine($"BEGIN TRY DBCC CHECKIDENT('{table.QualifiedName}', RESEED, 0); END TRY BEGIN CATCH END CATCH;");
            }
        }

        return sb.ToString();
    }

    public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null)
    {
        if (connection is not SqlConnection)
        {
            throw new ArgumentException("Expected SqlConnection", nameof(connection));
        }

        var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
        return new DatabaseWithTables(identifier ?? builder.InitialCatalog ?? "weasel", connection.ConnectionString);
    }
}
