using System.Data.Common;
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

    public override IDatabaseProvider Provider => SqlServerProvider.Instance;

    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
    {
        writeStep(this, writer);
    }

    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames) writer.WriteLine(CreateSchemaStatementFor(schemaName));
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

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
    }

    public override IDatabaseWithTables CreateDatabase(DbConnection connection)
    {
        if (connection is not SqlConnection)
        {
            throw new ArgumentException("Expected SqlConnection", nameof(connection));
        }

        var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
        return new DatabaseWithTables(builder.InitialCatalog ?? "weasel", connection.ConnectionString);
    }
}
