using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using MySqlConnector;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.MySql;

public class MySqlMigrator: Migrator
{
    public MySqlMigrator(): base(MySqlProvider.Instance.DefaultDatabaseSchemaName)
    {
    }

    public override bool MatchesConnection(DbConnection connection)
    {
        return connection is MySqlConnection;
    }

    public override IDatabaseProvider Provider => MySqlProvider.Instance;

    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
    {
        writeStep(this, writer);
    }

    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames)
        {
            writer.WriteLine(CreateDatabaseStatementFor(schemaName));
        }
    }

    protected override async Task executeDelta(
        SchemaMigration migration,
        DbConnection conn,
        AutoCreate autoCreate,
        IMigrationLogger logger,
        CancellationToken ct = default)
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
        return $"source {scriptName}";
    }

    public override void AssertValidIdentifier(string name)
    {
        // MySQL identifiers can be up to 64 characters
        if (name.Length > 64)
        {
            throw new ArgumentException($"MySQL identifier '{name}' exceeds the 64 character limit.");
        }
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
            new MySqlMigrator().WriteSchemaCreationSql(migration.Schemas, writer);
            if (writer.ToString().Trim().IsNotEmpty())
            {
                await executeCommand(conn, logger, writer, ct).ConfigureAwait(false);
            }
        }
    }

    private static async Task executeCommand(DbConnection conn, IMigrationLogger logger, StringWriter writer, CancellationToken ct = default)
    {
        var sql = writer.ToString();

        // Split on semicolons and execute each statement
        var statements = sql.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);

        foreach (var statement in statements)
        {
            var trimmed = statement.Trim();
            if (trimmed.IsEmpty()) continue;

            var cmd = conn.CreateCommand();
            cmd.CommandText = trimmed;
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

    public static string CreateDatabaseStatementFor(string databaseName)
    {
        return $"CREATE DATABASE IF NOT EXISTS `{databaseName}`;";
    }

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
    }

    public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null)
    {
        if (connection is not MySqlConnection)
        {
            throw new ArgumentException("Expected MySqlConnection", nameof(connection));
        }

        var builder = new MySqlConnectionStringBuilder(connection.ConnectionString);
        return new DatabaseWithTables(identifier ?? builder.Database ?? "weasel", connection.ConnectionString);
    }
}
