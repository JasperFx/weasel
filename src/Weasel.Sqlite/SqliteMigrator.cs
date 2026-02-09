using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.Sqlite;

public class SqliteMigrator: Migrator
{
    public SqliteMigrator(): base(SqliteProvider.Instance.DefaultDatabaseSchemaName)
    {
    }

    public override IDatabaseProvider Provider => SqliteProvider.Instance;

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
    }

    public override bool MatchesConnection(DbConnection connection)
    {
        return connection is Microsoft.Data.Sqlite.SqliteConnection;
    }

    /// <summary>
    ///     Write out a templated SQL script with all rules
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="writeStep">A continuation to write the inner SQL</param>
    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
    {
        if (IsTransactional)
        {
            writer.WriteLine("BEGIN TRANSACTION;");
            writer.WriteLine();
        }

        writeStep(this, writer);

        if (IsTransactional)
        {
            writer.WriteLine();
            writer.WriteLine("COMMIT;");
        }
    }

    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        // SQLite doesn't support CREATE SCHEMA like PostgreSQL
        // SQLite only uses the "main" schema - no schema creation needed
    }

    protected override async Task executeDelta(
        SchemaMigration migration,
        DbConnection conn,
        AutoCreate autoCreate,
        IMigrationLogger logger,
        CancellationToken ct = default
    )
    {
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
        // SQLite CLI command to read script file
        return $".read {scriptName}";
    }

    public override void AssertValidIdentifier(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new InvalidOperationException($"SQLite identifier cannot be empty or whitespace");
        }

        // SQLite is quite permissive with identifier names, but we should check for basic issues
        // SQLite allows up to 1073741824 characters, but that's impractical
        // We'll use a reasonable limit similar to other databases
        if (name.Length > 255)
        {
            throw new InvalidOperationException($"SQLite identifier '{name}' is too long ({name.Length} characters). Maximum recommended length is 255.");
        }
    }

    private static async Task executeCommand(DbConnection conn, IMigrationLogger logger, StringWriter writer, CancellationToken ct = default)
    {
        var cmd = conn.CreateCommand(writer.ToString());
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

    public override IDatabaseWithTables CreateDatabase(DbConnection connection)
    {
        if (connection is not Microsoft.Data.Sqlite.SqliteConnection)
        {
            throw new ArgumentException("Expected SqliteConnection", nameof(connection));
        }

        var builder = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder(connection.ConnectionString);
        return new DatabaseWithTables(builder.DataSource ?? "sqlite", connection.ConnectionString);
    }
}
