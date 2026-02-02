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
        // SQLite uses ATTACH DATABASE for multiple schemas
        // The "main" schema is the default database and doesn't need to be created
        foreach (var schemaName in schemaNames.Where(s => s != "main"))
        {
            writer.WriteLine(CreateSchemaStatementFor(schemaName));
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

    private static async Task createSchemas(
        SchemaMigration migration,
        DbConnection conn,
        IMigrationLogger logger,
        CancellationToken ct = default)
    {
        var writer = new StringWriter();

        if (migration.Schemas.Any())
        {
            new SqliteMigrator().WriteSchemaCreationSql(migration.Schemas, writer);
            if (writer.ToString().Trim().IsNotEmpty())
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

    public static string CreateSchemaStatementFor(string schemaName)
    {
        // SQLite schemas are separate database files attached to the connection
        // In practice, most SQLite usage sticks with the main database
        // For multi-schema support, we would use: ATTACH DATABASE 'filename.db' AS schemaName
        // However, this requires a physical file path which we don't have here
        // So we'll return an empty statement and rely on the consumer to handle schema creation
        return $"-- SQLite schema '{schemaName}': Use ATTACH DATABASE 'path/to/{schemaName}.db' AS {schemaName};";
    }
}
