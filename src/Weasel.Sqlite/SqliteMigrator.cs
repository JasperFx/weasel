using System.Data.Common;
using System.Text;
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

    public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        // SQLite doesn't support dropping schemas
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

    /// <summary>
    ///     The character SQLite delimits identifiers with. <see cref="SchemaUtils.QuoteName" /> does double
    ///     an embedded <c>"</c>, but only quotes at all for reserved keywords, spaces, dashes and leading
    ///     digits -- an ordinary-looking name carrying a quote is written out raw.
    /// </summary>
    private const string UnsafeIdentifierCharacters = "\"";

    /// <summary>
    ///     SQLite itself allows identifiers up to 1073741824 characters, which is not a useful limit; this
    ///     is the practical one Weasel enforces, in line with the other providers.
    /// </summary>
    public int MaxIdentifierLength { get; set; } = 255;

    /// <summary>
    ///     Validates a database object name before it is written into DDL. See
    ///     <see cref="IdentifierValidation" /> for why each rule is here; before weasel#416 this checked
    ///     null/whitespace and length only.
    /// </summary>
    /// <remarks>
    ///     The single quote matters here because SQLite's introspection path interpolates the table name
    ///     into string literals -- <c>WHERE name = '...'</c> against <c>sqlite_master</c> and
    ///     <c>pragma_table_info('...')</c> in <c>Table.FetchExisting</c>.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The name cannot be safely written into DDL.</exception>
    public override void AssertValidIdentifier(string name)
    {
        AssertValidLocalIdentifier(name);

        if (name.Length > MaxIdentifierLength)
        {
            throw new InvalidOperationException(
                $"SQLite identifier '{name}' is too long ({name.Length} characters). Maximum recommended length is {MaxIdentifierLength}.");
        }
    }

    /// <summary>
    ///     The safety half of <see cref="AssertValidIdentifier" />, without the length limit.
    /// </summary>
    /// <remarks>
    ///     A column, primary key or check constraint name is only ever emitted inside its own
    ///     table's DDL and is never addressed by name afterwards, and the delta comparison already
    ///     reads both sides through TruncatedNameIdentifier so a name SQLite truncated still
    ///     matches. Refusing to create one would reject schemas the rest of Weasel handles
    ///     (weasel#485). The safety rules still apply in full.
    /// </remarks>
    public override void AssertValidLocalIdentifier(string name)
    {
        var problem = IdentifierValidation.FindProblem(name, UnsafeIdentifierCharacters);
        if (problem != null)
        {
            throw new InvalidOperationException($"SQLite identifier '{name}' is not valid because {problem}.");
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

    /// <summary>
    ///     No-op for SQLite. SQLite databases are automatically created when the connection is opened.
    /// </summary>
    public override Task EnsureDatabaseExistsAsync(DbConnection connection, CancellationToken ct = default)
    {
        return Task.CompletedTask;
    }

    public override string GenerateDeleteAllSql(IReadOnlyList<DbObjectName> tables, bool resetIdentity = true)
    {
        if (tables.Count == 0)
        {
            return string.Empty;
        }

        var sb = new StringBuilder();
        var names = tables.Select(SqliteObjectName.From).ToList();

        foreach (var table in names)
        {
            sb.AppendLine($"DELETE FROM {qualify(table.Schema, table.Name)};");
        }

        if (resetIdentity)
        {
            foreach (var group in names.GroupBy(x => x.Schema, StringComparer.OrdinalIgnoreCase))
            {
                var literals = string.Join(", ", group.Select(t => $"'{SchemaUtils.EscapeLiteral(t.Name)}'"));
                sb.AppendLine($"DELETE FROM {qualify(group.Key, "sqlite_sequence")} WHERE name IN ({literals});");
            }
        }

        return sb.ToString();
    }

    /// <summary>
    ///     Names the schema even for <c>main</c>, which the rest of Weasel.Sqlite leaves bare. DML
    ///     resolves an unqualified name against <c>temp</c> first, so a temp table silently takes the
    ///     delete meant for the main one; <c>sqlite_sequence</c> is per-database for the same reason.
    ///     See <c>delete_all_data_empties_main_even_when_a_temp_table_shadows_it</c>.
    /// </summary>
    private static string qualify(string schema, string name)
    {
        var owner = schema.IsEmpty() ? SqliteProvider.Instance.DefaultDatabaseSchemaName : schema;
        return $"{SchemaUtils.QuoteName(owner)}.{SchemaUtils.QuoteName(name)}";
    }

    public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null)
    {
        if (connection is not Microsoft.Data.Sqlite.SqliteConnection)
        {
            throw new ArgumentException("Expected SqliteConnection", nameof(connection));
        }

        var builder = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder(connection.ConnectionString);
        return new DatabaseWithTables(identifier ?? builder.DataSource ?? "sqlite", connection.ConnectionString);
    }
}
