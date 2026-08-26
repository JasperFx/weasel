using System.Data;
using System.Data.Common;
using MySqlConnector;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.MySql.Views;

/// <summary>
///     A MySQL view.
/// </summary>
/// <remarks>
///     <para>
///         MySQL is the one provider that does not store the view text you gave it. It parses the
///         body and re-renders it, fully qualifying every table, aliasing every column, and
///         parenthesizing every predicate. Submit
///         <c>select id, name from probe_src where qty > 0</c> and
///         <c>information_schema.VIEWS.VIEW_DEFINITION</c> hands back
///         <c>select `db`.`probe_src`.`id` AS `id`,`db`.`probe_src`.`name` AS `name` from
///         `db`.`probe_src` where (`db`.`probe_src`.`qty` > 0)</c>.
///     </para>
///     <para>
///         SQL Server, SQLite and Oracle all store the submitted text verbatim, which is why
///         whitespace-insensitive comparison works for them. It cannot work here: no amount of
///         local normalization bridges that rewrite, so comparing the caller's SQL against the
///         stored text reports <c>Update</c> on every check forever — the permanent-drift disease
///         weasel#445 and weasel#446 were about.
///     </para>
///     <para>
///         So the expected SQL is canonicalized by the only thing that can canonicalize it: the
///         server. <see cref="CanonicalizeAsync" /> creates a throwaway view from the caller's SQL,
///         reads its <c>VIEW_DEFINITION</c>, drops it, and compares that against the stored text.
///         This is exact rather than approximate — a view's own name never appears in its
///         <c>VIEW_DEFINITION</c>, so the probe's rendering is byte-identical to what the real view
///         would produce — and MySQL's canonicalization is idempotent, so the comparison is stable.
///     </para>
///     <para>
///         The probe runs on the apply path and the assert path alike, so a view never reports one
///         thing to <c>ApplyAllConfiguredChangesToDatabaseAsync</c> and another to
///         <c>AssertDatabaseMatchesConfigurationAsync</c>. The price is that both paths need
///         CREATE VIEW / DROP VIEW permission. The result is cached per instance, so it is one
///         probe per view per process rather than one per check.
///     </para>
/// </remarks>
public class View: ViewBase
{
    private string? _canonicalExpected;

    /// <summary>
    ///     Captured in <see cref="ConfigureQueryCommand" /> so <see cref="CreateDeltaAsync" /> —
    ///     which is handed a reader and nothing else — can reach the server it is comparing
    ///     against. The delta runs while the batch reader is still open, so the probe cannot
    ///     borrow this connection; it clones it.
    /// </summary>
    private MySqlConnection? _connection;

    public View(string viewName, string viewSql)
        : this(
            viewName != null
                ? DbObjectName.Parse(MySqlProvider.Instance, viewName)
                : throw new ArgumentNullException(nameof(viewName)),
            viewSql)
    {
    }

    public View(DbObjectName identifier, string viewSql): base(identifier, viewSql)
    {
    }

    /// <inheritdoc />
    protected override DbObjectName WithSchema(string schemaName)
        => new MySqlObjectName(schemaName, Identifier.Name);

    /// <inheritdoc />
    protected override Migrator GetDefaultMigratorForBasicSql()
        => new MySqlMigrator { Formatting = SqlFormatting.Concise };

    /// <summary>
    ///     <c>CREATE OR REPLACE VIEW</c>: idempotent, and one statement. MySQL's migrator splits a
    ///     delta's SQL on semicolons to execute it, so a body carrying a semicolon inside a string
    ///     literal is safest when the delta is a single statement to begin with.
    /// </summary>
    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        var body = ViewSql.TrimEnd().TrimEnd(';');

        writer.WriteLine($"CREATE OR REPLACE VIEW {QualifiedName(Identifier)} AS {body}");
    }

    public override void WriteDropStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine($"DROP VIEW IF EXISTS {QualifiedName(Identifier)}");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        _connection ??= builder.Command.Connection as MySqlConnection;

        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

        builder.Append(
            $"SELECT view_definition FROM information_schema.VIEWS WHERE table_schema = @{schemaParam} AND table_name = @{nameParam};");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readDefinitionAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new ViewDelta(this, SchemaPatchDifference.Create);
        }

        if (_connection == null)
        {
            throw new InvalidOperationException(
                $"Cannot compare MySQL view {Identifier} without a connection. MySQL rewrites a view's "
                + "definition when it stores it, so the expected SQL has to be canonicalized by the server "
                + "before the two can be compared; ConfigureQueryCommand is where that connection is "
                + "captured, so it has to have run first.");
        }

        var expected = await CanonicalizeAsync(_connection, ct).ConfigureAwait(false);

        return NormalizeSql(existing) == NormalizeSql(expected)
            ? new ViewDelta(this, SchemaPatchDifference.None)
            : new ViewDelta(this, SchemaPatchDifference.Update);
    }

    public async Task<View?> FetchExistingAsync(MySqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var definition = await readDefinitionAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return definition == null ? null : new View(Identifier, definition);
    }

    public static Task<View?> FetchExistingAsync(MySqlConnection conn, DbObjectName identifier,
        CancellationToken ct = default)
        => new View(identifier, "select 1").FetchExistingAsync(conn, ct);

    public async Task<bool> ExistsInDatabaseAsync(MySqlConnection conn, CancellationToken ct = default)
        => await FetchExistingAsync(conn, ct).ConfigureAwait(false) != null;

    /// <summary>
    ///     Render this view's SQL the way MySQL would store it, by having MySQL store it: create a
    ///     throwaway view from the body, read its <c>VIEW_DEFINITION</c>, drop it.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The probe runs on its own connection — cloned from <paramref name="template" />,
    ///         which carries the credentials over — because the caller is typically holding an open
    ///         reader on that one.
    ///     </para>
    ///     <para>
    ///         The result is cached: the caller's SQL does not change over the life of the object,
    ///         and neither does the server's rendering of it.
    ///     </para>
    /// </remarks>
    public async Task<string> CanonicalizeAsync(MySqlConnection template, CancellationToken ct = default)
    {
        if (_canonicalExpected != null)
        {
            return _canonicalExpected;
        }

        var probeName = $"weasel_view_probe_{Guid.NewGuid():N}";
        var probe = $"{SchemaUtils.QuoteName(Identifier.Schema)}.{SchemaUtils.QuoteName(probeName)}";
        var body = ViewSql.TrimEnd().TrimEnd(';');

        // The probe has to see the same default database as the connection the real view will be
        // created on: an unqualified name in a view body resolves against the session's default
        // schema at creation time, and MySQL bakes that resolution into the stored definition. A
        // probe run with a different default would canonicalize to a different table.
        var builder = new MySqlConnectionStringBuilder(template.ConnectionString)
        {
            Database = template.Database
        };

        await using var conn = template.CloneWith(builder.ConnectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        try
        {
            await conn.CreateCommand($"CREATE OR REPLACE VIEW {probe} AS {body}")
                .ExecuteNonQueryAsync(ct).ConfigureAwait(false);

            var rendered = await conn
                .CreateCommand(
                    "SELECT view_definition FROM information_schema.VIEWS WHERE table_schema = @schema AND table_name = @name")
                .With("schema", Identifier.Schema)
                .With("name", probeName)
                .ExecuteScalarAsync(ct).ConfigureAwait(false);

            // Nothing came back only if the CREATE silently did not take, which MySQL does not do;
            // falling back to the caller's own SQL keeps the comparison defined rather than null.
            _canonicalExpected = rendered as string ?? body;
            return _canonicalExpected;
        }
        catch (MySqlException e) when (e.Number is 1142 or 1044)
        {
            throw new InvalidOperationException(
                $"Weasel needs CREATE VIEW and DROP VIEW permission on schema '{Identifier.Schema}' to compare "
                + $"view {Identifier}. MySQL rewrites a view's definition when it stores it, so the only way to "
                + "know whether the configured SQL matches what is in the database is to have the server render "
                + "it. This applies to AssertDatabaseMatchesConfigurationAsync as well as to the apply path, so "
                + "that a view does not report differently depending on which one you call.", e);
        }
        finally
        {
            await conn.CreateCommand($"DROP VIEW IF EXISTS {probe}")
                .ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static string QualifiedName(DbObjectName identifier)
        => $"{SchemaUtils.QuoteName(identifier.Schema)}.{SchemaUtils.QuoteName(identifier.Name)}";

    private static async Task<string?> readDefinitionAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return null;
        }

        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false))
        {
            return null;
        }

        var definition = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);

        return string.IsNullOrWhiteSpace(definition) ? null : definition.Trim();
    }

    /// <summary>
    ///     Both sides of the comparison are already canonical MySQL renderings by the time they get
    ///     here, so this only has to absorb whitespace and case.
    /// </summary>
    internal static string NormalizeSql(string sql)
        => sql.Replace("\r\n", "")
            .Replace("\n", "")
            .Replace("\r", "")
            .Replace("\t", "")
            .Replace(" ", "")
            .Trim()
            .TrimEnd(';')
            .ToUpperInvariant();
}

/// <summary>
///     MySQL applies a view change with <c>CREATE OR REPLACE VIEW</c> and nothing else, so the
///     update is the create statement on its own — no DROP ahead of it, which would be a second
///     statement for MySQL's semicolon-splitting executor to run for no reason.
/// </summary>
internal class ViewDelta: ISchemaObjectDelta
{
    private readonly View _view;

    public ViewDelta(View view, SchemaPatchDifference difference)
    {
        _view = view;
        Difference = difference;
    }

    public ISchemaObject SchemaObject => _view;

    public SchemaPatchDifference Difference { get; }

    public void WriteUpdate(Migrator rules, TextWriter writer)
    {
        _view.WriteCreateStatement(rules, writer);
    }

    public void WriteRollback(Migrator rules, TextWriter writer)
    {
    }

    public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
    {
        throw new NotSupportedException();
    }
}
