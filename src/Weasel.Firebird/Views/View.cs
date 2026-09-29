using System.Data.Common;
using FirebirdSql.Data.FirebirdClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Firebird.Views;

/// <summary>
///     A Firebird view.
/// </summary>
/// <remarks>
///     <para>
///         Firebird keeps a view's query as it was written: <c>RDB$RELATIONS.RDB$VIEW_SOURCE</c> holds the
///         text after <c>AS</c>, trimmed, comments and all. So the delta is a comparison of the two
///         texts that ignores whitespace and case outside literals, as on Oracle, SQL Server and SQLite.
///     </para>
///     <para>
///         Created and updated with <c>CREATE OR ALTER VIEW</c>, which changes the view in place: its
///         columns can be added, removed, renamed or retyped, and the views, procedures and privileges
///         that depend on it survive. The one change Firebird refuses is removing a column something
///         else still uses, and dropping the view first would be refused for the same reason.
///     </para>
/// </remarks>
public class View: ViewBase
{
    public View(string viewName, string viewSql)
        : this(
            viewName != null
                ? FirebirdProvider.Instance.Parse(viewName)
                : throw new ArgumentNullException(nameof(viewName)),
            viewSql)
    {
    }

    public View(DbObjectName identifier, string viewSql)
        : base(FirebirdObjectName.From(identifier ?? throw new ArgumentNullException(nameof(identifier))), viewSql)
    {
    }

    /// <summary>
    ///     The view's name as Firebird's catalog stores it.
    /// </summary>
    internal string CatalogName => SchemaUtils.CatalogName(Identifier.Name);

    /// <inheritdoc />
    protected override DbObjectName WithSchema(string schemaName)
        => new FirebirdObjectName(schemaName, Identifier.Name);

    /// <inheritdoc />
    protected override Migrator GetDefaultMigratorForBasicSql()
        => new FirebirdMigrator { Formatting = SqlFormatting.Concise };

    /// <summary>
    ///     <c>CREATE OR ALTER VIEW</c>, one plain statement.
    /// </summary>
    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        FirebirdObjectName.AssertDefaultSchema(Identifier.Schema, $"view {Identifier.Name}");

        FirebirdScript.WriteStatement(writer, $"CREATE OR ALTER VIEW {SchemaUtils.QuoteName(Identifier.Name)} AS {body(ViewSql)}");
    }

    /// <summary>
    ///     A <c>DROP VIEW</c> that runs only while the view exists. Firebird before 6 has no
    ///     <c>DROP VIEW IF EXISTS</c>.
    /// </summary>
    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        FirebirdScript.WriteGuardedWhenExists(writer, CatalogProbes.View(CatalogName),
            $"DROP VIEW {SchemaUtils.QuoteName(Identifier.Name)}");
    }

    /// <summary>
    ///     Firebird executes one statement per command, so introspection goes through
    ///     <see cref="FirebirdDbCommandBuilder" />.
    /// </summary>
    protected override DbCommandBuilder CreateCommandBuilder(DbConnection conn)
        => conn is FbConnection firebird ? new FirebirdDbCommandBuilder(firebird) : base.CreateCommandBuilder(conn);

    /// <summary>
    ///     The view's source, read whole: a query longer than a <c>VARCHAR</c> would otherwise be cut
    ///     short and report drift for good (weasel#445, weasel#446). Unterminated, because Firebird runs
    ///     one statement per command.
    /// </summary>
    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var name = builder.AddParameter(CatalogName).ParameterName;

        builder.Append(
            $"SELECT RDB$VIEW_SOURCE FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = @{name} AND RDB$VIEW_BLR IS NOT NULL");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readSourceAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new CreateOrAlterDelta(this, null, SchemaPatchDifference.Create);
        }

        var actual = new View(Identifier, existing);

        return PsqlLexer.NormalizeSource(existing) == PsqlLexer.NormalizeSource(body(ViewSql))
            ? new CreateOrAlterDelta(this, actual, SchemaPatchDifference.None)
            : new CreateOrAlterDelta(this, actual, SchemaPatchDifference.Update, differences: ["the query differs"]);
    }

    /// <summary>
    ///     The view as the database has it, or null when there is no view of this name.
    /// </summary>
    public async Task<View?> FetchExistingAsync(FbConnection conn, CancellationToken ct = default)
    {
        var builder = new FirebirdDbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var source = await readSourceAsync(reader, ct).ConfigureAwait(false);

        return source == null ? null : new View(Identifier, source);
    }

    public async Task<bool> ExistsInDatabaseAsync(FbConnection conn, CancellationToken ct = default)
        => await FetchExistingAsync(conn, ct).ConfigureAwait(false) != null;

    private static string body(string viewSql) => viewSql.Trim().TrimEnd(';').TrimEnd();

    private static async Task<string?> readSourceAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false) || await reader.IsDBNullAsync(0, ct).ConfigureAwait(false))
        {
            return null;
        }

        var source = reader.GetString(0);
        return string.IsNullOrWhiteSpace(source) ? null : source.Trim();
    }
}
