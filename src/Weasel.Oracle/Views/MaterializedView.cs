using System.Data.Common;
using JasperFx.Core;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle.Views;

/// <summary>How a materialized view is brought up to date.</summary>
public enum MaterializedViewRefresh
{
    /// <summary>Only when someone asks, through <c>dbms_mview.refresh</c>.</summary>
    OnDemand,

    /// <summary>At the end of every transaction that changes a base table.</summary>
    OnCommit
}

/// <summary>
///     An Oracle materialized view: a query whose results are stored, and which is refreshed rather
///     than re-evaluated.
/// </summary>
/// <remarks>
///     <para>
///         Not a subclass of <see cref="View" />, unlike PostgreSQL's, because Oracle's differ in
///         more than the keyword. There is no <c>CREATE OR REPLACE MATERIALIZED VIEW</c>, the query
///         lives in <c>all_mviews</c> rather than <c>all_views</c>, and refresh mode and query
///         rewrite have no equivalent on a plain view.
///     </para>
///     <para>
///         <strong>The container table shares the view's name.</strong> Oracle lists it in
///         <c>all_tables</c>, and <c>DROP TABLE</c> against it fails with ORA-12083 — which is why
///         the teardown in weasel#465 excludes mview containers from its table sweep.
///     </para>
/// </remarks>
public class MaterializedView: SchemaObjectBase
{
    public MaterializedView(string name, string viewSql)
        : this(DbObjectName.Parse(OracleProvider.Instance, name), viewSql)
    {
    }

    public MaterializedView(DbObjectName identifier, string viewSql): base(identifier)
    {
        ViewSql = viewSql ?? throw new ArgumentNullException(nameof(viewSql));
    }

    /// <summary>The SELECT whose results are stored.</summary>
    public string ViewSql { get; }

    public MaterializedViewRefresh Refresh { get; set; } = MaterializedViewRefresh.OnDemand;

    /// <summary>
    ///     Let the optimizer answer a query against the base tables from this view instead. Off by
    ///     default, because it changes plans for queries that never mention the view.
    /// </summary>
    public bool EnableQueryRewrite { get; set; }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        var refresh = Refresh == MaterializedViewRefresh.OnCommit ? "REFRESH ON COMMIT" : "REFRESH ON DEMAND";
        var rewrite = EnableQueryRewrite ? " ENABLE QUERY REWRITE" : string.Empty;
        var body = ViewSql.TrimEnd().TrimEnd(';');

        writer.WriteLine(
            $"CREATE MATERIALIZED VIEW {Identifier.QualifiedName} {refresh}{rewrite} AS {body}");
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        // ORA-12003: materialized view does not exist.
        writer.WriteLine($@"BEGIN
    EXECUTE IMMEDIATE 'DROP MATERIALIZED VIEW {SchemaUtils.EscapeLiteral(Identifier.QualifiedName)}';
EXCEPTION
    WHEN OTHERS THEN IF SQLCODE != -12003 THEN RAISE; END IF;
END;");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        if (builder.Command is OracleCommand oracleCommand)
        {
            // all_mviews.query is a LONG, and ODP.NET reads a LONG back empty by default -- the
            // trap weasel#450 hit on all_views.TEXT.
            oracleCommand.InitialLONGFetchSize = -1;
        }

        var schemaParam = builder.AddParameter(Identifier.Schema.ToUpperInvariant()).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name.ToUpperInvariant()).ParameterName;

        builder.Append(
            $"SELECT query FROM all_mviews WHERE owner = :{schemaParam} AND mview_name = :{nameParam}");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readQueryAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
        }

        // No CREATE OR REPLACE for a materialized view, so a change is Invalid -- drop and create,
        // which for a materialized view is exactly right: its contents are derived, not authored.
        return Normalize(existing) == Normalize(ViewSql)
            ? new SchemaObjectDelta(this, SchemaPatchDifference.None)
            : new SchemaObjectDelta(this, SchemaPatchDifference.Invalid);
    }

    internal static string Normalize(string sql)
        => sql.Replace("\r\n", "").Replace("\n", "").Replace("\r", "").Replace("\t", "")
            .Replace(" ", "").Trim().TrimEnd(';').ToUpperInvariant();

    public async Task<string?> FetchExistingQueryAsync(OracleConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var query = await readQueryAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return query;
    }

    public async Task<bool> ExistsInDatabaseAsync(OracleConnection conn, CancellationToken ct = default)
        => await FetchExistingQueryAsync(conn, ct).ConfigureAwait(false) != null;

    private static async Task<string?> readQueryAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) return null;

        var query = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        return query.IsEmpty() ? null : query;
    }
}
