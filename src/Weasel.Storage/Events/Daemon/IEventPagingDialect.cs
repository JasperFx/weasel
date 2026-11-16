#nullable enable
using System.Data.Common;

namespace Weasel.Storage;

/// <summary>
/// The SQL half of the async daemon's event loader. A store's dialect renders an
/// <see cref="EventPageQuery"/> into a command; <see cref="EventLoaderBase"/> owns everything
/// around it — paging, skip accounting, the ceiling calculation, and cancellation.
/// </summary>
/// <remarks>
/// <para>
/// Deliberately separate from <see cref="IEventStoreSqlDialect"/>'s append-side descriptors, and
/// reached through <see cref="IEventStoreSqlDialect.EventPaging"/> so a dialect can adopt the
/// loader without also being ready to hand over its append SQL. That matters because the three
/// stores are at different points: Marten's loader carries an adaptive strategy the others lack,
/// so it will adopt the primitives before it adopts a plain page read.
/// </para>
/// <para>
/// A dialect renders whole commands rather than SQL strings so it keeps control of parameter
/// naming and typing. Npgsql, Microsoft.Data.SqlClient and Microsoft.Data.Sqlite all accept
/// <c>@name</c> placeholders, but Postgres wants an explicit <c>NpgsqlDbType.Bigint</c> on the
/// bounds to keep the planner on an index scan, and only the dialect can say so.
/// </para>
/// <para>
/// <b>A dialect must render an event-type allow list of any length in one statement</b>
/// (weasel#846). <see cref="EventLoaderBase"/> has no client-side filter to fall back on, by design
/// (see <see cref="EventTypeAllowList"/>), so a list the provider refuses fails the shard. Binding one
/// parameter per name is the trap: SQL Server refuses any request with more than 2,100 parameters, so
/// a shard reading a few thousand event types would die with an <c>SqlException</c>. Bind the whole list
/// as <b>one</b> parameter and expand it server-side instead:
/// </para>
/// <list type="bullet">
/// <item>SQL Server: <c>type in (select value from openjson(@types) with (value varchar(500) '$'))</c>,
/// with <c>@types</c> a JSON array. Give the <c>with</c> clause the column's own type: OPENJSON's
/// default <c>nvarchar(max)</c> compared with a <c>varchar</c> column converts the column and loses the
/// index seek.</item>
/// <item>PostgreSQL: <c>type = any(@types)</c>, with <c>@types</c> a <c>text[]</c>.</item>
/// <item>SQLite: <c>type in (select value from json_each(@types))</c>. SQLite's own cap is 32,766
/// variables, so one parameter per name only fails much later, but the same shape avoids it.</item>
/// </list>
/// <para>
/// A dialect matching on <c>dotnet_type</c> binds <see cref="EventTypeAllowList.DotNetTypeNames"/> the
/// same way, plus one more parameter for <see cref="EventTypeAllowList.UpcastSourceAliases"/>. The
/// skip-ahead probe takes the same filter. <c>LargeEventTypeAllowList</c> in Weasel's own test suite
/// renders a 2,500-name list through a dialect against SQL Server.
/// </para>
/// </remarks>
public interface IEventPagingDialect
{
    /// <summary>
    /// The bounded page read: the events in <c>(Floor, Ceiling]</c> ordered by sequence, limited to
    /// the batch size, with the tenant and event-type filters applied <b>in SQL</b> — including the
    /// allow list's upcast sources; see <see cref="EventPageQuery.EventTypes"/>.
    /// </summary>
    /// <remarks>
    /// The column list is the dialect's and the store's joint business — whatever the loader's
    /// <see cref="EventLoaderBase.ReadEventAsync"/> reads back. The only column the base itself
    /// touches is the sequence, through <see cref="EventLoaderBase.ReadSequence"/>, which defaults
    /// to ordinal 0.
    /// </remarks>
    EventPageCommand BuildPageCommand(EventPageQuery query);

    /// <summary>
    /// The skip-ahead probe: the lowest <c>seq_id</c> above <see cref="EventPageQuery.Floor"/> and
    /// at or below <see cref="EventPageQuery.Ceiling"/> that matches this query's filters, as a
    /// single-column, single-row result — or no row when nothing matches.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the primitive Marten's adaptive loader needs and the other two stores do not have
    /// yet. It exists on the interface rather than in Marten so the escalation strategy can be
    /// layered on the shared base instead of forcing Marten to keep a private loader.
    /// </para>
    /// <para>
    /// <b>Render it as <c>order by seq_id limit 1</c>, not as <c>min(seq_id)</c></b>. The two
    /// return the same number, but a MIN aggregate is only rewritten into an ordered index scan
    /// when its input is a single relation — joined to a streams table it degrades into a scan of
    /// every remaining row, which makes the probe O(events after the floor) on the one code path
    /// that exists precisely because the store is too large for the ordinary query. That was
    /// marten#5277.
    /// </para>
    /// </remarks>
    EventPageCommand BuildNextMatchingSequenceCommand(EventPageQuery query);
}

/// <summary>
/// A rendered command: the SQL and the parameters to bind, in a form
/// <see cref="EventLoaderBase"/> can apply to any <see cref="DbCommand"/>.
/// </summary>
public sealed record EventPageCommand(string Sql, IReadOnlyList<EventPageParameter> Parameters)
{
    /// <summary>Convenience for a command with no parameters.</summary>
    public EventPageCommand(string sql) : this(sql, [])
    {
    }
}

/// <summary>
/// One parameter of an <see cref="EventPageCommand"/>.
/// </summary>
/// <param name="Name">
/// The parameter name <em>without</em> a provider prefix. <see cref="EventLoaderBase"/> assigns it
/// to <see cref="DbParameter.ParameterName"/> verbatim, which every supported provider accepts
/// whether or not the SQL spells the placeholder with an <c>@</c>.
/// </param>
/// <param name="Value">The value to bind. Null is written as <see cref="DBNull"/>.</param>
/// <param name="DbType">
/// Optional explicit type. Worth setting on the sequence bounds — an untyped bigint bound as a
/// numeric can cost Postgres its index scan on <c>seq_id</c>.
/// </param>
public sealed record EventPageParameter(string Name, object? Value, System.Data.DbType? DbType = null);
