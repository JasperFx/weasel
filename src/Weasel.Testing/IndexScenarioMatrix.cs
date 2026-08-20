using System.Data.Common;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Testing;

/// <summary>
///     One index scenario list, run against every provider (weasel#449). Every scenario is
///     create-then-introspect: the table goes into a real database, comes back through
///     <c>FetchExisting</c>, and the assertion is about the delta — never about a generated string.
/// </summary>
/// <remarks>
///     <para>
///         Index DDL generation was already well covered — 45 tests on PostgreSQL, 21 on MySQL, 14
///         on SQLite, 11 on Oracle, 9 on SQL Server. What was thin is everything past the generated
///         string: SQLite had no index round-trip test at all, and no provider systematically proved
///         that changing one modeled property produces exactly one difference. That untested middle
///         is where weasel#445 lived.
///     </para>
///     <para>
///         A provider joins by deriving from this class and implementing the four hooks. A scenario
///         that does not apply to an engine is turned off by a capability flag rather than by being
///         absent, so the gap is visible in the source instead of being an absent thought.
///     </para>
/// </remarks>
public abstract class IndexScenarioMatrix
{
    /// <summary>Open a connection to the test database. The caller disposes it.</summary>
    protected abstract Task<DbConnection> OpenAsync();

    /// <summary>Empty the schema these scenarios build in.</summary>
    protected abstract Task ResetSchemaAsync(DbConnection conn);

    /// <summary>A migrator for this provider, used to apply every migration.</summary>
    protected abstract Migrator CreateMigrator();

    /// <summary>
    ///     A new, empty table in the schema under test. Implementations return their own concrete
    ///     <c>Table</c>; everything here drives it through <see cref="ITable" />.
    /// </summary>
    protected abstract ITable NewTable(string name);

    /// <summary>
    ///     How many index differences this table delta reports. Each provider's <c>TableDelta</c>
    ///     exposes its <c>ItemDelta&lt;IndexDefinition&gt;</c> as <c>internal</c>, visible only to
    ///     that provider's own test assembly — which is exactly where the subclass lives, so the
    ///     override is one line.
    /// </summary>
    protected abstract (int Different, int Extra, int Missing) IndexDifferences(ISchemaObjectDelta delta);

    // Capability flags. Off means "this engine does not have the feature", not "untested".
    protected virtual bool SupportsUniqueIndexes => true;
    protected virtual bool SupportsDescendingIndexes => true;
    protected virtual bool SupportsPartialIndexes => false;
    protected virtual bool SupportsIncludedColumns => false;

    /// <summary>
    ///     Whether an unquoted mixed-case name survives as written. Oracle folds to uppercase and
    ///     SQLite and PostgreSQL fold to lowercase, so the round trip is case-insensitive there —
    ///     which is correct behaviour rather than a gap.
    /// </summary>
    protected virtual StringComparison NameComparison => StringComparison.OrdinalIgnoreCase;

    /// <summary>A column name that is a reserved word in this dialect.</summary>
    protected virtual string ReservedWordColumnName => "order";

    /// <summary>
    ///     Optional detail for a failure message — the expected and actual index as the provider
    ///     sees them. Default is nothing; a provider that can cheaply render both overrides it.
    /// </summary>
    protected virtual string DescribeIndexes(ISchemaObjectDelta delta) => string.Empty;

    /// <summary>
    ///     Apply this table, then report the delta that remains — which must be
    ///     <see cref="SchemaPatchDifference.None" /> for every scenario that converges.
    /// </summary>
    /// <remarks>
    ///     The migration is built from <see cref="FindDeltaAsync" /> rather than from
    ///     <c>SchemaMigration.DetermineAsync</c> directly, so that a provider which overrides how a
    ///     delta is found applies through the same path it compares through. Otherwise Oracle would
    ///     compare with one mechanism and correct with another, and the fix would never include the
    ///     difference the comparison had just reported.
    /// </remarks>
    protected async Task<ISchemaObjectDelta> ApplyAndFindDeltaAsync(DbConnection conn, ITable table)
    {
        var migrator = CreateMigrator();
        var delta = await FindDeltaAsync(conn, table).ConfigureAwait(false);
        await migrator.ApplyAllAsync(conn, new SchemaMigration(delta), AutoCreate.CreateOrUpdate)
            .ConfigureAwait(false);

        return await FindDeltaAsync(conn, table).ConfigureAwait(false);
    }

    /// <summary>
    ///     Compute the delta for this table the way this provider's own callers do.
    /// </summary>
    /// <remarks>
    ///     The default is <see cref="SchemaMigration.DetermineAsync(DbConnection, CancellationToken, ISchemaObject[])" />,
    ///     which is the migration path. Oracle overrides it: its
    ///     <c>CreateDeltaAsync(DbDataReader)</c> reads columns only — ODP.NET cannot return several
    ///     result sets from one command — so index drift is invisible on the batched path and only
    ///     <c>Table.FindDeltaAsync(OracleConnection)</c> sees it. That limitation is documented on
    ///     the method and is a real gap in Oracle's migration path, not something for this harness
    ///     to paper over silently.
    /// </remarks>
    protected virtual async Task<ISchemaObjectDelta> FindDeltaAsync(DbConnection conn, ITable table)
    {
        var migration = await SchemaMigration.DetermineAsync(conn, CancellationToken.None, table).ConfigureAwait(false);
        return migration.Deltas.Single();
    }

    private async Task RoundTripsWithNoDeltaAsync(string tableName, Action<ITable> configure)
    {
        await using var conn = await OpenAsync().ConfigureAwait(false);
        await ResetSchemaAsync(conn).ConfigureAwait(false);

        var table = NewTable(tableName);
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));
        table.AddColumn("quantity", typeof(int));
        configure(table);

        var delta = await ApplyAndFindDeltaAsync(conn, table).ConfigureAwait(false);

        delta.Difference.ShouldBe(SchemaPatchDifference.None,
            $"{tableName} does not round trip: {DescribeIndexes(delta)}");
    }

    [Fact]
    public Task a_single_column_index_round_trips()
        => RoundTripsWithNoDeltaAsync("ism_single", t => t.AddIndex("ism_single_idx", ["name"]));

    [Fact]
    public Task a_multi_column_index_round_trips()
        => RoundTripsWithNoDeltaAsync("ism_multi", t => t.AddIndex("ism_multi_idx", ["name", "quantity"]));

    [Fact]
    public async Task a_unique_index_round_trips()
    {
        if (!SupportsUniqueIndexes) return;

        await RoundTripsWithNoDeltaAsync("ism_unique",
            t => t.AddIndex("ism_unique_idx", ["name"], true)).ConfigureAwait(false);
    }

    /// <summary>
    ///     A column named for a reserved word. This is the scenario that needed weasel#447 first:
    ///     the index has to quote the column the same way the table did, or it names something else.
    /// </summary>
    [Fact]
    public async Task an_index_over_a_reserved_word_column_round_trips()
    {
        await using var conn = await OpenAsync().ConfigureAwait(false);
        await ResetSchemaAsync(conn).ConfigureAwait(false);

        var table = NewTable("ism_reserved");
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn(ReservedWordColumnName, typeof(int));
        table.AddIndex("ism_reserved_idx", [ReservedWordColumnName]);

        var delta = await ApplyAndFindDeltaAsync(conn, table).ConfigureAwait(false);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     A column name carrying a space — legal since weasel#448, and no longer rewritten into an
    ///     underscore since weasel#458. The index and the column have to agree on it.
    /// </summary>
    [Fact]
    public async Task an_index_over_a_column_name_with_a_space_round_trips()
    {
        await using var conn = await OpenAsync().ConfigureAwait(false);
        await ResetSchemaAsync(conn).ConfigureAwait(false);

        var table = NewTable("ism_spaced");
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("order date", typeof(int));
        table.AddIndex("ism_spaced_idx", ["order date"]);

        var delta = await ApplyAndFindDeltaAsync(conn, table).ConfigureAwait(false);

        delta.Difference.ShouldBe(SchemaPatchDifference.None,
            $"expected index DDL and the one read back disagree; {DescribeIndexes(delta)}");
        table.Columns.ShouldContain(x => x.Name.Equals("order date", NameComparison));
    }

    /// <summary>
    ///     Applying the same table twice is a no-op the second time. The cheapest possible check,
    ///     and the one that catches permanent drift — the disease weasel#445 and weasel#446 were
    ///     about.
    /// </summary>
    [Fact]
    public async Task applying_the_same_table_twice_is_a_no_op()
    {
        await using var conn = await OpenAsync().ConfigureAwait(false);
        await ResetSchemaAsync(conn).ConfigureAwait(false);

        var table = NewTable("ism_idempotent");
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));
        table.AddIndex("ism_idempotent_idx", ["name"]);

        await ApplyAndFindDeltaAsync(conn, table).ConfigureAwait(false);
        var second = await ApplyAndFindDeltaAsync(conn, table).ConfigureAwait(false);

        second.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Change one property and exactly one index reports as different — not zero, which would
    ///     mean the property is not compared at all, and not several, which would mean the
    ///     comparison is matching on the wrong thing. Then the generated fix has to execute and
    ///     converge.
    /// </summary>
    [Fact]
    public async Task changing_an_index_reports_exactly_one_difference_and_the_fix_converges()
    {
        if (!SupportsUniqueIndexes) return;

        await using var conn = await OpenAsync().ConfigureAwait(false);
        await ResetSchemaAsync(conn).ConfigureAwait(false);

        var original = NewTable("ism_drift");
        original.AddPrimaryKeyColumn("id", typeof(int));
        original.AddColumn("name", typeof(string));
        original.AddIndex("ism_drift_idx", ["name"]);

        await ApplyAndFindDeltaAsync(conn, original).ConfigureAwait(false);

        var changed = NewTable("ism_drift");
        changed.AddPrimaryKeyColumn("id", typeof(int));
        changed.AddColumn("name", typeof(string));
        changed.AddIndex("ism_drift_idx", ["name"], true);

        var drift = await FindDeltaAsync(conn, changed).ConfigureAwait(false);

        drift.Difference.ShouldBe(SchemaPatchDifference.Update);
        IndexDifferences(drift).Different.ShouldBe(1);

        var converged = await ApplyAndFindDeltaAsync(conn, changed).ConfigureAwait(false);
        converged.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Drop the declaration and the index is reported as extra, and the generated DROP runs.
    /// </summary>
    [Fact]
    public async Task removing_an_index_reports_exactly_one_extra_and_the_drop_executes()
    {
        await using var conn = await OpenAsync().ConfigureAwait(false);
        await ResetSchemaAsync(conn).ConfigureAwait(false);

        var original = NewTable("ism_removal");
        original.AddPrimaryKeyColumn("id", typeof(int));
        original.AddColumn("name", typeof(string));
        original.AddIndex("ism_removal_idx", ["name"]);

        await ApplyAndFindDeltaAsync(conn, original).ConfigureAwait(false);

        var without = NewTable("ism_removal");
        without.AddPrimaryKeyColumn("id", typeof(int));
        without.AddColumn("name", typeof(string));

        var drift = await FindDeltaAsync(conn, without).ConfigureAwait(false);

        IndexDifferences(drift).Extra.ShouldBe(1);

        var converged = await ApplyAndFindDeltaAsync(conn, without).ConfigureAwait(false);
        converged.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_partial_index_round_trips()
    {
        if (!SupportsPartialIndexes) return;

        await using var conn = await OpenAsync().ConfigureAwait(false);
        await ResetSchemaAsync(conn).ConfigureAwait(false);

        var table = NewTable("ism_partial");
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("quantity", typeof(int));
        var index = table.AddIndex("ism_partial_idx", ["quantity"]);
        index.Predicate = "quantity > 0";

        var delta = await ApplyAndFindDeltaAsync(conn, table).ConfigureAwait(false);

        delta.Difference.ShouldBe(SchemaPatchDifference.None, DescribeIndexes(delta));
    }

    [Fact]
    public async Task an_index_with_included_columns_round_trips()
    {
        if (!SupportsIncludedColumns) return;

        await using var conn = await OpenAsync().ConfigureAwait(false);
        await ResetSchemaAsync(conn).ConfigureAwait(false);

        var table = NewTable("ism_included");
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));
        table.AddColumn("quantity", typeof(int));
        var index = table.AddIndex("ism_included_idx", ["name"]);
        index.IncludeColumns = ["quantity"];

        var delta = await ApplyAndFindDeltaAsync(conn, table).ConfigureAwait(false);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     An engine that does not support a property must reject it rather than ignore it. MySQL
    ///     and Oracle both exposed a <c>Predicate</c> that was never emitted and never compared, so
    ///     a caller who set one got an index silently wider than the one they asked for
    ///     (weasel#449).
    /// </summary>
    [Fact]
    public void an_unsupported_index_property_is_refused_rather_than_ignored()
    {
        if (SupportsPartialIndexes) return;

        var table = NewTable("ism_refused");
        table.AddColumn("quantity", typeof(int));
        var index = table.AddIndex("ism_refused_idx", ["quantity"]);

        Should.Throw<NotSupportedException>(() => index.Predicate = "quantity > 0");
    }
}
