using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     weasel#503. An index that exists but is <em>invalid</em> was read back as a perfectly good one,
///     so the delta reported no drift and the index was never repaired.
/// </summary>
/// <remarks>
///     <para>
///         The introspection query read <c>indisunique</c> and <c>indisprimary</c> but not
///         <c>indisvalid</c>, and <c>pg_get_indexdef</c> renders the same definition either way — so
///         nothing distinguished the two.
///     </para>
///     <para>
///         An invalid index is ignored by the planner. The object exists, <c>\d</c> shows it, Weasel
///         said the schema matched configuration, and every query that was meant to use it silently did
///         a sequential scan instead. Nothing surfaced it.
///     </para>
///     <para>
///         PostgreSQL leaves an index in exactly that state when <c>CREATE INDEX CONCURRENTLY</c> fails
///         partway, which is documented behaviour rather than an edge case. weasel#494 made it more
///         reachable still: a concurrent index on a partitioned table is built per partition, and the
///         parent is <em>deliberately</em> invalid until the last child is attached, so an interrupted
///         run leaves a genuinely half-finished index behind.
///     </para>
/// </remarks>
[Collection("invalid_index")]
public class an_invalid_index_is_drift: IntegrationContext
{
    public an_invalid_index_is_drift(): base("invalid_index")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private Table BuildTable()
    {
        var table = new Table(new PostgresqlObjectName(SchemaName, "docs"));
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("body");

        var index = new IndexDefinition("idx_docs_body");
        index.AgainstColumns("body");
        table.Indexes.Add(index);

        return table;
    }

    /// <summary>
    ///     Marking the index invalid directly is the only way to reach the state deterministically —
    ///     making a real concurrent build fail on demand is not something a test can arrange.
    /// </summary>
    private Task InvalidateTheIndexAsync() =>
        theConnection.CreateCommand(
                $"update pg_index set indisvalid = false where indexrelid = '{SchemaName}.idx_docs_body'::regclass")
            .ExecuteNonQueryAsync();

    private async Task<bool> IndexIsValidAsync()
    {
        var result = await theConnection.CreateCommand(
                $"select indisvalid from pg_index where indexrelid = '{SchemaName}.idx_docs_body'::regclass")
            .ExecuteScalarAsync();

        result.ShouldNotBeNull("the index is gone entirely");
        return (bool)result!;
    }

    private async Task ApplyAsync() =>
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable()), AutoCreate.CreateOrUpdate);

    [Fact]
    public async Task an_invalid_index_is_reported_as_drift()
    {
        await ApplyAsync();
        await InvalidateTheIndexAsync();

        // Pre-fix: None. The index was unusable and Weasel said the schema was correct.
        (await SchemaMigration.DetermineAsync(theConnection, BuildTable()))
            .Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    /// <summary>
    ///     And the drift has to be repairable. The rebuild goes through <c>ItemDelta.Different</c>,
    ///     which drops before it creates — a bare <c>create index</c> would fail with <c>42P07</c>
    ///     against the invalid index still sitting there.
    /// </summary>
    [Fact]
    public async Task applying_the_drift_rebuilds_the_index()
    {
        await ApplyAsync();
        await InvalidateTheIndexAsync();

        (await IndexIsValidAsync()).ShouldBeFalse("the fixture did not reach the state under test");

        await ApplyAsync();

        (await IndexIsValidAsync()).ShouldBeTrue("the index was not rebuilt");

        (await SchemaMigration.DetermineAsync(theConnection, BuildTable()))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     A valid index is still no drift — the check must not make every index rebuild on every run.
    /// </summary>
    [Fact]
    public async Task a_valid_index_is_still_no_drift()
    {
        await ApplyAsync();

        (await SchemaMigration.DetermineAsync(theConnection, BuildTable()))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
