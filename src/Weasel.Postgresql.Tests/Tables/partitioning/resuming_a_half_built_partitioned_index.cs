using JasperFx;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

/// <summary>
///     A concurrent index on a partitioned table that stopped partway through could never be finished:
///     every later run failed with <c>42P07</c>, "relation ... already exists".
/// </summary>
/// <remarks>
///     <para>
///         weasel#494 builds such an index in three steps — <c>CREATE INDEX ON ONLY parent</c>, then
///         per partition a <c>CREATE INDEX CONCURRENTLY</c> under the deterministic
///         <c>IndexDefinition.ChildIndexName</c>, then <c>ALTER INDEX ... ATTACH PARTITION</c> — and the
///         parent stays invalid until the last child is attached. weasel#503 then reads the invalid
///         parent as drift and rebuilds it, by dropping it before creating it again.
///     </para>
///     <para>
///         The drop is what fell short. Dropping a partitioned index takes only its <em>attached</em>
///         children with it, and the child of the partition the run stopped at was never attached:
///         left invalid if its build failed, valid if the run died before the attach. So the rebuild
///         reached that partition, asked for the same name again, and failed — on every run, because
///         nothing ever removed it.
///     </para>
/// </remarks>
[Collection("resume_partition_index")]
public class resuming_a_half_built_partitioned_index: IntegrationContext
{
    private const string IndexName = "idx_resumed_docs_body";

    public resuming_a_half_built_partitioned_index(): base("resume_partition_index")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private Table BuildTable(bool withIndex)
    {
        var table = new Table(new PostgresqlObjectName(SchemaName, "resumed_docs"));
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("tenant_id").AsPrimaryKey().NotNull()
            .PartitionByListValues()
            .AddPartition("one", "one")
            .AddPartition("two", "two");
        table.AddColumn<string>("body");

        if (withIndex)
        {
            // Unique, so that duplicate rows in one partition fail that partition's build while the
            // one before it succeeds. Weasel appends tenant_id, as PostgreSQL requires of a unique
            // index on a partitioned table.
            var index = new IndexDefinition(IndexName) { IsConcurrent = true, IsUnique = true };
            index.AgainstColumns("body");
            table.Indexes.Add(index);
        }

        return table;
    }

    private async Task ApplyAsync(bool withIndex) =>
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable(withIndex)), AutoCreate.CreateOrUpdate);

    private Task ExecuteAsync(string sql) => theConnection.CreateCommand(sql).ExecuteNonQueryAsync();

    /// <summary>
    ///     <c>null</c> when there is no such index, otherwise whether it is valid and the index it is
    ///     attached to, if any.
    /// </summary>
    private async Task<(bool IsValid, string? AttachedTo)?> IndexStateAsync(string indexName)
    {
        await using var reader = await theConnection.CreateCommand(
                "select i.indisvalid, parent.relname from pg_index i "
                + "join pg_class c on c.oid = i.indexrelid "
                + "join pg_namespace n on n.oid = c.relnamespace "
                + "left join pg_inherits h on h.inhrelid = c.oid "
                + "left join pg_class parent on parent.oid = h.inhparent "
                + $"where n.nspname = '{SchemaName}' and c.relname = '{indexName}'")
            .ExecuteReaderAsync();

        if (!await reader.ReadAsync())
        {
            return null;
        }

        return (reader.GetBoolean(0), await reader.IsDBNullAsync(1) ? null : reader.GetString(1));
    }

    private async Task ShouldBeFullyBuiltAsync()
    {
        (await IndexStateAsync(IndexName)).ShouldBe((true, null), "the parent index is not valid");

        foreach (var suffix in new[] { "one", "two", "default" })
        {
            (await IndexStateAsync($"{IndexName}_{suffix}"))
                .ShouldBe((true, IndexName), $"the {suffix} partition's index is not a valid, attached child");
        }

        (await SchemaMigration.DetermineAsync(theConnection, BuildTable(withIndex: true)))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The way PostgreSQL documents it happening: a uniqueness violation partway through a
    ///     concurrent build leaves an invalid index behind.
    /// </summary>
    [Fact]
    public async Task a_build_that_failed_on_one_partition_is_finished_by_the_next_run()
    {
        await ApplyAsync(withIndex: false);
        await ExecuteAsync(
            $"insert into {SchemaName}.resumed_docs (id, tenant_id, body) values "
            + "(gen_random_uuid(), 'one', 'a'), (gen_random_uuid(), 'two', 'dup'), (gen_random_uuid(), 'two', 'dup')");

        var failure = await Should.ThrowAsync<PostgresException>(() => ApplyAsync(withIndex: true));
        failure.SqlState.ShouldBe(PostgresErrorCodes.UniqueViolation);

        // The state under test: the partition before the failure built and attached, the failed one
        // left behind invalid and unattached, the parent invalid because of it.
        (await IndexStateAsync(IndexName)).ShouldBe((false, null));
        (await IndexStateAsync($"{IndexName}_one")).ShouldBe((true, IndexName));
        (await IndexStateAsync($"{IndexName}_two")).ShouldBe((false, null));

        // ...which the delta already reports, through the invalid parent (weasel#503).
        (await SchemaMigration.DetermineAsync(theConnection, BuildTable(withIndex: true)))
            .Difference.ShouldBe(SchemaPatchDifference.Update);

        await ExecuteAsync(
            $"delete from {SchemaName}.resumed_docs_two where ctid = (select min(ctid) from {SchemaName}.resumed_docs_two)");

        // Pre-fix: 42P07, relation "idx_resumed_docs_body_two" already exists -- on this run and on
        // every one after it.
        await ApplyAsync(withIndex: true);

        await ShouldBeFullyBuiltAsync();
    }

    /// <summary>
    ///     A run that died between building a partition's index and attaching it leaves a
    ///     <em>valid</em> child behind. It is rebuilt rather than adopted: it may predate a change to
    ///     the definition, and nothing here compares it.
    /// </summary>
    [Fact]
    public async Task a_build_that_stopped_before_an_attach_is_finished_by_the_next_run()
    {
        await ApplyAsync(withIndex: false);

        // The first two steps of the sequence for "one", and the build without the attach for "two".
        await ExecuteAsync(
            $"create unique index {IndexName} on only {SchemaName}.resumed_docs (body, tenant_id);"
            + $"create unique index {IndexName}_one on {SchemaName}.resumed_docs_one (body, tenant_id);"
            + $"alter index {SchemaName}.{IndexName} attach partition {SchemaName}.{IndexName}_one;"
            + $"create unique index {IndexName}_two on {SchemaName}.resumed_docs_two (body, tenant_id);");

        (await IndexStateAsync($"{IndexName}_two")).ShouldBe((true, null), "the fixture did not reach the state under test");

        // Pre-fix: 42P07 again, for the same reason.
        await ApplyAsync(withIndex: true);

        await ShouldBeFullyBuiltAsync();
    }
}
