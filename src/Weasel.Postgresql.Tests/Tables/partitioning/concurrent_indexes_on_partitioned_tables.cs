using JasperFx;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

/// <summary>
///     weasel#494. <c>IndexDefinition.IsConcurrent</c> emits <c>CREATE INDEX CONCURRENTLY</c>, which
///     covers an ordinary table and is refused outright on a partitioned one.
/// </summary>
/// <remarks>
///     <para>
///         The supported sequence on a partitioned parent is three steps:
///     </para>
///     <list type="number">
///         <item><c>CREATE INDEX ON ONLY parent</c> — metadata only; the parent index is left invalid</item>
///         <item><c>CREATE INDEX CONCURRENTLY child ON partition</c> — per partition, non-blocking</item>
///         <item><c>ALTER INDEX parent_idx ATTACH PARTITION child_idx</c> — the parent flips to valid
///         once every child is attached</item>
///     </list>
///     <para>
///         Step 2 cannot share a command with the others, which is what the
///         <c>IndexCreationBeginComment</c> markers and the split in <c>PostgresqlMigrator.executeDelta</c>
///         already exist to arrange.
///     </para>
/// </remarks>
[Collection("concurrent_partition_indexes")]
public class concurrent_indexes_on_partitioned_tables: IntegrationContext
{
    public concurrent_indexes_on_partitioned_tables(): base("concurrent_partition_indexes")
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await ResetSchema();
    }

    private Table BuildTable(bool withConcurrentIndex)
    {
        var table = new Table(new PostgresqlObjectName(SchemaName, "partitioned_docs"));
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("tenant_id").AsPrimaryKey().NotNull()
            .PartitionByListValues()
            .AddPartition("one", "one")
            .AddPartition("two", "two");
        table.AddColumn<string>("body");

        if (withConcurrentIndex)
        {
            var index = new IndexDefinition("idx_partitioned_docs_body") { IsConcurrent = true };
            index.AgainstColumns("body");
            table.Indexes.Add(index);
        }

        return table;
    }

    /// <summary>
    ///     The parent index only becomes valid once every partition's index is attached, so this is
    ///     the assertion that the whole three-step sequence ran and not merely the first step.
    /// </summary>
    private async Task<bool> ParentIndexIsValidAsync()
    {
        var result = await theConnection.CreateCommand(
                "select i.indisvalid from pg_index i "
                + "join pg_class c on c.oid = i.indexrelid "
                + "join pg_namespace n on n.oid = c.relnamespace "
                + $"where n.nspname = '{SchemaName}' and c.relname = 'idx_partitioned_docs_body'")
            .ExecuteScalarAsync();

        result.ShouldNotBeNull("the parent index was never created");
        return (bool)result!;
    }

    private async Task<int> AttachedChildIndexCountAsync()
    {
        var result = await theConnection.CreateCommand(
                "select count(*) from pg_inherits h "
                + "join pg_class parent on parent.oid = h.inhparent "
                + "join pg_namespace n on n.oid = parent.relnamespace "
                + $"where n.nspname = '{SchemaName}' and parent.relname = 'idx_partitioned_docs_body'")
            .ExecuteScalarAsync();

        return Convert.ToInt32(result);
    }

    [Fact]
    public async Task a_concurrent_index_can_be_created_on_a_partitioned_table()
    {
        var table = BuildTable(withConcurrentIndex: true);

        // Pre-fix this threw 0A000: "cannot create index on partitioned table ... concurrently".
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, table), AutoCreate.CreateOrUpdate);

        // Every partition, the default one included -- an index that skipped it would leave the
        // parent invalid forever.
        var expected = table.Partitioning!.PartitionTableNames(table).Count();
        expected.ShouldBe(3, "two named partitions plus the default");

        (await AttachedChildIndexCountAsync()).ShouldBe(expected, "one attached index per partition");
        (await ParentIndexIsValidAsync()).ShouldBeTrue("the parent index is still invalid");
    }

    /// <summary>
    ///     The case the issue is actually about: the table already exists with data in it, and the
    ///     index is added afterwards without taking a write outage.
    /// </summary>
    [Fact]
    public async Task a_concurrent_index_can_be_added_to_an_existing_partitioned_table()
    {
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable(withConcurrentIndex: false)),
            AutoCreate.CreateOrUpdate);

        await theConnection.CreateCommand(
                $"insert into {SchemaName}.partitioned_docs (id, tenant_id, body) values (gen_random_uuid(), 'one', 'x')")
            .ExecuteNonQueryAsync();

        var withIndex = BuildTable(withConcurrentIndex: true);
        var migration = await SchemaMigration.DetermineAsync(theConnection, withIndex);
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        await new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        (await AttachedChildIndexCountAsync()).ShouldBe(withIndex.Partitioning!.PartitionTableNames(withIndex).Count());
        (await ParentIndexIsValidAsync()).ShouldBeTrue();
    }

    /// <summary>
    ///     And it has to converge: a second check reports nothing to do, rather than trying to build
    ///     the index again on every run.
    /// </summary>
    [Fact]
    public async Task the_migration_converges()
    {
        var table = BuildTable(withConcurrentIndex: true);

        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, table), AutoCreate.CreateOrUpdate);

        (await SchemaMigration.DetermineAsync(theConnection, BuildTable(withConcurrentIndex: true)))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Changing the index has to rebuild it, which means dropping the parent first. A partitioned
    ///     index cannot be dropped CONCURRENTLY — PostgreSQL refuses — so the drop this path emits has
    ///     to know the difference.
    /// </summary>
    [Fact]
    public async Task a_changed_concurrent_index_is_rebuilt()
    {
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable(withConcurrentIndex: true)),
            AutoCreate.CreateOrUpdate);

        // Same index name, different columns: a rebuild rather than an addition.
        var changed = BuildTable(withConcurrentIndex: false);
        var index = new IndexDefinition("idx_partitioned_docs_body") { IsConcurrent = true };
        index.AgainstColumns("body", "tenant_id");
        changed.Indexes.Add(index);

        var migration = await SchemaMigration.DetermineAsync(theConnection, changed);
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        await new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        (await AttachedChildIndexCountAsync()).ShouldBe(changed.Partitioning!.PartitionTableNames(changed).Count());
        (await ParentIndexIsValidAsync()).ShouldBeTrue();

        (await SchemaMigration.DetermineAsync(theConnection, changed)).Difference
            .ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     An ordinary table is untouched by any of this — it still gets the single
    ///     <c>CREATE INDEX CONCURRENTLY</c> it has always got.
    /// </summary>
    [Fact]
    public async Task an_unpartitioned_table_still_gets_a_plain_concurrent_index()
    {
        var table = new Table(new PostgresqlObjectName(SchemaName, "plain"));
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("body");

        var index = new IndexDefinition("idx_plain_body") { IsConcurrent = true };
        index.AgainstColumns("body");
        table.Indexes.Add(index);

        table.ToCreateSql(new PostgresqlMigrator()).ShouldContain("CONCURRENTLY");
        table.ToCreateSql(new PostgresqlMigrator()).ShouldNotContain("ATTACH PARTITION");

        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, table), AutoCreate.CreateOrUpdate);

        (await SchemaMigration.DetermineAsync(theConnection, table)).Difference
            .ShouldBe(SchemaPatchDifference.None);
    }
}
