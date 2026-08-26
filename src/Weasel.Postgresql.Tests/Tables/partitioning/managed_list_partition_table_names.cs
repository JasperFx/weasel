using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

/// <summary>
///     weasel#520. <see cref="ListPartitioning" /> resolved its partitions manager-first in
///     <c>WriteCreateStatement</c> and <c>CreateDelta</c>, but <c>PartitionTableNames</c> read the
///     statically declared list directly.
/// </summary>
/// <remarks>
///     <para>
///     <see cref="ListPartitioning.UsePartitionManager" /> also clears
///     <see cref="ListPartitioning.EnableDefaultPartition" />, so for a manager-owned partitioning the
///     name enumeration returned the <b>empty</b> sequence — not a short list, nothing at all.
///     </para>
///     <para>
///     <see cref="RangePartitioning" /> never had the problem: its <c>PartitionTableNames</c> goes
///     through the same manager-first resolution as everything else, which is the shape
///     <c>ListPartitioning.expectedPartitions</c> now copies.
///     </para>
/// </remarks>
[Collection("managed_list_names")]
public class managed_list_partition_table_names: IntegrationContext
{
    public managed_list_partition_table_names(): base("managed_list_names")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    /// <summary>
    ///     The manager in the product reads its partitions out of a lookup table. Nothing here needs
    ///     that machinery — only that the partitions come from the manager rather than from
    ///     <c>AddPartition</c>.
    /// </summary>
    private sealed class StubPartitionManager: IListPartitionManager
    {
        private readonly ListPartition[] _partitions;

        public StubPartitionManager(params string[] values)
            => _partitions = values.Select(x => new ListPartition(x, x.FormatSqlValue())).ToArray();

        public IEnumerable<ListPartition> Partitions() => _partitions;
    }

    private Table managedTable(bool withConcurrentIndex)
    {
        var table = new Table(new PostgresqlObjectName(SchemaName, "managed_docs"));
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("tenant_id").AsPrimaryKey().NotNull()
            .PartitionByListValues()
            .UsePartitionManager(new StubPartitionManager("one", "two"));
        table.AddColumn<string>("body");

        if (withConcurrentIndex)
        {
            var index = new IndexDefinition("idx_managed_docs_body") { IsConcurrent = true };
            index.AgainstColumns("body");
            table.Indexes.Add(index);
        }

        return table;
    }

    [Fact]
    public void the_manager_supplies_the_partition_table_names()
    {
        var table = managedTable(withConcurrentIndex: false);

        table.Partitioning!.PartitionTableNames(table)
            .ShouldBe(["managed_docs_one", "managed_docs_two"]);
    }

    /// <summary>
    ///     The empty sequence is the specific failure: UsePartitionManager clears the default
    ///     partition, so nothing at all came back rather than merely the wrong thing.
    /// </summary>
    [Fact]
    public void a_manager_owned_partitioning_does_not_enumerate_nothing()
    {
        var table = managedTable(withConcurrentIndex: false);

        table.Partitioning!.PartitionTableNames(table).ShouldNotBeEmpty();
    }

    /// <summary>
    ///     A statically declared partitioning still reports its own partitions and its default one,
    ///     so the manager-first resolution did not change the case that already worked.
    /// </summary>
    [Fact]
    public void a_statically_declared_partitioning_is_unaffected()
    {
        var table = new Table(new PostgresqlObjectName(SchemaName, "static_docs"));
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("tenant_id").AsPrimaryKey().NotNull()
            .PartitionByListValues()
            .AddPartition("one", "one")
            .AddPartition("two", "two");

        table.Partitioning!.PartitionTableNames(table)
            .ShouldBe(["static_docs_one", "static_docs_two", "static_docs_default"]);
    }

    private async Task<bool> parentIndexIsValidAsync()
    {
        var result = await theConnection.CreateCommand(
                "select i.indisvalid from pg_index i "
                + "join pg_class c on c.oid = i.indexrelid "
                + "join pg_namespace n on n.oid = c.relnamespace "
                + $"where n.nspname = '{SchemaName}' and c.relname = 'idx_managed_docs_body'")
            .ExecuteScalarAsync();

        result.ShouldNotBeNull("the parent index was never created");
        return (bool)result!;
    }

    private async Task<int> attachedChildIndexCountAsync()
    {
        var result = await theConnection.CreateCommand(
                "select count(*) from pg_inherits h "
                + "join pg_class parent on parent.oid = h.inhparent "
                + "join pg_namespace n on n.oid = parent.relnamespace "
                + $"where n.nspname = '{SchemaName}' and parent.relname = 'idx_managed_docs_body'")
            .ExecuteScalarAsync();

        return Convert.ToInt32(result);
    }

    /// <summary>
    ///     The consequence the issue is actually about. With an empty name list only step 1 of the
    ///     three-step sequence renders — <c>CREATE INDEX ON ONLY parent</c>, which is metadata-only
    ///     and leaves the parent index invalid by design, waiting for children that never arrive.
    /// </summary>
    [Fact]
    public async Task a_concurrent_index_on_a_manager_owned_table_becomes_valid()
    {
        var table = managedTable(withConcurrentIndex: true);

        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, table), AutoCreate.CreateOrUpdate);

        (await attachedChildIndexCountAsync()).ShouldBe(2, "one attached index per manager partition");
        (await parentIndexIsValidAsync()).ShouldBeTrue("the parent index is still invalid");
    }

    /// <summary>
    ///     Adding the index to a table that already exists and holds rows — the case someone reaches
    ///     for <c>IsConcurrent</c> to avoid an outage over.
    /// </summary>
    [Fact]
    public async Task a_concurrent_index_can_be_added_to_an_existing_manager_owned_table()
    {
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, managedTable(withConcurrentIndex: false)),
            AutoCreate.CreateOrUpdate);

        await theConnection.CreateCommand(
                $"insert into {SchemaName}.managed_docs (id, tenant_id, body) values (gen_random_uuid(), 'one', 'x')")
            .ExecuteNonQueryAsync();

        var withIndex = managedTable(withConcurrentIndex: true);
        var migration = await SchemaMigration.DetermineAsync(theConnection, withIndex);
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        await new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        (await attachedChildIndexCountAsync()).ShouldBe(2);
        (await parentIndexIsValidAsync()).ShouldBeTrue("the parent index is still invalid");
    }

    /// <summary>
    ///     The other consumer of this list, called out in weasel#520 as changing behaviour with the
    ///     fix and wanting a test rather than a hope: <c>ReadOtherTables</c> strips foreign keys that
    ///     point at a <em>partition</em> of a partitioned table.
    /// </summary>
    /// <remarks>
    ///     The strip is driven by a foreign key to the partitioned <em>parent</em> — that is what
    ///     locates the partitioned table — and then removes any key pointing at one of its
    ///     partitions. So both keys have to be present for the case to arise at all, and the parent
    ///     one has to survive, or this would just be "remove every foreign key".
    ///     <para>
    ///     With an empty name list the partition key was never stripped, because there were no names
    ///     to match it against.
    ///     </para>
    /// </remarks>
    [Fact]
    public void a_foreign_key_to_a_manager_owned_partition_is_stripped()
    {
        var parent = managedTable(withConcurrentIndex: false);

        var child = new Table(new PostgresqlObjectName(SchemaName, "child_docs"));
        child.AddColumn<Guid>("id").AsPrimaryKey();
        child.AddColumn<Guid>("doc_id")
            .ForeignKeyTo(new PostgresqlObjectName(SchemaName, "managed_docs"), "id");
        child.AddColumn<Guid>("partition_doc_id")
            .ForeignKeyTo(new PostgresqlObjectName(SchemaName, "managed_docs_one"), "id");

        child.ForeignKeys.Count.ShouldBe(2, "the fixture did not add both foreign keys");

        child.ReadOtherTables([parent]);

        child.ForeignKeys.Select(x => x.LinkedTable.Name)
            .ShouldBe(["managed_docs"], "the key to the partition should be stripped and the one to the parent kept");
    }

    /// <summary>
    ///     Nothing is stripped when the table a key points at is not partitioned at all.
    /// </summary>
    [Fact]
    public void a_foreign_key_to_an_unpartitioned_table_is_kept()
    {
        var plain = new Table(new PostgresqlObjectName(SchemaName, "plain_docs"));
        plain.AddColumn<Guid>("id").AsPrimaryKey();

        var child = new Table(new PostgresqlObjectName(SchemaName, "child_docs2"));
        child.AddColumn<Guid>("id").AsPrimaryKey();
        child.AddColumn<Guid>("doc_id")
            .ForeignKeyTo(new PostgresqlObjectName(SchemaName, "plain_docs"), "id");

        child.ReadOtherTables([plain]);

        child.ForeignKeys.Count.ShouldBe(1);
    }
}
