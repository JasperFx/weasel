using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Weasel.Postgresql.Tests.Tables.Indexes;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

[Collection("partition_deltas")]
public class detecting_table_deltas_with_partitions : IndexDeltasDetectionContext
{
    private int id;

    public detecting_table_deltas_with_partitions() : base("partition_deltas", "people")
    {


    }

    /*
     * Add column and partition at the same time
     *
     *
     */

    protected async Task addRow(string firstName, string lastName, string userName = null)
    {
        userName ??= $"{firstName}_{lastName}";
        await theConnection.CreateCommand(
            "insert into partition_deltas.people (id, first_name, last_name, user_name) values (:id, :first, :last, :user_name)")
            .With("id", ++id)
            .With("first", firstName)
            .With("last", lastName)
            .With("user_name", userName)
            .ExecuteNonQueryAsync();
    }

    protected async Task<long> countIs()
    {
        var raw = await theConnection.CreateCommand("select count(*) from partition_deltas.people")
            .ExecuteScalarAsync();

        return (long)raw;
    }

    [Fact]
    public async Task no_partition_to_list_partition()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("last_name").AsPrimaryKey();

        theTable.PartitionByList("last_name")
            .AddPartition("Miller", "Miller")
            .AddPartition("May", "May")
            .AddPartition("Smith", "Smith");


        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();
    }

    [Fact]
    public async Task apply_migration_to_list_partitioned()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("last_name").AsPrimaryKey();

        theTable.PartitionByList("last_name")
            .AddPartition("Miller", "Miller")
            .AddPartition("May", "May")
            .AddPartition("Smith", "Smith");


        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();

        await AssertNoDeltasAfterPatching();
    }


    [Fact]
    public async Task apply_migration_to_list_partitioned_with_data()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("last_name").AsPrimaryKey();

        theTable.PartitionByList("last_name")
            .AddPartition("Miller", "Miller")
            .AddPartition("May", "May")
            .AddPartition("Smith", "Smith");

        await addRow("Jeremy", "Miller");
        await addRow("Lindsey", "Miller");
        await addRow("Russell", "May");
        await addRow("Tyler", "May");


        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();

        await AssertNoDeltasAfterPatching();

        (await countIs()).ShouldBe(4);
    }

    [Fact]
    public async Task apply_migration_to_list_partitioned_with_data_with_no_default_partitioning()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("last_name").AsPrimaryKey();

        theTable.PartitionByList("last_name")
            .DisableDefaultPartition()
            .AddPartition("Miller", "Miller")
            .AddPartition("May", "May")
            .AddPartition("Smith", "Smith");

        await addRow("Jeremy", "Miller");
        await addRow("Lindsey", "Miller");
        await addRow("Russell", "May");
        await addRow("Tyler", "May");


        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();

        await AssertNoDeltasAfterPatching();

        (await countIs()).ShouldBe(4);
    }



    [Fact]
    public async Task no_partition_to_hash_partition()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("last_name").AsPrimaryKey();

        theTable.PartitionByHash(new HashPartitioning{Columns = ["last_name"], Suffixes = ["one", "two", "three"]});

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();
    }

    [Fact]
    public async Task apply_migration_to_hash_partitioned()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("last_name").AsPrimaryKey();

        theTable.PartitionByHash(new HashPartitioning{Columns = ["last_name"], Suffixes = ["one", "two", "three"]});

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task no_partition_to_range_partition()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("first_name").AsPrimaryKey();

        theTable.PartitionByRange("first_name")
            .AddRange("m", "'m'", "'mz'");

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();
    }

    [Fact]
    public async Task apply_migration_to_range_partitioned()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("first_name").AsPrimaryKey();

        theTable.PartitionByRange("first_name")
            .AddRange("m", "'m'", "'mz'");

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task apply_migration_to_range_partitioned_with_other_table_changes()
    {
        await CreateSchemaObjectInDatabase(theTable);

        await addRow("Jeremy", "Miller");
        await addRow("Lindsey", "Miller");
        await addRow("Russell", "May");
        await addRow("Tyler", "May");

        theTable.AddColumn<string>("stuff");

        theTable.ModifyColumn("first_name").AsPrimaryKey();

        theTable.PartitionByRange("first_name")
            .AddRange("m", "'m'", "'mz'");

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();

        await AssertNoDeltasAfterPatching();

        (await countIs()).ShouldBe(4);
    }


    [Fact]
    public async Task apply_migration_to_list_partitioned_with_foreign_key_to_other_partitioned_table()
    {
        theTable.ModifyColumn("last_name").AsPrimaryKey();
        theOtherTable.ModifyColumn("last_name").AsPrimaryKey();

        theTable.PartitionByList("last_name")
            .AddPartition("Miller", "Miller")
            .AddPartition("May", "May")
            .AddPartition("Smith", "Smith");

        theOtherTable.PartitionByList("last_name")
            .AddPartition("Miller", "Miller")
            .AddPartition("May", "May")
            .AddPartition("Smith", "Smith");

        theTable.ForeignKeys.Add(new ForeignKey("fk_other")
        {
            ColumnNames = ["id", "last_name"],
            LinkedNames = ["id", "last_name"],
            LinkedTable = theOtherTable.Identifier
        });

        await CreateSchemaObjectInDatabase(theOtherTable);
        await CreateSchemaObjectInDatabase(theTable);

        var existing = await theOtherTable.FetchExistingAsync(theConnection);

        // There should be no delta after patching
        await AssertNoDeltasAfterPatching(theTable);
    }


}
