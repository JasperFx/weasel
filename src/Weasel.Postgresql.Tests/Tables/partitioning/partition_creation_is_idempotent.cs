using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

// Partition creation must be idempotent (CREATE TABLE IF NOT EXISTS ... PARTITION OF): two concurrent
// schema appliers (e.g. dynamic tenant provisioning racing an async daemon's storage check in a Marten
// consumer) can both compute the same "missing" partition from a momentarily stale snapshot and both
// emit the CREATE. Without IF NOT EXISTS the loser fails its whole migration batch with 42P07.
[Collection("partitions")]
public class partition_creation_is_idempotent: IntegrationContext
{
    public partition_creation_is_idempotent(): base("partitions")
    {
    }

    private static string DdlOf(Action<TextWriter> write)
    {
        var writer = new StringWriter();
        write(writer);
        return writer.ToString();
    }

    [Fact]
    public async Task reapplying_list_partition_ddl_of_an_existing_partition_does_not_throw()
    {
        await ResetSchema();

        var table = new Table("partitions.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("role")
            .PartitionByListValues()
            .AddPartition("admin", "admin");

        await table.CreateAsync(theConnection);

        // Re-issue the same partition DDL as a racing second applier would.
        var ddl = DdlOf(w => ((IPartition)new ListPartition("admin", "'admin'")).WriteCreateStatement(w, table));
        await theConnection.CreateCommand(ddl).ExecuteNonQueryAsync(); // must not throw 42P07
    }

    [Fact]
    public async Task reapplying_range_partition_ddl_of_an_existing_partition_does_not_throw()
    {
        await ResetSchema();

        var table = new Table("partitions.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("role")
            .PartitionByRange()
            .AddRange("twenties", 20, 29);

        await table.CreateAsync(theConnection);

        var ddl = DdlOf(w => ((IPartition)new RangePartition("twenties", "20", "29")).WriteCreateStatement(w, table));
        await theConnection.CreateCommand(ddl).ExecuteNonQueryAsync(); // must not throw 42P07
    }

    [Fact]
    public async Task reapplying_hash_partition_ddl_of_an_existing_partition_does_not_throw()
    {
        await ResetSchema();

        var table = new Table("partitions.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("role")
            .PartitionByHash("one", "two", "three");

        await table.CreateAsync(theConnection);

        var ddl = DdlOf(w => new HashPartition("one", 3, 0).WriteCreateStatement(w, table));
        await theConnection.CreateCommand(ddl).ExecuteNonQueryAsync(); // must not throw 42P07
    }

    [Fact]
    public async Task reapplying_default_partition_ddl_of_an_existing_partition_does_not_throw()
    {
        await ResetSchema();

        var table = new Table("partitions.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("role")
            .PartitionByListValues()
            .AddPartition("admin", "admin");

        await table.CreateAsync(theConnection);

        var ddl = DdlOf(w => w.WriteDefaultPartition(table.Identifier));
        await theConnection.CreateCommand(ddl).ExecuteNonQueryAsync(); // first applier creates people_default
        await theConnection.CreateCommand(ddl).ExecuteNonQueryAsync(); // racing applier must not throw 42P07
    }
}
