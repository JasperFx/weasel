using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     An index aligned with its table's partition scheme carries an implicit sys.index_columns row
///     for the partitioning column. Read as a key column, it made every aligned index compare
///     unequal to itself, so each migration run dropped and recreated it -- forever, because the
///     rebuilt index is aligned too and reads back the same way.
/// </summary>
public class partition_aligned_indexes: IntegrationContext
{
    public partition_aligned_indexes(): base("korf")
    {
    }

    private async Task setUpPartitioning(string suffix, string table, string index)
    {
        await ResetSchema();
        await theConnection.CreateCommand(
                $"if exists (select 1 from sys.partition_schemes where name = 'ps_{suffix}') drop partition scheme ps_{suffix};\nif exists (select 1 from sys.partition_functions where name = 'pf_{suffix}') drop partition function pf_{suffix};")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand(
            $"create partition function pf_{suffix} (int) as range right for values (10, 20)").ExecuteNonQueryAsync();
        await theConnection.CreateCommand(
            $"create partition scheme ps_{suffix} as partition pf_{suffix} all to ([PRIMARY])").ExecuteNonQueryAsync();
        await theConnection.CreateCommand(table).ExecuteNonQueryAsync();
        await theConnection.CreateCommand(index).ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task an_aligned_index_does_not_read_its_partitioning_column_as_a_key()
    {
        await setUpPartitioning("korf",
            "create table korf.pt (a int not null, b int not null, part int not null) on ps_korf(part)",
            "create index ix_pt on korf.pt (b, a) on ps_korf(part)");

        var table = new Table("korf.pt");
        table.AddColumn<int>("a").NotNull();
        table.AddColumn<int>("b").NotNull();
        table.AddColumn<int>("part").NotNull();

        var existing = await table.FetchExistingAsync(theConnection);

        existing!.IndexFor("ix_pt")!.Columns.ShouldBe(["b", "a"]);
    }

    [Fact]
    public async Task an_aligned_index_reports_no_drift_and_converges()
    {
        await setUpPartitioning("korf2",
            "create table korf.ev (tenant_id int not null, stamp int not null) on ps_korf2(stamp)",
            "create index ix_tenant on korf.ev (tenant_id) on ps_korf2(stamp)");

        var table = new Table("korf.ev");
        table.AddColumn<int>("tenant_id").NotNull();
        table.AddColumn<int>("stamp").NotNull();
        table.Indexes.Add(new IndexDefinition("ix_tenant") { Columns = ["tenant_id"] });

        var delta = await table.FindDeltaAsync(theConnection);
        var writer = new StringWriter();
        if (delta.Difference != SchemaPatchDifference.None)
        {
            delta.WriteUpdate(new SqlServerMigrator(), writer);
        }

        delta.Difference.ShouldBe(SchemaPatchDifference.None, "patch would be >>>" + writer + "<<<");
    }

    [Fact]
    public async Task an_aligned_unique_index_keeps_the_partitioning_column_it_declares()
    {
        await setUpPartitioning("korfu",
            "create table korf.ptu (a int not null, part int not null) on ps_korfu(part)",
            "create unique index ix_ptu on korf.ptu (a, part) on ps_korfu(part)");

        var table = new Table("korf.ptu");
        table.AddColumn<int>("a").NotNull();
        table.AddColumn<int>("part").NotNull();

        var existing = await table.FetchExistingAsync(theConnection);

        existing!.IndexFor("ix_ptu")!.Columns.ShouldBe(["a", "part"]);
    }
}
