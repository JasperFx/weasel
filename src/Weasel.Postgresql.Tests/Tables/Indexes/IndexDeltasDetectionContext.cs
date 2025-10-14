using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;

namespace Weasel.Postgresql.Tests.Tables.Indexes;

public abstract class IndexDeltasDetectionContext: IntegrationContext
{
    protected Table theTable;
    protected Table theOtherTable;

    public override Task InitializeAsync() =>
        ResetSchema();

    protected IndexDeltasDetectionContext(string schemaName, string tableName = "people"): base(schemaName)
    {
        theTable = new Table($"{schemaName}.{tableName}");
        theTable.AddColumn<int>("id").AsPrimaryKey();
        theTable.AddColumn<string>("first_name");
        theTable.AddColumn<string>("last_name");
        theTable.AddColumn<string>("user_name");
        theTable.AddColumn<DateTime>("created_datetime");
        theTable.AddColumn<DateTimeOffset>("created_datetime_offset");
        theTable.AddColumn("data", "jsonb");
        theTable.AddColumn<Guid[]>("array_uuid");
        theTable.AddColumn<short[]>("array_short");
        theTable.AddColumn<int[]>("array_int");
        theTable.AddColumn<long[]>("array_long");
        theTable.AddColumn<float[]>("array_float");
        theTable.AddColumn<double[]>("array_double");
        theTable.AddColumn<string[]>("array_string");

        theOtherTable = new Table($"{schemaName}.other");
        theOtherTable.AddColumn<int>("id").AsPrimaryKey();
        theOtherTable.AddColumn<string>("last_name");
    }


    protected async Task AssertNoDeltasAfterPatching(Table? table = null)
    {
        table ??= theTable;
        await table.ApplyChangesAsync(theConnection);

        var delta = await table.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeFalse();
    }

    protected async Task<TableDelta> AssertIndexUpdate(
        string indexName,
        SchemaPatchDifference difference = SchemaPatchDifference.Update,
        Table? table = null
    )
    {
        var delta = await AssertIndexChange(difference, table);

        delta.Indexes.Different.ShouldContain(
            i => i.Expected.Name == indexName && i.Actual.Name == indexName
        );

        return delta;
    }

    protected async Task<TableDelta> AssertIndexRecreation(
        string oldName,
        string? indexName = null,
        SchemaPatchDifference difference = SchemaPatchDifference.Update,
        Table? table = null
    )
    {
        indexName ??= oldName;

        var delta = await AssertIndexChange(difference, table);

        delta.Indexes.Extras.ShouldContain(i => i.Name == indexName);
        delta.Indexes.Missing.ShouldContain(i => i.Name == oldName);

        return delta;
    }

    private async Task<TableDelta> AssertIndexChange(SchemaPatchDifference difference, Table? table)
    {
        table ??= theTable;

        var delta = await table.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeTrue();

        delta.Indexes.Difference().ShouldBe(difference);
        return delta;
    }
}
