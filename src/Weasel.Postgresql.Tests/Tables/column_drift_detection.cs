using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     Opt-in column drift detection (ITable.DetectColumnDrift): default
///     expressions and nullability of otherwise-matching columns participate in
///     delta detection and are corrected with ALTER COLUMN statements.
/// </summary>
[Collection("column_drift")]
public class column_drift_detection: IntegrationContext
{
    public column_drift_detection(): base("column_drift")
    {
    }

    public override Task InitializeAsync() => ResetSchema();

    private async Task AssertNoDeltasAfterPatching(Table table)
    {
        await table.ApplyChangesAsync(theConnection);

        var delta = await table.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeFalse();
    }

    private Table theTable()
    {
        var table = new Table("column_drift.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("status");
        table.ColumnFor("status")!.DefaultExpression = "'pending'";
        table.ColumnFor("status")!.AllowNulls = false;
        table.AddColumn<int>("score");
        table.ColumnFor("score")!.DefaultExpression = "42";
        table.DetectColumnDrift = true;
        return table;
    }

    [Fact]
    public async Task no_drift_is_no_delta()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        var delta = await table.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task off_by_default_ignores_drift()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        await theConnection.CreateCommand("alter table column_drift.people alter column score set default 99;")
            .ExecuteNonQueryAsync();

        table.DetectColumnDrift = false;
        var delta = await table.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task detects_and_corrects_changed_default()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        await theConnection.CreateCommand("alter table column_drift.people alter column score set default 99;")
            .ExecuteNonQueryAsync();

        var delta = await table.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(table);
    }

    [Fact]
    public async Task detects_and_corrects_dropped_default()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        await theConnection.CreateCommand("alter table column_drift.people alter column status drop default;")
            .ExecuteNonQueryAsync();

        var delta = await table.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(table);
    }

    [Fact]
    public async Task detects_and_corrects_nullability_drift()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        await theConnection.CreateCommand("alter table column_drift.people alter column status drop not null;")
            .ExecuteNonQueryAsync();

        var delta = await table.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(table);
    }
}
