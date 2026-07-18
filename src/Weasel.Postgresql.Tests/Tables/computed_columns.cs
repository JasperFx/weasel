using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     Computed / generated column support (#363): GENERATED ALWAYS AS (...)
///     STORED columns are emitted, read back by FetchExisting, and participate
///     in delta detection by canonicalized generation expression.
/// </summary>
[Collection("computed")]
public class computed_columns: IntegrationContext
{
    public computed_columns(): base("computed")
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
        var table = new Table("computed.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");
        table.AddColumn("full_name", "text").GeneratedAs("first_name || ' ' || last_name");
        return table;
    }

    [Fact]
    public async Task fetch_existing_reads_generation_expression()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);

        var column = existing!.ColumnFor("full_name")!;
        column.ComputedExpression.ShouldNotBeNull();
        TableCheckConstraint.Canonicalize(column.ComputedExpression!)
            .ShouldBe(TableCheckConstraint.Canonicalize("first_name || ' ' || last_name"));
    }

    [Fact]
    public async Task computed_column_round_trips_with_no_delta()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        var delta = await table.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task changed_expression_is_detected_and_migrated()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        var changed = new Table("computed.people");
        changed.AddColumn<int>("id").AsPrimaryKey();
        changed.AddColumn<string>("first_name");
        changed.AddColumn<string>("last_name");
        changed.AddColumn("full_name", "text").GeneratedAs("last_name || ', ' || first_name");

        var delta = await changed.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(changed);
    }

    [Fact]
    public async Task adding_a_computed_column_to_an_existing_table()
    {
        var table = new Table("computed.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");
        await CreateSchemaObjectInDatabase(table);

        var expanded = theTable();
        var delta = await expanded.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(expanded);
    }

    [Fact]
    public async Task not_null_computed_column_can_still_be_added()
    {
        var table = new Table("computed.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");
        await CreateSchemaObjectInDatabase(table);

        var expanded = new Table("computed.people");
        expanded.AddColumn<int>("id").AsPrimaryKey();
        expanded.AddColumn<string>("first_name");
        expanded.AddColumn<string>("last_name");
        expanded.AddColumn("full_name", "text").GeneratedAs("first_name || ' ' || last_name").NotNull();

        var delta = await expanded.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(expanded);
    }

    [Fact]
    public async Task undeclared_generated_columns_are_left_alone()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        // the model doesn't know full_name is generated — conservative
        // comparison leaves the actual column untouched
        var unaware = new Table("computed.people");
        unaware.AddColumn<int>("id").AsPrimaryKey();
        unaware.AddColumn<string>("first_name");
        unaware.AddColumn<string>("last_name");
        unaware.AddColumn("full_name", "text");

        var delta = await unaware.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
