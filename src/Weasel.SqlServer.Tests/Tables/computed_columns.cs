using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     Computed column support (#363): [name] AS (expr) [PERSISTED] columns are
///     emitted, read back by FetchExisting via sys.computed_columns, and
///     participate in delta detection by canonicalized expression + PERSISTED
///     flag.
/// </summary>
public class computed_columns: IntegrationContext
{
    public computed_columns(): base("computed")
    {
    }

    public override Task InitializeAsync() => ResetSchema();

    private async Task AssertNoDeltasAfterPatching(Table table)
    {
        var delta = await table.FindDeltaAsync(theConnection);
        var migration = new SchemaMigration(delta);
        var migrator = new SqlServerMigrator();
        await migrator.ApplyAllAsync(theConnection, migration, JasperFx.AutoCreate.CreateOrUpdate);

        var after = await table.FindDeltaAsync(theConnection);
        after.HasChanges().ShouldBeFalse();
    }

    private Table theTable()
    {
        var table = new Table("computed.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");
        table.AddColumn<string>("full_name").ComputedAs("first_name + ' ' + last_name");
        table.AddColumn<int>("name_length").ComputedAs("len(first_name) + len(last_name)", persisted: true);
        return table;
    }

    [Fact]
    public async Task fetch_existing_reads_computed_definition()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);

        var virtualColumn = existing!.ColumnFor("full_name")!;
        virtualColumn.ComputedExpression.ShouldNotBeNull();
        virtualColumn.ComputedColumnIsStored.ShouldBeFalse();

        var persistedColumn = existing.ColumnFor("name_length")!;
        persistedColumn.ComputedExpression.ShouldNotBeNull();
        persistedColumn.ComputedColumnIsStored.ShouldBeTrue();
    }

    [Fact]
    public async Task computed_columns_round_trip_with_no_delta()
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
        changed.AddColumn<string>("full_name").ComputedAs("last_name + ', ' + first_name");
        changed.AddColumn<int>("name_length").ComputedAs("len(first_name) + len(last_name)", persisted: true);

        var delta = await changed.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(changed);
    }

    [Fact]
    public async Task changed_persisted_flag_is_detected_and_migrated()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        var changed = new Table("computed.people");
        changed.AddColumn<int>("id").AsPrimaryKey();
        changed.AddColumn<string>("first_name");
        changed.AddColumn<string>("last_name");
        changed.AddColumn<string>("full_name").ComputedAs("first_name + ' ' + last_name", persisted: true);
        changed.AddColumn<int>("name_length").ComputedAs("len(first_name) + len(last_name)", persisted: true);

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
    public async Task undeclared_computed_columns_are_left_alone()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        // the model doesn't know these columns are computed — conservative
        // comparison leaves the actual columns untouched
        var unaware = new Table("computed.people");
        unaware.AddColumn<int>("id").AsPrimaryKey();
        unaware.AddColumn<string>("first_name");
        unaware.AddColumn<string>("last_name");
        unaware.AddColumn<string>("full_name");
        unaware.AddColumn<int>("name_length");

        var delta = await unaware.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
