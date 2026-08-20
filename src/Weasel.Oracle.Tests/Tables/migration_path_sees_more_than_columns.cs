using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Tables;

/// <summary>
///     weasel#474: on Oracle the migration path could only see a table's columns. ODP.NET will not
///     execute several statements from one command, so a schema object could register exactly one
///     introspection query and <c>Table</c> spent it on columns — leaving indexes, foreign keys and
///     the primary key invisible to <c>SchemaMigration.DetermineAsync</c>, which is what
///     <c>ApplyChangesAsync</c>, <c>ApplyAllConfiguredChangesToDatabaseAsync</c> and
///     <c>AssertDatabaseMatchesConfigurationAsync</c> all go through.
/// </summary>
/// <remarks>
///     <para>
///         The practical effect: a declared index was created with the table and never touched
///         again. Add one to an existing table and nothing happened. Change one and nothing
///         happened. Remove one and it stayed. And the assert method reported a match throughout.
///     </para>
///     <para>
///         These go through <c>ApplyChangesAsync</c> deliberately — the point is the path, not the
///         introspection. <c>Table.FetchExistingAsync</c> could always see all of this.
///     </para>
/// </remarks>
[Collection("integration")]
public class migration_path_sees_more_than_columns: IntegrationContext
{
    private const string SchemaName = "WEASEL";

    public migration_path_sees_more_than_columns(): base(SchemaName)
    {
    }

    private static Table NewTable()
    {
        var table = new Table($"{SchemaName}.mp_orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.AddColumn<int>("quantity");
        return table;
    }

    [Fact]
    public async Task an_index_added_after_the_table_exists_is_created()
    {
        await ResetSchema();

        await NewTable().ApplyChangesAsync(theConnection);

        var withIndex = NewTable();
        withIndex.Indexes.Add(new IndexDefinition("mp_orders_name_idx") { Columns = ["name"] });

        var before = await withIndex.FindDeltaAsync(theConnection);
        before.Difference.ShouldBe(SchemaPatchDifference.Update);

        await withIndex.ApplyChangesAsync(theConnection);

        var after = await withIndex.FindDeltaAsync(theConnection);
        after.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The one that matters most for anyone running <c>db-assert</c> in CI: an index the model
    ///     declares and the database does not have used to report a clean match.
    /// </summary>
    [Fact]
    public async Task a_missing_index_is_reported_by_the_migration_path()
    {
        await ResetSchema();

        await NewTable().ApplyChangesAsync(theConnection);

        var withIndex = NewTable();
        withIndex.Indexes.Add(new IndexDefinition("mp_orders_qty_idx") { Columns = ["quantity"] });

        var migration = await SchemaMigration.DetermineAsync(
            theConnection, new OracleMigrator().CreateCommandBuilder(theConnection), default, withIndex);

        migration.Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task the_primary_key_is_read_back_by_the_migration_path()
    {
        await ResetSchema();

        var table = NewTable();
        await table.ApplyChangesAsync(theConnection);

        var migration = await SchemaMigration.DetermineAsync(
            theConnection, new OracleMigrator().CreateCommandBuilder(theConnection), default, table);

        var delta = migration.Deltas.Single().ShouldBeOfType<TableDelta>();

        delta.Actual.ShouldNotBeNull();
        delta.Actual!.PrimaryKeyColumns.ShouldContain(x => x.Equals("id", StringComparison.OrdinalIgnoreCase));
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_foreign_key_is_read_back_by_the_migration_path()
    {
        await ResetSchema();

        var states = new Table($"{SchemaName}.mp_states");
        states.AddColumn<int>("id").AsPrimaryKey();
        await states.ApplyChangesAsync(theConnection);

        var orders = NewTable();
        orders.AddColumn<int>("state_id").ForeignKeyTo(states, "id");
        await orders.ApplyChangesAsync(theConnection);

        var migration = await SchemaMigration.DetermineAsync(
            theConnection, new OracleMigrator().CreateCommandBuilder(theConnection), default, orders);

        var delta = migration.Deltas.Single().ShouldBeOfType<TableDelta>();

        delta.Actual!.ForeignKeys.ShouldNotBeEmpty();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Several tables in one batch: the reader has to walk six result sets per table and land on
    ///     the right boundary, or the second table reads the first one's rows.
    /// </summary>
    [Fact]
    public async Task several_tables_in_one_batch_each_read_their_own_result_sets()
    {
        await ResetSchema();

        var first = NewTable();
        first.Indexes.Add(new IndexDefinition("mp_orders_name_idx") { Columns = ["name"] });

        var second = new Table($"{SchemaName}.mp_customers");
        second.AddColumn<int>("id").AsPrimaryKey();
        second.AddColumn<string>("email");
        second.Indexes.Add(new IndexDefinition("mp_customers_email_idx") { Columns = ["email"] });

        await first.ApplyChangesAsync(theConnection);
        await second.ApplyChangesAsync(theConnection);

        var migration = await SchemaMigration.DetermineAsync(
            theConnection, new OracleMigrator().CreateCommandBuilder(theConnection), default, first, second);

        migration.Deltas.Count.ShouldBe(2);
        migration.Difference.ShouldBe(SchemaPatchDifference.None);

        foreach (var delta in migration.Deltas.Cast<TableDelta>())
        {
            delta.Actual!.Indexes.Count.ShouldBe(1);
        }
    }
}
