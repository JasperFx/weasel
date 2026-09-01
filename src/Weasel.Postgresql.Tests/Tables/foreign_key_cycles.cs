using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

[Collection("fkcycles")]
public class foreign_key_cycles: IntegrationContext
{
    public foreign_key_cycles(): base("fkcycles")
    {
    }

    private static Table BuildTable(string name, string linkedTable, string foreignKeyName)
    {
        var table = new Table(new PostgresqlObjectName("fkcycles", name));
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("other_id").AllowNulls();

        table.ForeignKeys.Add(new ForeignKey(foreignKeyName)
        {
            LinkedTable = new PostgresqlObjectName("fkcycles", linkedTable),
            ColumnNames = ["other_id"],
            LinkedNames = ["id"]
        });

        return table;
    }

    private async Task<bool> ConstraintExistsAsync(string name)
    {
        var count = await theConnection.CreateCommand(
                "select count(*) from pg_constraint where conname = :name and contype = 'f'")
            .With("name", name)
            .ExecuteScalarAsync();

        return Convert.ToInt32(count) == 1;
    }

    [Fact]
    public async Task mutually_referencing_tables_are_created_from_scratch()
    {
        await ResetSchema();

        var db = new DatabaseWithTables("cycles", theDataSource);
        db.AddTable(BuildTable("fk_cycle_a", "fk_cycle_b", "fk_cycle_a_to_b"));
        db.AddTable(BuildTable("fk_cycle_b", "fk_cycle_a", "fk_cycle_b_to_a"));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();

        (await ConstraintExistsAsync("fk_cycle_a_to_b")).ShouldBeTrue();
        (await ConstraintExistsAsync("fk_cycle_b_to_a")).ShouldBeTrue();

        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task a_partially_created_cycle_is_completed_by_the_next_apply()
    {
        await ResetSchema();

        var first = new DatabaseWithTables("cycles", theDataSource);
        var withoutForeignKey = new Table(new PostgresqlObjectName("fkcycles", "fk_half_a"));
        withoutForeignKey.AddColumn<int>("id").AsPrimaryKey();
        withoutForeignKey.AddColumn<int>("other_id").AllowNulls();
        first.AddTable(withoutForeignKey);

        await first.ApplyAllConfiguredChangesToDatabaseAsync();

        var db = new DatabaseWithTables("cycles", theDataSource);
        db.AddTable(BuildTable("fk_half_a", "fk_half_b", "fk_half_a_to_b"));
        db.AddTable(BuildTable("fk_half_b", "fk_half_a", "fk_half_b_to_a"));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();

        (await ConstraintExistsAsync("fk_half_a_to_b")).ShouldBeTrue();
        (await ConstraintExistsAsync("fk_half_b_to_a")).ShouldBeTrue();
    }

    [Fact]
    public void an_acyclic_migration_generates_exactly_the_same_ddl_as_before()
    {
        var parent = new Table(new PostgresqlObjectName("fkcycles", "fk_parent"));
        parent.AddColumn<int>("id").AsPrimaryKey();

        var child = BuildTable("fk_child", "fk_parent", "fk_child_to_parent");

        var migrator = new PostgresqlMigrator();
        var migration = new SchemaMigration(new ISchemaObjectDelta[]
        {
            new TableDelta(parent, null), new TableDelta(child, null)
        });

        var actual = new StringWriter();
        migration.WriteAllUpdates(actual, migrator, AutoCreate.All);

        var expected = new StringWriter();
        parent.WriteCreateStatement(migrator, expected);
        child.WriteCreateStatement(migrator, expected);

        actual.ToString().ShouldBe(expected.ToString());
    }
}
