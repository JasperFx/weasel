using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

public class creating_tables_in_database: IntegrationContext
{
    private static Table people()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");
        return table;
    }

    [Fact]
    public async Task create_table_in_the_database()
    {
        var table = people();

        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();

        await ExecuteAsync("INSERT INTO people (id, first_name, last_name) VALUES (1, 'Elton', 'John')");
        (await ScalarAsync<int>("SELECT COUNT(*) FROM people")).ShouldBe(1);
    }

    [Fact]
    public async Task a_created_table_reads_back_with_no_delta()
    {
        var table = people();
        table.AddColumn<Guid>("external_id").NotNull();
        table.AddColumn<decimal>("amount");
        table.AddColumn<DateTime>("created_at");
        table.AddColumn<bool>("active").DefaultValueByExpression("TRUE");
        table.AddColumn<byte[]>("payload");
        table.AddColumn("notes", "BLOB SUB_TYPE TEXT");
        table.Indexes.Add(new IndexDefinition("idx_people_last_name") { Columns = ["last_name"] });

        await CreateSchemaObjectInDatabase(table);

        var delta = await table.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task migrate_async()
    {
        var table = people();

        (await table.MigrateAsync(theConnection)).ShouldBeTrue();
        (await table.MigrateAsync(theConnection)).ShouldBeFalse("the second run has nothing left to do");
    }

    [Fact]
    public async Task create_then_drop()
    {
        var table = people();

        await CreateSchemaObjectInDatabase(table);
        await table.DropAsync(theConnection);

        (await table.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    /// <summary>
    ///     weasel#620: every CREATE is guarded, so the same script runs again cleanly.
    /// </summary>
    [Fact]
    public async Task creating_twice_is_harmless()
    {
        var table = people();
        table.Indexes.Add(new IndexDefinition("idx_people_first_name") { Columns = ["first_name"] });

        await CreateSchemaObjectInDatabase(table);
        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
    }

    [Fact]
    public async Task create_with_multi_column_pk()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("tenant_id").AsPrimaryKey();
        table.AddColumn<string>("first_name");

        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);
        existing!.PrimaryKeyColumns.ShouldBe(["ID", "TENANT_ID"]);
        existing.PrimaryKeyName.ShouldBe("PK_PEOPLE");
    }

    [Fact]
    public async Task create_tables_with_foreign_keys_too_in_the_database()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();

        var people = new Table("people");
        people.AddColumn<int>("id").AsPrimaryKey();
        people.AddColumn<string>("first_name");
        people.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await CreateSchemaObjectInDatabase(states);
        await CreateSchemaObjectInDatabase(people);

        var existing = await people.FetchExistingAsync(theConnection);
        var foreignKey = existing!.ForeignKeys.Single();

        foreignKey.Name.ShouldBe("FK_PEOPLE_STATE_ID");
        foreignKey.LinkedTable!.Name.ShouldBe("STATES");
        foreignKey.ColumnNames.ShouldBe(["STATE_ID"]);
        foreignKey.LinkedNames.ShouldBe(["ID"]);

        (await people.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Two tables that reference each other: neither can go first, so the migration holds the keys
    ///     back until both exist.
    /// </summary>
    [Fact]
    public async Task tables_that_reference_each_other_are_created_by_one_migration()
    {
        var left = new Table("lefts");
        left.AddColumn<int>("id").AsPrimaryKey();
        left.AddColumn<int>("right_id").ForeignKeyTo("rights", "id");

        var right = new Table("rights");
        right.AddColumn<int>("id").AsPrimaryKey();
        right.AddColumn<int>("left_id").ForeignKeyTo("lefts", "id");

        await ApplyAsync(AutoCreate.CreateOrUpdate, left, right);

        var migration = await DetermineAsync(left, right);
        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task dropping_a_table_drops_the_keys_that_reference_it_first()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();

        var people = new Table("people");
        people.AddColumn<int>("id").AsPrimaryKey();
        people.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await CreateSchemaObjectInDatabase(states);
        await CreateSchemaObjectInDatabase(people);

        await states.DropAsync(theConnection);

        (await states.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await people.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await people.FetchExistingAsync(theConnection))!.ForeignKeys.ShouldBeEmpty();
    }
}
