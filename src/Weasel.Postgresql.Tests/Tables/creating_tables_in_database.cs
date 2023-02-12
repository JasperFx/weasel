using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

[Collection("tables")]
public class creating_tables_in_database: IntegrationContext
{
    public creating_tables_in_database(): base("tables")
    {
    }

    [Fact]
    public async Task create_table_in_the_database()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();

        await theConnection.CreateCommand(
                "insert into people (id, first_name, last_name) values (1, 'Elton', 'John')")
            .ExecuteNonQueryAsync();
    }


    [Fact]
    public async Task create_then_drop()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        await CreateSchemaObjectInDatabase(table);

        await DropSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection))
            .ShouldBeFalse();
    }

    [Fact]
    public async Task create_with_multi_column_pk()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("tenant_id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();
    }


    [Fact]
    public async Task create_tables_with_foreign_keys_too_in_the_database()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var states = new Table("tables.states");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);


        var table = new Table("tables.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name").NotNull();
        table.AddColumn<string>("last_name").NotNull();
        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();
    }

    [Fact]
    public async Task create_tables_with_indexes_too_in_the_database()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);


        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name").NotNull().AddIndex(x => x.IsConcurrent = true);
        table.AddColumn<string>("last_name").NotNull().AddIndex();
        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();
    }

    [Fact]
    public async Task create_tables_with_indexes_too_in_the_database_with_different_methods()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);


        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name").AddIndex(x =>
        {
            x.IsConcurrent = true;
            x.IsUnique = true;
            x.SortOrder = SortOrder.Desc;
            x.Predicate = "id > 3";
        });
        table.AddColumn<string>("last_name").AddIndex();
        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();
    }
}
