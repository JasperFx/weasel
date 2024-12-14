using Shouldly;
using Weasel.Core;
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
    public async Task migrate_async()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var table = new Table("tables.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        await table.MigrateAsync(theConnection);
    }

    [Fact]
    public async Task migrate_async_with_multiples()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var table = new Table("tables.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        var sequence = new Sequence(new PostgresqlObjectName("public", "counter"));

        var objects = new ISchemaObject[] { table, sequence };

        await objects.MigrateAsync(theConnection);
    }

    [Fact]
    public async Task create_table_in_the_database()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var table = new Table("tables.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();

        await theConnection.CreateCommand(
                "insert into tables.people (id, first_name, last_name) values (1, 'Elton', 'John')")
            .ExecuteNonQueryAsync();
    }


    [Fact]
    public async Task create_then_drop()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var table = new Table("tables.people");
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

        var table = new Table("tables.people");
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

    [PgVersionTargetedFact(MaximumVersion = "13.0")]
    public Task create_tables_with_indexes_concurrently_too_in_the_database() =>
        create_tables_with_indexes_too_in_the_database(true);

    [Fact]
    public Task create_tables_with_indexes_not_concurrently_too_in_the_database() =>
        create_tables_with_indexes_too_in_the_database(false);

    private async Task create_tables_with_indexes_too_in_the_database(bool withConcurrentIndexIndex)
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var states = new Table("tables.states");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);

        var table = new Table("tables.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name").NotNull().AddIndex(x => x.IsConcurrent = withConcurrentIndexIndex);
        table.AddColumn<string>("last_name").NotNull().AddIndex();
        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();
    }


    [PgVersionTargetedFact(MaximumVersion = "13.0")]
    public Task create_tables_with_indexes_concurrently_too_in_the_database_with_different_methods() =>
        create_tables_with_indexes_too_in_the_database_with_different_methods(true, false);

    [Fact]
    public Task create_tables_with_indexes_not_concurrently_too_in_the_database_with_different_methods() =>
        create_tables_with_indexes_too_in_the_database_with_different_methods(false, false);

    [PgVersionTargetedFact(MinimumVersion = "15.0")]
    public Task create_tables_with_indexes_not_concurrently_and_distinct_nulls_too_in_the_database_with_different_methods() =>
        create_tables_with_indexes_too_in_the_database_with_different_methods(false, true);

    public async Task create_tables_with_indexes_too_in_the_database_with_different_methods(
        bool withConcurrentIndexIndex,
        bool withDistinctNulls
    )
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var states = new Table("tables.states");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);

        var table = new Table("tables.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name").AddIndex(x =>
        {
            x.NullsNotDistinct = withDistinctNulls;
            x.IsConcurrent = withConcurrentIndexIndex;
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

    [Fact]
    public async Task create_table_with_name_and_column_using_reserved_keyword()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("tables");

        var table = new Table("order");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");
        table.AddColumn<int>("order");

        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();

        await theConnection.CreateCommand(
                "insert into \"order\" (id, first_name, last_name, \"order\") values (1, 'Elton', 'John',1)")
            .ExecuteNonQueryAsync();
    }
}
