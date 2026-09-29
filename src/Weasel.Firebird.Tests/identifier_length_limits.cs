using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Functions;
using Weasel.Firebird.Procedures;
using Weasel.Firebird.Tables;
using Weasel.Firebird.Triggers;
using Weasel.Firebird.Views;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     A6: Firebird 3's limit is 31 bytes for every kind of object, Firebird 4's and 5's 63 characters,
///     and the server refuses a longer name rather than truncating it. Weasel refuses it first, and
///     never truncates.
/// </summary>
public class identifier_length_limits: IntegrationContext
{
    private static Table named(string name, string? primaryKeyName = null)
    {
        var table = new Table(name);
        table.AddColumn<int>("id").AsPrimaryKey();
        if (primaryKeyName != null)
        {
            table.PrimaryKeyName = primaryKeyName;
        }

        return table;
    }

    [Fact]
    public async Task a_31_byte_name_works_on_every_version()
    {
        var table = named(new string('t', 31), "pk_31");

        await ApplyAsync(table);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_31_byte_name_of_multibyte_characters_works_on_every_version()
    {
        var table = named(new string('Ä', 15) + "A", "pk_multibyte");

        await ApplyAsync(table);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The measurement: Firebird 3 refuses a 32-byte name outright, with no truncation.
    /// </summary>
    [Fact]
    public async Task firebird_3_refuses_a_32_byte_name()
    {
        if (ServerVersion.Major >= 4)
        {
            Assert.Skip($"Firebird {ServerVersion} allows 63 characters");
        }

        var ex = await Should.ThrowAsync<FbException>(() =>
            ExecuteAsync($"CREATE TABLE {new string('t', 32)} (id INTEGER)"));

        FirebirdMigrator.HasErrorNumber(ex, 336068767).ShouldBeTrue(ex.Message);
    }

    [Fact]
    public async Task firebird_4_and_later_take_63_characters_when_asked_to()
    {
        if (ServerVersion.Major < 4)
        {
            Assert.Skip($"Firebird {ServerVersion} allows 31 bytes");
        }

        var table = named(new string('t', 63), "pk_63");
        var migrator = new FirebirdMigrator { MaxIdentifierLength = 63 };
        migrator.AssertValidIdentifier(table.Identifier.Name);

        var migration = await SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None, table);
        await migrator.ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_derived_primary_key_name_over_the_limit_is_refused_with_the_setting_to_use()
    {
        var table = named("a_table_name_of_29_characters");

        var ex = await Should.ThrowAsync<InvalidOperationException>(() => ApplyAsync(table));
        ex.Message.ShouldContain("PrimaryKeyName");

        (await table.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    /// <summary>
    ///     An index name derived from a table and a column -- <c>idx_people_a_much_longer_column_name</c> --
    ///     passes 31 bytes easily. Every table of the migration is refused, the one before it included,
    ///     because nothing runs until everything has been written.
    /// </summary>
    [Fact]
    public async Task an_index_name_over_the_limit_is_refused_before_anything_runs()
    {
        var first = named("first_table", "pk_first");
        var people = named("people");
        people.AddColumn<string>("a_much_longer_column_name").AddIndex();

        var ex = await Should.ThrowAsync<InvalidOperationException>(() => ApplyAsync(first, people));
        ex.Message.ShouldContain("idx_people_a_much_longer_column_name");
        ex.Message.ShouldContain("an index on table people");

        (await first.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse("nothing runs when a name is refused");
        (await people.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    [Fact]
    public async Task a_foreign_key_name_over_the_limit_is_refused_before_anything_runs()
    {
        var states = named("states", "pk_states");
        var people = named("people");
        people.AddColumn<int>("state_id").ForeignKeyTo(states, "id", new string('f', 32));

        var ex = await Should.ThrowAsync<InvalidOperationException>(() => ApplyAsync(states, people));
        ex.Message.ShouldContain("a foreign key of table people");

        (await states.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    [Fact]
    public async Task a_column_name_over_the_limit_is_refused_before_anything_runs()
    {
        var people = named("people");
        people.AddColumn<int>(new string('c', 32));

        var ex = await Should.ThrowAsync<InvalidOperationException>(() => ApplyAsync(people));
        ex.Message.ShouldContain("a column of table people");

        (await people.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    /// <summary>
    ///     An update checks the names it adds: the column that would have been added first is not.
    /// </summary>
    [Fact]
    public async Task an_update_that_adds_a_name_over_the_limit_is_refused_before_anything_runs()
    {
        var states = named("states", "pk_states");
        var people = named("people");
        await ApplyAsync(states, people);

        people.AddColumn<int>("added");
        people.AddColumn<string>("a_much_longer_column_name").AddIndex();

        await Should.ThrowAsync<InvalidOperationException>(() => people.ApplyChangesAsync(theConnection));

        var withForeignKey = named("people");
        withForeignKey.AddColumn<int>("added");
        withForeignKey.AddColumn<int>("state_id").ForeignKeyTo(states, "id", new string('f', 32));

        var ex = await Should.ThrowAsync<InvalidOperationException>(() => ApplyAsync(withForeignKey));
        ex.Message.ShouldContain("a foreign key of table people");

        (await people.FetchExistingAsync(theConnection))!.HasColumn("added")
            .ShouldBeFalse("nothing runs when a name is refused");
    }

    /// <summary>
    ///     A name that is already there -- from a database first migrated under a longer limit -- is
    ///     compared, not created, so an update under a shorter limit leaves it alone and adds what fits.
    /// </summary>
    [Fact]
    public async Task an_update_does_not_check_the_names_already_there()
    {
        var people = named("people");
        people.AddColumn<string>("a_rather_long_column").AddIndex(x => x.Name = "idx_people_long");
        await ApplyAsync(people);

        people.AddColumn<int>("added");
        var migrator = new FirebirdMigrator { MaxIdentifierLength = 8 };
        var migration = await SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None, people);
        await migrator.ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        (await people.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_table_name_over_the_limit_is_refused_by_a_database_before_it_reaches_the_server()
    {
        var database = new DatabaseWithTables("limits", ConnectionString);
        database.AddTable(named(new string('t', 32), "pk_short"));

        await Should.ThrowAsync<InvalidOperationException>(() => database.ApplyAllConfiguredChangesToDatabaseAsync());
    }

    /// <summary>
    ///     A name longer than the catalog's own columns hold -- 31 bytes on Firebird 3, 63 characters from
    ///     4 on -- cannot be bound against them: FirebirdClient fails the query with "string truncation"
    ///     (335544321), which says nothing about the name. Every way into introspection refuses it as over
    ///     the limit instead, whatever <see cref="FirebirdMigrator.MaxIdentifierLength" /> says.
    /// </summary>
    private string longerThanTheCatalogHolds() => new('t', ServerVersion.MaxIdentifierLength + 3);

    private static void shouldBeTheLengthRefusal(InvalidOperationException ex)
    {
        ex.Message.ShouldContain("over the");
        ex.Message.ShouldContain("no longer name");
    }

    [Fact]
    public async Task determining_a_migration_refuses_a_table_name_the_catalog_cannot_hold()
    {
        var table = named(longerThanTheCatalogHolds(), "pk_long");
        var migrator = new FirebirdMigrator { MaxIdentifierLength = 63 };

        shouldBeTheLengthRefusal(await Should.ThrowAsync<InvalidOperationException>(() =>
            SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None, table)));
    }

    [Fact]
    public async Task reading_a_table_refuses_a_name_the_catalog_cannot_hold()
    {
        var table = named(longerThanTheCatalogHolds(), "pk_long");

        shouldBeTheLengthRefusal(await Should.ThrowAsync<InvalidOperationException>(() =>
            table.FetchExistingAsync(theConnection)));
        shouldBeTheLengthRefusal(await Should.ThrowAsync<InvalidOperationException>(() =>
            table.FindDeltaAsync(theConnection)));
        shouldBeTheLengthRefusal(await Should.ThrowAsync<InvalidOperationException>(() =>
            table.ExistsInDatabaseAsync(theConnection)));
    }

    [Fact]
    public async Task applying_changes_refuses_a_table_name_the_catalog_cannot_hold()
    {
        var table = named(longerThanTheCatalogHolds(), "pk_long");

        shouldBeTheLengthRefusal(await Should.ThrowAsync<InvalidOperationException>(() =>
            table.ApplyChangesAsync(theConnection)));
    }

    [Fact]
    public async Task a_database_refuses_a_table_name_the_catalog_cannot_hold_when_it_checks_itself()
    {
        var database = new DatabaseWithTables("limits", ConnectionString);
        database.AddTable(named(longerThanTheCatalogHolds(), "pk_long"));

        shouldBeTheLengthRefusal(await Should.ThrowAsync<InvalidOperationException>(() =>
            database.AssertDatabaseMatchesConfigurationAsync()));
    }

    [Fact]
    public async Task determining_a_migration_refuses_a_sequence_name_the_catalog_cannot_hold()
    {
        var sequence = new Sequence(longerThanTheCatalogHolds());
        var migrator = new FirebirdMigrator { MaxIdentifierLength = 63 };

        shouldBeTheLengthRefusal(await Should.ThrowAsync<InvalidOperationException>(() =>
            SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None, sequence)));
    }

    /// <summary>
    ///     Views, functions, procedures and triggers bind their names against the catalog as tables do, so
    ///     each is refused the same way, on every way in: a migration, a delta, a read.
    /// </summary>
    private async Task shouldRefuseToIntrospectAsync(ISchemaObject schemaObject, Func<Task> read)
    {
        var migrator = new FirebirdMigrator { MaxIdentifierLength = 63 };

        shouldBeTheLengthRefusal(await Should.ThrowAsync<InvalidOperationException>(() =>
            SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None, schemaObject)));
        shouldBeTheLengthRefusal(await Should.ThrowAsync<InvalidOperationException>(() =>
            ((SchemaObjectBase)schemaObject).FindDeltaAsync(theConnection)));
        shouldBeTheLengthRefusal(await Should.ThrowAsync<InvalidOperationException>(read));
    }

    [Fact]
    public async Task a_view_name_the_catalog_cannot_hold_is_refused_before_it_is_bound()
    {
        var view = new View(longerThanTheCatalogHolds(), "select 1 as x from rdb$database");

        await shouldRefuseToIntrospectAsync(view, () => view.ExistsInDatabaseAsync(theConnection));
    }

    [Fact]
    public async Task a_function_name_the_catalog_cannot_hold_is_refused_before_it_is_bound()
    {
        var name = longerThanTheCatalogHolds();
        var function = new Function(name, $"CREATE FUNCTION {name} RETURNS INTEGER AS BEGIN RETURN 1; END");

        await shouldRefuseToIntrospectAsync(function, () => function.ExistsInDatabaseAsync(theConnection));
    }

    [Fact]
    public async Task a_procedure_name_the_catalog_cannot_hold_is_refused_before_it_is_bound()
    {
        var name = longerThanTheCatalogHolds();
        var procedure = new StoredProcedure(name, $"CREATE PROCEDURE {name} AS BEGIN END");

        await shouldRefuseToIntrospectAsync(procedure, () => procedure.ExistsInDatabaseAsync(theConnection));
    }

    [Fact]
    public async Task a_trigger_name_the_catalog_cannot_hold_is_refused_before_it_is_bound()
    {
        var trigger = new Trigger(longerThanTheCatalogHolds(), "orders", "BEGIN END");

        await shouldRefuseToIntrospectAsync(trigger, () => trigger.ExistsInDatabaseAsync(theConnection));
    }

    /// <summary>
    ///     A name the catalog holds but the migrator's limit does not is read like any other -- there is
    ///     no such table -- and refused when the DDL for it is written.
    /// </summary>
    [Fact]
    public async Task a_name_the_catalog_holds_is_read_and_refused_when_it_would_be_created()
    {
        if (ServerVersion.Major < 4)
        {
            Assert.Skip($"Firebird {ServerVersion}'s catalog holds no more than the default limit");
        }

        var table = named(new string('t', 40), "pk_long");

        (await table.FetchExistingAsync(theConnection)).ShouldBeNull();

        var ex = await Should.ThrowAsync<InvalidOperationException>(() => ApplyAsync(table));
        ex.Message.ShouldContain("the name of a table");
    }
}
