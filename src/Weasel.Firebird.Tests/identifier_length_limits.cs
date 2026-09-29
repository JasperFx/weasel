using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
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
}
