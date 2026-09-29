using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     A name that needs delimiting -- a reserved word, a space, a leading underscore -- has to be
///     delimited the same way everywhere it appears (weasel#447), or the index names a column the table
///     does not have.
/// </summary>
public class identifier_quoting: IntegrationContext
{
    [Theory]
    [InlineData("order")]
    [InlineData("value")]
    [InlineData("position")]
    [InlineData("user")]
    [InlineData("order date")]
    [InlineData("_internal")]
    [InlineData("2nd")]
    [InlineData("Grüße")]
    public async Task a_name_that_needs_delimiting_round_trips_everywhere_it_appears(string name)
    {
        var parent = new Table($"{name} parents");
        parent.AddColumn<int>(name).AsPrimaryKey();
        parent.PrimaryKeyName = "pk_quoting_parents";

        var child = new Table(name);
        child.AddColumn<int>(name).AsPrimaryKey();
        child.AddColumn<int>($"{name} ref").ForeignKeyTo(parent, name, "fk_quoting");
        child.PrimaryKeyName = "pk_quoting";
        child.Indexes.Add(new IndexDefinition($"{name} idx") { Columns = [name, $"{name} ref"] });

        await ApplyAsync(parent, child);

        (await DetermineAsync(parent, child)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_reserved_word_is_stored_in_the_folded_spelling()
    {
        var table = new Table("order");
        table.AddColumn<int>("value").AsPrimaryKey();

        await ApplyAsync(table);

        (await ListAsync("SELECT RDB$FIELD_NAME FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = 'ORDER'"))
            .ShouldBe(["VALUE"]);
    }

    /// <summary>
    ///     The migration path refuses a name that could escape the statement it is written into; the
    ///     direct API delimits it and leaves it to the server (weasel#447, weasel#448).
    /// </summary>
    [Theory]
    [InlineData("bad\"name")]
    [InlineData("bad^name")]
    [InlineData("bad;name")]
    [InlineData("bad'name")]
    public void the_migration_path_refuses_a_name_that_could_escape(string name)
    {
        var table = new Table("people");
        table.AddColumn<int>(name);

        var migrator = new FirebirdMigrator();
        Should.Throw<InvalidOperationException>(() =>
        {
            foreach (var column in table.LocalIdentifiers())
            {
                migrator.AssertValidLocalIdentifier(column);
            }
        });
    }

    [Fact]
    public async Task a_caret_or_quote_in_a_name_is_still_written_safely_by_the_direct_api()
    {
        var table = new Table("people");
        table.AddColumn<int>("a^b").AsPrimaryKey();
        table.AddColumn<string>("c\"d");
        table.PrimaryKeyName = "pk_people";

        await CreateSchemaObjectInDatabase(table);

        (await ListAsync("SELECT RDB$FIELD_NAME FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = 'PEOPLE' ORDER BY RDB$FIELD_POSITION"))
            .ShouldBe(["A^B", "C\"D"]);
        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
