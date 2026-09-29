using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     A model holds the name the catalog reports, however the caller spelled it, so introspection binds
///     the right object (weasel#499) and a hand-built identifier compares equal to a read one.
/// </summary>
public class table_identifier_normalization: IntegrationContext
{
    [Theory]
    [InlineData("people")]
    [InlineData("PEOPLE")]
    [InlineData("People")]
    [InlineData("\"PEOPLE\"")]
    [InlineData("PUBLIC.people")]
    [InlineData("public.PEOPLE")]
    public async Task every_spelling_of_one_table_finds_it(string name)
    {
        await ExecuteAsync("CREATE TABLE people (id INTEGER NOT NULL, CONSTRAINT pk_people PRIMARY KEY (id))");

        var table = new Table(name);
        table.AddColumn<int>("id").AsPrimaryKey();

        (await table.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_delimited_name_that_needs_delimiting_finds_its_table()
    {
        await ExecuteAsync("CREATE TABLE \"ORDER LINES\" (\"LINE NO\" INTEGER NOT NULL, CONSTRAINT pk_lines PRIMARY KEY (\"LINE NO\"))");

        var table = new Table("\"order lines\"");
        table.AddColumn<int>("\"line no\"").AsPrimaryKey();

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_read_table_names_its_foreign_key_target_as_a_model_does()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();
        var people = new Table("people");
        people.AddColumn<int>("id").AsPrimaryKey();
        people.AddColumn<int>("state_id").ForeignKeyTo("states", "id");

        await ApplyAsync(states, people);

        var existing = await people.FetchExistingAsync(theConnection);
        existing!.ForeignKeys.Single().LinkedTable.ShouldBe(states.Identifier);
    }

    /// <summary>
    ///     A case-preserving table -- EF Core's, say -- is delimited exactly, and the catalog keeps the case.
    /// </summary>
    [Fact]
    public async Task a_case_preserving_table_keeps_its_case_in_the_catalog()
    {
        var table = new Table("Blogs") { PreserveIdentifierCase = true };
        table.AddColumn<int>("BlogId").AsPrimaryKey();
        table.AddColumn<string>("Url");
        table.PrimaryKeyName = "PK_Blogs";
        table.Indexes.Add(new IndexDefinition("IX_Blogs_Url") { Columns = ["Url"] });

        await ApplyAsync(table);

        (await ListAsync("SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'Blogs'"))
            .ShouldBe(["Blogs"]);
        (await ListAsync("SELECT RDB$FIELD_NAME FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = 'Blogs' ORDER BY RDB$FIELD_POSITION"))
            .ShouldBe(["BlogId", "Url"]);
        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_reserved_table_name_round_trips()
    {
        var table = new Table("order");
        table.AddColumn<int>("value").AsPrimaryKey();
        table.AddColumn<int>("position");

        await ApplyAsync(table);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
