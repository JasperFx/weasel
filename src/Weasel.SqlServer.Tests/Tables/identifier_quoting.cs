using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     Names that are not regular identifiers have to be bracketed to survive a trip through DDL.
///     SchemaUtils.QuoteName previously bracketed only a hardcoded reserved-word list, and several
///     emission sites did not call it at all — so a legal object name could produce SQL that will not
///     parse. The unfilled missing-index template, the dotted EF6 key and the column named "Table"
///     all came out of a real database; the rest are constructed edge cases.
/// </summary>
public class identifier_quoting: IntegrationContext
{
    public identifier_quoting(): base("quoting")
    {
    }

    [Theory]
    [InlineData("orders", "orders")]                       // regular identifier, left alone
    [InlineData("Table", "[Table]")]                       // reserved word
    [InlineData("unit price", "[unit price]")]             // space
    [InlineData("PK_dbo.__MigrationHistory", "[PK_dbo.__MigrationHistory]")] // EF6
    [InlineData("<Name of Missing Index, sysname,>", "[<Name of Missing Index, sysname,>]")]
    // A leading @ makes the parser read a variable, so it has to be bracketed even though the
    // general identifier rule allows @. SQL Server's own QUOTENAME agrees.
    [InlineData("@param", "[@param]")]
    // A leading # is fine unbracketed and bracketing does not change its meaning, so it stays bare.
    [InlineData("#temp", "#temp")]
    [InlineData("2ndPlace", "[2ndPlace]")]                 // leading digit
    [InlineData("a]b", "[a]]b]")]                          // embedded delimiter is doubled
    public void quote_name_brackets_what_needs_it(string name, string expected)
    {
        SchemaUtils.QuoteName(name).ShouldBe(expected);
    }

    /// <summary>
    ///     There is no "it already looks bracketed, leave it alone" shortcut, because a name that
    ///     merely starts with [ and ends with ] is not necessarily bracketed. Skipping the escape for
    ///     those let arbitrary DDL straight through.
    /// </summary>
    [Fact]
    public void a_name_that_looks_bracketed_is_still_escaped()
    {
        // A column or index genuinely named "[orders]" escapes to [[orders]]].
        SchemaUtils.QuoteName("[orders]").ShouldBe("[[orders]]]");
    }

    [Fact]
    public void a_name_carrying_ddl_cannot_escape_its_brackets()
    {
        var injection = "[ix] ON dbo.t(id); DROP TABLE dbo.victim; --]";

        var quoted = SchemaUtils.QuoteName(injection);

        // Every ] is doubled, so the name cannot terminate its own delimiter. The DROP text is
        // still present but is now inert content inside a single delimited identifier.
        quoted.ShouldBe("[[ix]] ON dbo.t(id); DROP TABLE dbo.victim; --]]]");
    }

    [Fact]
    public async Task an_index_name_carrying_ddl_does_not_execute_it()
    {
        await ResetSchema();
        await theConnection.CreateCommand("create table quoting.victim (id int not null)")
            .ExecuteNonQueryAsync();

        var table = new Table("quoting.host");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.Indexes.Add(new IndexDefinition("[ix] ON quoting.host(id); DROP TABLE quoting.victim; --]")
        {
            Columns = ["id"]
        });

        await CreateSchemaObjectInDatabase(table);

        // The whole point: the DROP was carried as part of an identifier, not run as a statement.
        // Identified rather than counted by name -- sys.tables spans every schema in the database.
        var victimStillThere = await theConnection
            .CreateCommand("select object_id('quoting.victim')")
            .ExecuteScalarAsync();

        victimStillThere.ShouldNotBe(DBNull.Value);
    }

    /// <summary>
    ///     Names that were already emitted bare must stay bare, and names that were always bracketed
    ///     must stay bracketed — an ordinary schema's DDL should be byte-identical to before.
    /// </summary>
    [Theory]
    [InlineData("Grüße")]
    [InlineData("price$")]
    [InlineData("_internal")]
    [InlineData("col#1")]
    public void a_regular_identifier_is_left_alone(string name)
    {
        SchemaUtils.QuoteName(name).ShouldBe(name);
    }

    [Fact]
    public void a_check_constraint_is_still_bracketed_as_it_always_was()
    {
        var table = new Table("quoting.item");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("price").NotNull();
        table.CheckConstraints.Add(new TableCheckConstraint("ck_price", "price > 0"));

        table.ToBasicCreateTableSql().ShouldContain("CONSTRAINT [ck_price] CHECK");
    }

    /// <summary>
    ///     Column lists were never quoted at all before this change, so a user whose column needed
    ///     brackets had to write them into the string themselves. Re-escaping those would name a
    ///     column that does not exist, so an entry that is already bracketed end to end is passed
    ///     through untouched.
    /// </summary>
    [Theory]
    [InlineData("price", "price")]           // ordinary, unchanged
    [InlineData("Table", "[Table]")]         // reserved word, previously emitted bare and broken
    [InlineData("[payload]", "[payload]")]   // caller already bracketed it, left alone
    [InlineData("[a]]b]", "[a]]b]")]         // properly escaped already, left alone
    [InlineData("unit price", "[unit price]")]
    public void quote_column_entry_passes_through_what_is_already_bracketed(string name, string expected)
    {
        SchemaUtils.QuoteColumnEntry(name).ShouldBe(expected);
    }

    /// <summary>
    ///     The pass-through must not become a hole. A lone <c>]</c> inside means the entry is not
    ///     a well-formed bracketed name, so it gets escaped on its own terms.
    /// </summary>
    [Fact]
    public void quote_column_entry_does_not_pass_through_an_unbalanced_name()
    {
        SchemaUtils.QuoteColumnEntry("[ix] ON dbo.t(id); DROP TABLE dbo.victim; --]")
            .ShouldBe("[[ix]] ON dbo.t(id); DROP TABLE dbo.victim; --]]]");
    }

    /// <summary>
    ///     A column list entry that was working before must still work. Master emitted these bare,
    ///     so the bracketed name resolved correctly.
    /// </summary>
    [Fact]
    public async Task an_index_on_a_column_the_user_bracketed_themselves_still_works()
    {
        await ResetSchema();

        var table = new Table("quoting.prebracket");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("Table", "varchar(50)").NotNull();
        // How a user had to spell it before column lists were quoted at all.
        table.Indexes.Add(new IndexDefinition("ix_prebracket") { Columns = ["[Table]"] });

        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);
        existing!.IndexFor("ix_prebracket")!.Columns.ShouldBe(["Table"]);
    }

    [Fact]
    public async Task an_index_whose_name_is_not_a_regular_identifier_can_be_created()
    {
        await ResetSchema();

        var table = new Table("quoting.doc");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("payload").NotNull();
        // SQL Server's own "missing index" template, pasted without filling in the name. Common.
        table.Indexes.Add(new IndexDefinition("<Name of Missing Index, sysname,>") { Columns = ["payload"] });

        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);
        existing!.IndexFor("<Name of Missing Index, sysname,>").ShouldNotBeNull();
    }

    [Fact]
    public async Task an_index_on_a_reserved_word_column_can_be_created()
    {
        await ResetSchema();

        var table = new Table("quoting.bookkeeping");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("Table", "varchar(50)").NotNull();
        table.Indexes.Add(new IndexDefinition("ix_bookkeeping_table") { Columns = ["Table"] });

        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);
        existing!.IndexFor("ix_bookkeeping_table")!.Columns.ShouldBe(["Table"]);
    }

    [Fact]
    public async Task a_primary_key_whose_name_contains_a_dot_can_be_created()
    {
        await ResetSchema();

        // The name EF6 gives its migration history table's key.
        var table = new Table("quoting.migration_history");
        table.AddColumn("MigrationId", "nvarchar(150)").AsPrimaryKey();
        table.PrimaryKeyName = "PK_dbo.__MigrationHistory";

        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);
        existing!.PrimaryKeyName.ShouldBe("PK_dbo.__MigrationHistory");
    }
}
