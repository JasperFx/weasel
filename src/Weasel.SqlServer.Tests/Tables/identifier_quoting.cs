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
    ///     Weasel emitted most identifiers bare until 9.25, so bracketing the name yourself was the
    ///     only way to use one that needed delimiting. Re-escaping those would silently rename the
    ///     object, so a properly bracketed name is passed through.
    /// </summary>
    [Theory]
    [InlineData("[orders]", "[orders]")]     // caller bracketed an ordinary name
    [InlineData("[Table]", "[Table]")]       // caller bracketed a reserved word
    [InlineData("[unit price]", "[unit price]")]
    [InlineData("[a]]b]", "[a]]b]")]         // interior delimiter already doubled
    public void a_name_the_caller_already_bracketed_is_left_alone(string name, string expected)
    {
        SchemaUtils.QuoteName(name).ShouldBe(expected);
    }

    /// <summary>
    ///     The pass-through must not become a hole. A lone <c>]</c> inside means the name is not a
    ///     well-formed bracketed identifier, so it is escaped on its own terms.
    /// </summary>
    [Theory]
    [InlineData("[a]b]", "[[a]]b]]]")]
    [InlineData("[unbalanced", "[[unbalanced]")]
    [InlineData("unbalanced]", "[unbalanced]]]")]
    public void a_name_that_is_not_properly_bracketed_is_still_escaped(string name, string expected)
    {
        SchemaUtils.QuoteName(name).ShouldBe(expected);
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
    ///     The pass-through used to apply to column list entries only, which left the workaround
    ///     working in one half of a statement and renaming the object in the other: an index
    ///     declared over <c>[Table]</c> resolved, while the index's own pre-bracketed name did not.
    ///     One rule now covers every name Weasel writes.
    /// </summary>
    [Fact]
    public void the_pass_through_covers_names_and_column_lists_alike()
    {
        var table = new Table("quoting.consistent");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("[Table]", "varchar(50)");

        var index = new IndexDefinition("[ix consistent]") { Columns = ["[Table]"] };
        index.AddIncludedColumn("[Table]");

        var ddl = index.ToDDL(table);

        // The column list honoured the caller's brackets before; the index's own name did not.
        ddl.ShouldContain("INDEX [ix consistent] ON");
        ddl.ShouldContain("([Table])");
        ddl.ShouldContain("INCLUDE ([Table])");
        table.ToBasicCreateTableSql().ShouldContain("[Table] varchar(50)");
    }

    /// <summary>
    ///     A check constraint name was always bracketed and still is, so ordinary DDL is unchanged
    ///     — but it honours the same pass-through as everything else now.
    /// </summary>
    [Fact]
    public void a_check_constraint_name_the_caller_bracketed_is_not_double_bracketed()
    {
        var table = new Table("quoting.checked_item");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("price").NotNull();
        table.CheckConstraints.Add(new TableCheckConstraint("[ck price]", "price > 0"));

        table.ToBasicCreateTableSql().ShouldContain("CONSTRAINT [ck price] CHECK");
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
    public async Task an_index_whose_name_the_user_bracketed_themselves_still_works()
    {
        await ResetSchema();

        var table = new Table("quoting.prebracket_name");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("payload").NotNull();
        // The other half of the workaround: bracketing the index's own name. Escaping this one
        // created an index called "[ix prebracket]", brackets and all, and it drifted forever.
        table.Indexes.Add(new IndexDefinition("[ix prebracket]") { Columns = ["payload"] });

        await CreateSchemaObjectInDatabase(table);

        // Created under the name the caller meant, rather than one called "[ix prebracket]".
        var existing = await table.FetchExistingAsync(theConnection);
        existing!.IndexFor("ix prebracket").ShouldNotBeNull();
        existing.IndexFor("[ix prebracket]").ShouldBeNull();

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Emitting the caller's brackets is only half of it. The database reports the bare name
    ///     back, so a model still holding the bracketed spelling never pairs with it and the table
    ///     drifts on every check — which is what happened before names were normalized on the way
    ///     in. Every kind of name a caller can bracket is covered here at once.
    /// </summary>
    [Fact]
    public async Task a_table_named_entirely_in_brackets_round_trips_without_drift()
    {
        await ResetSchema();

        var parent = new Table("quoting.bracket_parent");
        parent.AddColumn<int>("id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(parent);

        var table = new Table("quoting.bracket_child");
        table.AddColumn("[id]", "int").AsPrimaryKey();
        table.AddColumn("[Table]", "varchar(50)").NotNull();
        table.AddColumn("[parent_id]", "int");
        table.PrimaryKeyName = "[pk bracket child]";
        table.CheckConstraints.Add(new TableCheckConstraint("[ck not empty]", "[Table] <> ''"));

        var index = new IndexDefinition("[ix bracket child]") { Columns = ["[Table]"] };
        index.AddIncludedColumn("[parent_id]");
        table.Indexes.Add(index);

        table.ForeignKeys.Add(new ForeignKey("[fk bracket child parent]")
        {
            ColumnNames = ["[parent_id]"],
            LinkedNames = ["[id]"],
            LinkedTable = parent.Identifier
        });

        await CreateSchemaObjectInDatabase(table);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        // And the model holds what the database reports, not the spelling it was handed.
        var existing = await table.FetchExistingAsync(theConnection);
        existing!.PrimaryKeyName.ShouldBe("pk bracket child");
        existing.IndexFor("ix bracket child")!.Columns.ShouldBe(["Table"]);
        existing.ForeignKeys.Single().Name.ShouldBe("fk bracket child parent");
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
