using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

/// <summary>
///     MySQL's <c>QuoteName</c> was <c>$"`{name}`"</c> with no doubling, so a name carrying a
///     backtick closed its own quoting, and 27 further sites interpolated backticks inline without
///     going through the helper at all. See weasel#447.
/// </summary>
public class identifier_quoting: IntegrationContext
{
    [Fact]
    public void an_embedded_backtick_is_doubled_rather_than_closing_the_quoting()
    {
        SchemaUtils.QuoteName("we`ird").ShouldBe("`we``ird`");
        SchemaUtils.Unquote("`we``ird`").ShouldBe("we`ird");
    }

    [Fact]
    public void a_name_carrying_ddl_stays_inside_its_delimiters()
    {
        var injection = "ix` ON t(id); DROP TABLE victim; --";

        var quoted = SchemaUtils.QuoteName(injection);

        // The backtick is doubled, so the name cannot terminate its own delimiter. The DROP text
        // survives as inert content inside one identifier.
        quoted.ShouldBe("`ix`` ON t(id); DROP TABLE victim; --`");
        SchemaUtils.Unquote(quoted).ShouldBe(injection);
    }

    [Fact]
    public void a_name_the_caller_already_quoted_is_left_alone()
    {
        SchemaUtils.QuoteName("`orders`").ShouldBe("`orders`");
        SchemaUtils.QuoteName("`we``ird`").ShouldBe("`we``ird`");
    }

    /// <summary>
    ///     The identity half. Emitting the caller's backticks untouched still leaves the model
    ///     holding the quoted spelling while the catalog reports the bare name, so the two never
    ///     compare equal and the table drifts on every check — the same defect weasel#446 fixed for
    ///     SQL Server.
    /// </summary>
    [Fact]
    public void names_the_caller_quoted_are_normalized_on_the_way_into_the_model()
    {
        var table = new Table("weasel_testing.quoting_normalized");
        table.AddColumn("`id`", "int").AsPrimaryKey();
        table.AddColumn("`order date`", "datetime");

        table.Columns[0].Name.ShouldBe("id");
        table.Columns[1].Name.ShouldBe("order date");

        var index = new IndexDefinition("`ix quoting`");
        index.AgainstColumns("`order date`");

        index.Name.ShouldBe("ix quoting");
        index.Columns.ShouldBe(["order date"]);

        new ForeignKey("`fk quoting`").Name.ShouldBe("fk quoting");
    }

    [Fact]
    public async Task a_table_named_entirely_in_backticks_round_trips_without_drift()
    {
        await DropTableAsync("`weasel_testing`.`quoting_round_trip`");

        var table = new Table("weasel_testing.quoting_round_trip");
        table.AddColumn("`id`", "int").AsPrimaryKey();
        table.AddColumn("`Table`", "varchar(50)");     // reserved word, quoted by the caller

        var index = new IndexDefinition("`ix_quoting_round_trip`");
        index.AgainstColumns("`Table`");
        table.Indexes.Add(index);

        await table.CreateAsync(theConnection);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        // And the model holds what the database reports, not the spelling it was handed.
        var existing = await table.FetchExistingAsync(theConnection);
        existing!.Columns.Select(x => x.Name).ShouldContain("Table");
        existing.Indexes.Single().Name.ShouldBe("ix_quoting_round_trip");
    }
}
