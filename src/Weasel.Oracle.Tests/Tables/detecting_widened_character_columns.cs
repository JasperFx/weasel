using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Tables;

/// <summary>
/// The Oracle twin of the MySQL and SQL Server tests by the same name. TableColumn.RawType() strips the
/// parenthesised part before comparing, so a model widened from VARCHAR2(500) to VARCHAR2(1000) used to
/// compare equal to the existing column and the differ never emitted the ALTER ... MODIFY. Reported
/// downstream as JasperFx/wolverine#4246.
/// </summary>
public class detecting_widened_character_columns: IntegrationContext
{
    public detecting_widened_character_columns() : base("widened")
    {
    }

    [Fact]
    public async Task detect_a_widened_varchar2()
    {
        await ResetSchema();

        var actual = new Table("widened.WIDENED_VARCHAR");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn("description", "VARCHAR2(500)");
        await CreateSchemaObjectInDatabase(actual);

        var expected = new Table("widened.WIDENED_VARCHAR");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn("description", "VARCHAR2(1000)");

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task patching_widens_the_column_and_settles()
    {
        await ResetSchema();

        var actual = new Table("widened.WIDENED_PATCH");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn("description", "VARCHAR2(500)");
        await CreateSchemaObjectInDatabase(actual);

        var expected = new Table("widened.WIDENED_PATCH");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn("description", "VARCHAR2(1000)");

        await expected.ApplyChangesAsync(theConnection);

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_number_precision_is_still_not_a_difference()
    {
        // The reason sizes are stripped in the first place -- NUMBER carries a precision, not a length.
        await ResetSchema();

        var actual = new Table("widened.NUMBER_PRECISION");
        actual.AddColumn("id", "NUMBER(10)").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(actual);

        var expected = new Table("widened.NUMBER_PRECISION");
        expected.AddColumn("id", "NUMBER").AsPrimaryKey();

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
