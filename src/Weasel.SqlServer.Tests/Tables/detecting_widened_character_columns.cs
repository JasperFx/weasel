using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
/// The SQL Server twin of the MySQL test by the same name. TableColumn.RawType() strips the
/// parenthesised part before comparing, so a model widened from varchar(500) to varchar(1000) used to
/// compare equal to the existing column and the differ never emitted the ALTER. Reported downstream as
/// JasperFx/wolverine#4246.
/// </summary>
[Collection("integration")]
public class detecting_widened_character_columns: IntegrationContext
{
    public detecting_widened_character_columns() : base("widened")
    {
    }

    [Fact]
    public async Task detect_a_widened_varchar()
    {
        await ResetSchema();

        var actual = new Table("widened.widened_varchar");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn("description", "varchar(500)");
        await CreateSchemaObjectInDatabase(actual);

        var expected = new Table("widened.widened_varchar");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn("description", "varchar(1000)");

        var delta = await expected.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task patching_widens_the_column_and_settles()
    {
        await ResetSchema();

        var actual = new Table("widened.widened_varchar_patch");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn("description", "varchar(500)");
        await CreateSchemaObjectInDatabase(actual);

        var expected = new Table("widened.widened_varchar_patch");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn("description", "varchar(1000)");

        await expected.ApplyChangesAsync(theConnection);

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_decimal_precision_is_still_not_a_difference()
    {
        // The reason the size is stripped wholesale: comparing every parenthesised size indiscriminately
        // would drift on precisions and scales the model spells differently from the catalog.
        await ResetSchema();

        var actual = new Table("widened.decimal_precision");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn("amount", "decimal(18,2)");
        await CreateSchemaObjectInDatabase(actual);

        var expected = new Table("widened.decimal_precision");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn("amount", "decimal");

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
