using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

/// <summary>
/// A widened character column used to be invisible to the differ: TableColumn.RawType() strips the
/// parenthesised part before comparing, so varchar(255) and varchar(1000) looked identical and an
/// existing table kept the narrow column forever, with only a hand-written ALTER to fix it. Reported
/// downstream as JasperFx/wolverine#4246, where a defaulted MySQL varchar(255) failed inserts with
/// "Data too long for column" and migrating the application did not widen it.
/// </summary>
public class detecting_widened_character_columns: IntegrationContext
{
    [Fact]
    public async Task detect_a_widened_varchar()
    {
        await DropTableAsync("`weasel_testing`.`widened_varchar`");

        var actual = new Table("weasel_testing.widened_varchar");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn("description", "varchar(255)");
        await actual.CreateAsync(theConnection);

        var expected = new Table("weasel_testing.widened_varchar");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn("description", "varchar(1000)");

        var delta = await expected.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task patching_widens_the_column_in_place()
    {
        await DropTableAsync("`weasel_testing`.`widened_varchar_patch`");

        var actual = new Table("weasel_testing.widened_varchar_patch");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn("description", "varchar(255)");
        await actual.CreateAsync(theConnection);

        await using (var seed = theConnection.CreateCommand(
                         "insert into `weasel_testing`.`widened_varchar_patch` (id, description) values (1, 'still here')"))
        {
            await seed.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var expected = new Table("weasel_testing.widened_varchar_patch");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn("description", "varchar(1000)");

        await expected.ApplyChangesAsync(theConnection);

        var afterwards = await expected.FetchExistingAsync(theConnection);
        afterwards!.ColumnFor("description")!.Type.ToUpperInvariant().ShouldBe("VARCHAR(1000)");

        // MODIFY COLUMN, not a rebuild -- the rows are still there.
        await using var check = theConnection.CreateCommand(
            "select description from `weasel_testing`.`widened_varchar_patch` where id = 1");
        (await check.ExecuteScalarAsync(TestContext.Current.CancellationToken)).ShouldBe("still here");

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_int_display_width_is_still_not_a_difference()
    {
        // The reason the size was stripped wholesale in the first place: MySQL 8 reports a bare INT for
        // a column declared int(11), so comparing sizes indiscriminately drifts on every schema check.
        await DropTableAsync("`weasel_testing`.`int_display_width`");

        var actual = new Table("weasel_testing.int_display_width");
        actual.AddColumn<int>("id").AsPrimaryKey();
        await actual.CreateAsync(theConnection);

        var expected = new Table("weasel_testing.int_display_width");
        expected.AddColumn("id", "int(11)").AsPrimaryKey();

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
