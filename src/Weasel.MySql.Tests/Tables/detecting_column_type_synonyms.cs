using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

/// <summary>
///     MySQL rewrites a type synonym when it stores the column: <c>BOOLEAN</c> comes back from
///     <c>information_schema.COLUMNS.COLUMN_TYPE</c> as <c>tinyint(1)</c>, <c>INTEGER</c> as <c>int</c>,
///     <c>NUMERIC(13,4)</c> as <c>decimal(13,4)</c>. The column comparison used the declared spelling
///     verbatim, so a model written with any synonym reported drift on every check, and every migration
///     re-issued a <c>MODIFY COLUMN</c> that changed nothing.
/// </summary>
public class detecting_column_type_synonyms: IntegrationContext
{
    private static Table synonymsTable(string name)
    {
        var table = new Table($"weasel_testing.{name}");
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("is_durable", "BOOLEAN").NotNull();
        table.AddColumn("flag", "BOOL");
        table.AddColumn("amount", "NUMERIC(13,4)");
        table.AddColumn("fee", "DEC(10,2)");
        table.AddColumn("ratio", "DOUBLE PRECISION");
        table.AddColumn("score", "REAL");
        table.AddColumn("wide_float", "FLOAT(53)");
        table.AddColumn("counter", "INT(10) UNSIGNED");
        table.AddColumn("big_counter", "BIGINT(19)");
        table.AddColumn("tiny", "INT1");
        table.AddColumn("medium", "MIDDLEINT");
        table.AddColumn("label", "CHARACTER VARYING(100)");
        table.AddColumn("code", "NATIONAL CHAR(3)");
        table.AddColumn("nickname", "NCHAR VARYING(20)");
        table.AddColumn("alias", "VARCHARACTER(20)");
        table.AddColumn("notes", "LONG VARCHAR");
        return table;
    }

    [Fact]
    public async Task a_table_declared_with_synonyms_matches_itself_once_created()
    {
        await DropTableAsync("`weasel_testing`.`type_synonyms`");

        var table = synonymsTable("type_synonyms");
        await table.CreateAsync(theConnection, TestContext.Current.CancellationToken);

        var delta = await table.FindDeltaAsync(theConnection, TestContext.Current.CancellationToken);

        delta.Columns!.Different.Select(x => x.Expected.Name).ShouldBeEmpty();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task applying_a_model_declared_with_synonyms_settles()
    {
        await DropTableAsync("`weasel_testing`.`type_synonyms_applied`");

        var table = synonymsTable("type_synonyms_applied");
        await table.ApplyChangesAsync(theConnection, TestContext.Current.CancellationToken);
        await table.ApplyChangesAsync(theConnection, TestContext.Current.CancellationToken);

        (await table.FindDeltaAsync(theConnection, TestContext.Current.CancellationToken))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_synonym_does_not_hide_a_real_type_change()
    {
        await DropTableAsync("`weasel_testing`.`type_synonyms_changed`");

        await CreateTableAsync(
            "CREATE TABLE `weasel_testing`.`type_synonyms_changed` "
            + "(id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(50) NULL, counter INT UNSIGNED NULL)");

        var expected = new Table("weasel_testing.type_synonyms_changed");
        expected.AddColumn("id", "INTEGER").AsPrimaryKey();
        expected.AddColumn("label", "CHARACTER VARYING(200)");
        expected.AddColumn("counter", "INTEGER");

        var delta = await expected.FindDeltaAsync(theConnection, TestContext.Current.CancellationToken);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns!.Different.Select(x => x.Expected.Name).OrderBy(x => x)
            .ShouldBe(["counter", "id", "label"]);
    }
}
