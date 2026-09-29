using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     A character length the model declares is compared, in characters -- the catalog's
///     <c>RDB$CHARACTER_LENGTH</c>, not its byte length, which is four times as long in a UTF8 database.
/// </summary>
public class detecting_widened_character_columns: IntegrationContext
{
    private static Table notes(string type)
    {
        var table = new Table("notes");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("body", type);
        return table;
    }

    [Theory]
    [InlineData("VARCHAR(20)", "VARCHAR(80)")]
    [InlineData("CHAR(20)", "CHAR(80)")]
    [InlineData("VARCHAR(20) CHARACTER SET UTF8", "VARCHAR(80) CHARACTER SET UTF8")]
    [InlineData("SMALLINT", "BIGINT")]
    [InlineData("NUMERIC(9,2)", "NUMERIC(18,2)")]
    public async Task a_widened_column_is_altered_in_place_and_keeps_its_data(string before, string after)
    {
        await CreateSchemaObjectInDatabase(notes(before));
        await ExecuteAsync("INSERT INTO notes (id, body) VALUES (1, '12')");

        var widened = notes(after);
        var delta = await widened.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Different.Single().Expected.Name.ShouldBe("body");

        await ApplyAsync(widened);

        (await widened.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await ScalarAsync<string>("SELECT CAST(body AS VARCHAR(20)) FROM notes")).Trim().ShouldStartWith("12");
    }

    [Theory]
    [InlineData("VARCHAR(80)", "VARCHAR(20)")]
    [InlineData("BIGINT", "SMALLINT")]
    [InlineData("VARCHAR(20)", "INTEGER")]
    [InlineData("VARCHAR(20)", "VARCHAR(20) COLLATE UNICODE_CI")]
    public async Task a_change_firebird_cannot_make_in_place_is_refused_before_anything_runs(string before, string after)
    {
        await CreateSchemaObjectInDatabase(notes(before));

        var changed = notes(after);
        changed.AddColumn<int>("added");

        var delta = await changed.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);

        await Should.ThrowAsync<SchemaMigrationException>(() => ApplyAsync(AutoCreate.CreateOrUpdate, changed));

        (await changed.FetchExistingAsync(theConnection))!.HasColumn("added").ShouldBeFalse();
    }

    /// <summary>
    ///     The measurements behind the refusal: each change above is one Firebird rejects.
    /// </summary>
    [Theory]
    [InlineData("VARCHAR(80)", "VARCHAR(20)", 336068816)]
    [InlineData("BIGINT", "SMALLINT", 336068817)]
    [InlineData("VARCHAR(20)", "INTEGER", 336068818)]
    public async Task firebird_really_refuses_those_changes(string before, string after, int error)
    {
        await CreateSchemaObjectInDatabase(notes(before));

        var ex = await Should.ThrowAsync<FirebirdSql.Data.FirebirdClient.FbException>(() =>
            ExecuteAsync($"ALTER TABLE notes ALTER body TYPE {after}"));

        FirebirdMigrator.HasErrorNumber(ex, error).ShouldBeTrue(ex.Message);
    }

    [Fact]
    public async Task the_length_is_read_in_characters_in_a_utf8_database()
    {
        await CreateSchemaObjectInDatabase(notes("VARCHAR(100)"));

        var existing = await notes("VARCHAR(100)").FetchExistingAsync(theConnection);

        existing!.ColumnFor("body")!.Type.ShouldBe("VARCHAR(100) CHARACTER SET UTF8");
    }
}
