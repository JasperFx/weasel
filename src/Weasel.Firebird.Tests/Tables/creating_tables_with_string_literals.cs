using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     Every CREATE runs inside an EXECUTE STATEMENT literal, so everything a model can put in a
///     statement -- a quote, a semicolon, a caret, a very long definition -- has to survive being
///     written into a string and split back out of a script (weasel#643, weasel#624).
/// </summary>
public class creating_tables_with_string_literals: IntegrationContext
{
    private async Task<string?> defaultOf(string column)
        => (await ListAsync(
                $"SELECT CAST(RDB$DEFAULT_SOURCE AS VARCHAR(200)) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = 'NOTES' AND RDB$FIELD_NAME = '{column}'"))
            .SingleOrDefault();

    [Theory]
    [InlineData("it's", "DEFAULT 'it''s'")]
    [InlineData("a;b", "DEFAULT 'a;b'")]
    [InlineData("a^b", "DEFAULT 'a^b'")]
    [InlineData("'';--", "DEFAULT ''''';--'")]
    [InlineData("SET TERM ^ ;", "DEFAULT 'SET TERM ^ ;'")]
    public async Task a_string_default_reaches_the_catalog_exactly(string value, string stored)
    {
        var table = new Table("notes");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("body").DefaultValueByString(value);

        await CreateSchemaObjectInDatabase(table);

        (await defaultOf("BODY")).ShouldBe(stored);

        await ExecuteAsync("INSERT INTO notes (id) VALUES (1)");
        (await ScalarAsync<string>("SELECT body FROM notes")).ShouldBe(value);
    }

    [Fact]
    public async Task a_string_default_round_trips_with_no_delta_under_drift_detection()
    {
        var table = new Table("notes");
        table.DetectColumnDrift = true;
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("body").DefaultValueByString("it's; ^ fine");

        await CreateSchemaObjectInDatabase(table);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_quote_in_a_default_survives_the_migration_path()
    {
        var table = new Table("notes");
        table.AddColumn<int>("id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(table);

        table.AddColumn<string>("body").DefaultValueByString("O'Brien");
        await ApplyAsync(table);

        (await defaultOf("BODY")).ShouldBe("DEFAULT 'O''Brien'");
    }

    /// <summary>
    ///     A3: a UTF8 attachment caps one literal at 16,383 characters, so a longer CREATE TABLE is
    ///     written as several literals joined with <c>||</c>.
    /// </summary>
    [Fact]
    public async Task a_create_table_longer_than_one_literal_allows_is_created()
    {
        var table = new Table("wide");
        table.AddColumn<int>("id").AsPrimaryKey();
        for (var i = 0; i < 700; i++)
        {
            table.AddColumn($"column_with_a_long_name_{i:D3}", "VARCHAR(10)").DefaultValueByString("x");
        }

        var writer = new StringWriter();
        table.WriteCreateStatement(new FirebirdMigrator(), writer);
        writer.ToString().Length.ShouldBeGreaterThan(40000);

        await CreateSchemaObjectInDatabase(table);

        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = 'WIDE'"))
            .ShouldBe(701);
        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_multibyte_default_is_kept()
    {
        var table = new Table("notes");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("body").DefaultValueByString("Grüße, 世界");

        await CreateSchemaObjectInDatabase(table);
        await ExecuteAsync("INSERT INTO notes (id) VALUES (1)");

        (await ScalarAsync<string>("SELECT body FROM notes")).ShouldBe("Grüße, 世界");
    }
}
