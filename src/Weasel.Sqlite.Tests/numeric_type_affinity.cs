using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests;

/// <summary>
///     SQLite gives a declared type REAL affinity only when it contains REAL, FLOA or DOUB.
///     NUMERIC and DECIMAL get NUMERIC affinity, which stores a whole number as an integer.
/// </summary>
public class numeric_type_affinity
{
    [Theory]
    [InlineData("numeric", "NUMERIC")]
    [InlineData("decimal", "NUMERIC")]
    [InlineData("real", "REAL")]
    [InlineData("float", "REAL")]
    [InlineData("double", "REAL")]
    public void a_declared_type_keeps_its_affinity(string declared, string expected)
    {
        SqliteProvider.Instance.ConvertSynonyms(declared).ShouldBe(expected);
    }

    [Fact]
    public async Task a_numeric_column_stores_a_whole_number_as_an_integer()
    {
        await using var conn = new SqliteConnection($"Data Source={Path.GetTempFileName()};");
        await conn.OpenAsync();
        await conn.ResetSchemaAsync("main");

        var table = new Table("amounts");
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("quantity", "numeric");

        var writer = new StringWriter();
        table.WriteCreateStatement(new SqliteMigrator(), writer);

        var create = conn.CreateCommand();
        create.CommandText = writer.ToString();
        await create.ExecuteNonQueryAsync();

        var insert = conn.CreateCommand();
        insert.CommandText = "insert into amounts (id, quantity) values (1, 1)";
        await insert.ExecuteNonQueryAsync();

        var query = conn.CreateCommand();
        query.CommandText = "select typeof(quantity) from amounts";
        (await query.ExecuteScalarAsync()).ShouldBe("integer");
    }
}
