using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Sqlite.Tests;

/// <summary>
///     weasel#863: Microsoft.Data.Sqlite's <c>GetStream</c> on a column with no origin table, beside
///     another computed column, calls <c>sqlite3_table_column_metadata</c> with a null table name and
///     crashes the process with a SIGSEGV. The serializer's reader overloads must read the string
///     instead. Before the fix each of these tests took the whole test host down.
/// </summary>
public class json_from_a_computed_column
{
    private const string TwoComputedColumns =
        "select json_object('name', 'Shiner', 'number', 2) as first, json_object('name', 'Bock', 'number', 3) as second";

    private readonly SystemTextJsonSerializer theSerializer = new();

    [Fact]
    public async Task read_both_computed_columns_by_type()
    {
        await using var conn = new SqliteConnection("Data Source=:memory:");
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = TwoComputedColumns;
        await using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        var first = (Beer)theSerializer.FromJson(typeof(Beer), reader, 0);
        var second = (Beer)theSerializer.FromJson(typeof(Beer), reader, 1);

        first.ShouldBe(new Beer("Shiner", 2));
        second.ShouldBe(new Beer("Bock", 3));
    }

    [Fact]
    public async Task read_both_computed_columns_generically()
    {
        await using var conn = new SqliteConnection("Data Source=:memory:");
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = TwoComputedColumns;
        await using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        theSerializer.FromJson<Beer>(reader, 1).ShouldBe(new Beer("Bock", 3));
        theSerializer.FromJson<Beer>(reader, 0).ShouldBe(new Beer("Shiner", 2));
    }

    [Fact]
    public async Task read_both_computed_columns_asynchronously()
    {
        await using var conn = new SqliteConnection("Data Source=:memory:");
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = TwoComputedColumns;
        await using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        (await theSerializer.FromJsonAsync<Beer>(reader, 0)).ShouldBe(new Beer("Shiner", 2));
        ((Beer)await theSerializer.FromJsonAsync(typeof(Beer), reader, 1)).ShouldBe(new Beer("Bock", 3));
    }

    [Fact]
    public async Task a_json_column_from_a_table_still_reads()
    {
        await using var conn = new SqliteConnection("Data Source=:memory:");
        await conn.OpenAsync();
        await using (var create = conn.CreateCommand())
        {
            create.CommandText =
                "create table beers (id integer primary key, data text not null); " +
                "insert into beers (data) values ('{\"name\":\"Lone Star\",\"number\":4}');";
            await create.ExecuteNonQueryAsync();
        }

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select data, json_object('name', 'Shiner', 'number', 2) from beers";
        await using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        theSerializer.FromJson<Beer>(reader, 0).ShouldBe(new Beer("Lone Star", 4));
        theSerializer.FromJson<Beer>(reader, 1).ShouldBe(new Beer("Shiner", 2));
    }

    public record Beer(string Name, int Number);
}
