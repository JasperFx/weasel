using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     <see cref="SystemTextJsonSerializer" />'s reader overloads stream a JSON column straight into
///     the parser where the provider supports it, avoiding a full-size UTF-16 string that STJ would
///     only transcode back to UTF-8 (weasel#573). SqlClient is the provider that does <b>not</b>
///     support it: <c>SqlDataReader.GetStream</c> throws <c>InvalidCastException</c> on
///     <c>nvarchar(max)</c> and on SQL Server 2025's native <c>json</c> type — "can only be used on
///     columns of type Binary, Image, Udt or VarBinary".
/// </summary>
/// <remarks>
///     The unit tests in Weasel.Core.Tests cover the fallback with a stub reader. This covers it
///     against the real driver, because the stub is only worth anything if SqlClient still behaves
///     the way it was measured — a future SqlClient that starts streaming <c>json</c> would make the
///     stub a fiction, and this test is what would notice.
/// </remarks>
[Collection("integration")]
public class json_column_reads_fall_back_to_the_string_path
{
    public class Doc
    {
        public string? Name { get; set; }
        public int Number { get; set; }
        public string[]? Tags { get; set; }
    }

    private readonly SystemTextJsonSerializer theSerializer = new();

    [Fact]
    public async Task nvarchar_max_round_trips_through_the_fallback()
    {
        await assertRoundTripAsync("nvarchar(max)");
    }

    [Fact]
    public async Task native_json_round_trips_through_the_fallback()
    {
        if (!await supportsNativeJsonAsync())
        {
            // Pre-2025 servers have no json type; the nvarchar(max) case still covers the fallback.
            return;
        }

        await assertRoundTripAsync("json");
    }

    [Fact]
    public async Task get_stream_is_still_refused_for_a_json_column()
    {
        // The premise the fallback exists for. If this ever starts passing a stream back, the
        // capability probe in Weasel.Core will notice on its own and start streaming — but the
        // documentation claiming SqlClient cannot would then be wrong, and this is where it shows.
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await createAsync(conn, "nvarchar(max)");

        var query = conn.CreateCommand();
        query.CommandText = "select doc from dbo.weasel_json_probe where id = 1";

        await using var reader = await query.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        Should.Throw<InvalidCastException>(() => reader.GetStream(0));

        // And the column is still readable afterwards, which is what makes try-then-fall-back safe.
        reader.GetString(0).ShouldContain("streamed or not");
    }

    private async Task assertRoundTripAsync(string columnType)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await createAsync(conn, columnType);

        var query = conn.CreateCommand();
        query.CommandText = "select doc from dbo.weasel_json_probe where id = 1";

        await using (var reader = await query.ExecuteReaderAsync())
        {
            (await reader.ReadAsync()).ShouldBeTrue();

            var sync = theSerializer.FromJson<Doc>(reader, 0);
            sync.Name.ShouldBe("streamed or not");
            sync.Number.ShouldBe(99);
            sync.Tags.ShouldBe(["a", "b"]);

            theSerializer.FromJson(typeof(Doc), reader, 0)
                .ShouldBeOfType<Doc>().Number.ShouldBe(99);
        }

        await using (var reader = await query.ExecuteReaderAsync())
        {
            (await reader.ReadAsync()).ShouldBeTrue();

            var async = await theSerializer.FromJsonAsync<Doc>(reader, 0, TestContext.Current.CancellationToken);
            async.Name.ShouldBe("streamed or not");
            async.Tags.ShouldBe(["a", "b"]);

            (await theSerializer.FromJsonAsync(typeof(Doc), reader, 0, TestContext.Current.CancellationToken))
                .ShouldBeOfType<Doc>().Number.ShouldBe(99);
        }
    }

    private async Task createAsync(SqlConnection conn, string columnType)
    {
        await executeAsync(conn, "if object_id('dbo.weasel_json_probe') is not null drop table dbo.weasel_json_probe");
        await executeAsync(conn, $"create table dbo.weasel_json_probe (id int primary key, doc {columnType})");

        var insert = conn.CreateCommand();
        insert.CommandText = "insert into dbo.weasel_json_probe (id, doc) values (1, @doc)";
        insert.Parameters.AddWithValue("@doc",
            theSerializer.ToJson(new Doc { Name = "streamed or not", Number = 99, Tags = ["a", "b"] }));
        await insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<bool> supportsNativeJsonAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        try
        {
            await executeAsync(conn, "declare @probe json = '{}'");
            return true;
        }
        catch (SqlException)
        {
            return false;
        }
    }

    private static async Task executeAsync(SqlConnection conn, string sql)
    {
        var command = conn.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
