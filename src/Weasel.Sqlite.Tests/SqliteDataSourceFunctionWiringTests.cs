using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Sqlite.Functions;
using Xunit;

namespace Weasel.Sqlite.Tests;

/// <summary>
///     SqliteDataSource applies the function registry and extension settings on every connection
///     it opens, sync and async, and keeps doing so for functions added after construction.
///     See JasperFx/weasel#588.
/// </summary>
public class SqliteDataSourceFunctionWiringTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"weasel-fn-{Guid.NewGuid():N}.db");

    private string ConnectionString => new SqliteConnectionStringBuilder { DataSource = _path }.ToString();

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        if (File.Exists(_path)) File.Delete(_path);
    }

    [Fact]
    public async Task functions_are_registered_on_every_async_open()
    {
        var functions = new SqliteFunctionRegistry();
        functions.AddScalar<long>("twice", (object? x) => Convert.ToInt64(x) * 2);

        using var dataSource = new SqliteDataSource(ConnectionString, null, functions, null);

        await using var first = await dataSource.OpenConnectionAsync(TestContext.Current.CancellationToken);
        await using var second = await dataSource.OpenConnectionAsync(TestContext.Current.CancellationToken);

        (await scalarAsync(first, "select twice(21)")).ShouldBe(42L);
        (await scalarAsync(second, "select twice(4)")).ShouldBe(8L);
    }

    [Fact]
    public void functions_are_registered_on_a_sync_open()
    {
        var functions = new SqliteFunctionRegistry();
        functions.AddScalar<string>("shout", (object? x) => x?.ToString()?.ToUpperInvariant() ?? "");

        using var dataSource = new SqliteDataSource(ConnectionString, null, functions, null);

        using var conn = dataSource.OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "select shout('weasel')";
        cmd.ExecuteScalar().ShouldBe("WEASEL");
    }

    [Fact]
    public async Task functions_added_after_construction_reach_later_opens()
    {
        using var dataSource = new SqliteDataSource(ConnectionString);

        dataSource.Functions.AddScalar<double>("cosine_distance",
            (object? a, object? b) => 1 - Convert.ToDouble(a) * Convert.ToDouble(b));

        await using var conn = await dataSource.OpenConnectionAsync(TestContext.Current.CancellationToken);
        (await scalarAsync(conn, "select cosine_distance(0.5, 1.0)")).ShouldBe(0.5);
    }

    [Fact]
    public async Task aggregate_functions_are_registered_too()
    {
        var functions = new SqliteFunctionRegistry();
        functions.AddAggregate<long, long>("sum_of_squares", 0L, (acc, x) => acc + (long)Math.Pow(Convert.ToInt64(x), 2));

        using var dataSource = new SqliteDataSource(ConnectionString, null, functions, null);

        await using var conn = await dataSource.OpenConnectionAsync(TestContext.Current.CancellationToken);
        (await scalarAsync(conn, "select sum_of_squares(value) from (select 1 as value union all select 2 union all select 3)"))
            .ShouldBe(14L);
    }

    [Fact]
    public async Task a_function_from_one_data_source_is_not_visible_through_a_bare_connection()
    {
        // Pins the reason the wiring lives in the data source: functions are per connection.
        var functions = new SqliteFunctionRegistry();
        functions.AddScalar<long>("twice", (object? x) => Convert.ToInt64(x) * 2);
        using var dataSource = new SqliteDataSource(ConnectionString, null, functions, null);
        await using var _ = await dataSource.OpenConnectionAsync(TestContext.Current.CancellationToken);

        await using var bare = new SqliteConnection(ConnectionString);
        await bare.OpenAsync(TestContext.Current.CancellationToken);

        await Should.ThrowAsync<SqliteException>(() => scalarAsync(bare, "select twice(1)"));
    }

    [Fact]
    public void the_two_argument_constructor_still_works_with_empty_registry_and_settings()
    {
        using var dataSource = new SqliteDataSource(ConnectionString, SqlitePragmaSettings.Default);

        dataSource.Functions.Functions.ShouldBeEmpty();
        dataSource.Extensions.Extensions.ShouldBeEmpty();

        using var conn = dataSource.OpenConnection();
        conn.State.ShouldBe(System.Data.ConnectionState.Open);
    }

    [Fact]
    public async Task a_failing_extension_load_fails_the_open_and_names_the_library()
    {
        var extensions = new SqliteExtensionSettings();
        extensions.AddExtension("/definitely/not/here/libnope");

        using var dataSource = new SqliteDataSource(ConnectionString, null, null, extensions);

        var ex = await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            await using var conn = await dataSource.OpenConnectionAsync(TestContext.Current.CancellationToken);
        });

        ex.Message.ShouldContain("libnope");
    }

    [Fact]
    public void a_failing_extension_load_fails_a_sync_open_too()
    {
        var extensions = new SqliteExtensionSettings();
        extensions.AddExtension("/definitely/not/here/libnope");

        using var dataSource = new SqliteDataSource(ConnectionString, null, null, extensions);

        Should.Throw<InvalidOperationException>(() => dataSource.OpenConnection());
    }

    private static async Task<object?> scalarAsync(System.Data.Common.DbConnection conn, string sql)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
    }
}
