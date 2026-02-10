using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Sqlite.Functions;
using Xunit;

namespace Weasel.Sqlite.Tests.Functions;

public class SqliteFunctionTests
{
    [Fact]
    public void scalar_function_validates_name()
    {
        Should.Throw<ArgumentException>(() => new ScalarFunction<int>(null!, () => 42));
        Should.Throw<ArgumentException>(() => new ScalarFunction<int>("", () => 42));
        Should.Throw<ArgumentException>(() => new ScalarFunction<int>("   ", () => 42));
    }

    [Fact]
    public void scalar_function_validates_delegate()
    {
        Should.Throw<ArgumentNullException>(() => new ScalarFunction<int>("test", (Func<int>)null!));
        Should.Throw<ArgumentNullException>(() => new ScalarFunction<int>("test", (Func<object?, int>)null!));
    }

    [Fact]
    public async Task scalar_function_with_no_parameters()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var function = new ScalarFunction<int>("get_answer", () => 42);
        function.Register(connection);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT get_answer()";
        var result = await cmd.ExecuteScalarAsync();

        result.ShouldBe(42L); // SQLite returns INTEGER as long
    }

    [Fact]
    public async Task scalar_function_with_two_parameters()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var function = new ScalarFunction<int>("add_numbers", (object? x, object? y) =>
        {
            var a = Convert.ToInt32(x);
            var b = Convert.ToInt32(y);
            return a + b;
        });
        function.Register(connection);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT add_numbers(10, 32)";
        var result = await cmd.ExecuteScalarAsync();

        result.ShouldBe(42L);
    }

    [Fact]
    public async Task scalar_function_with_string_result()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var function = new ScalarFunction<string>("greet", (object? name) =>
        {
            return $"Hello, {name}!";
        });
        function.Register(connection);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT greet('World')";
        var result = await cmd.ExecuteScalarAsync();

        result.ShouldBe("Hello, World!");
    }

    [Fact]
    public async Task aggregate_function_sum()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        // Create test table
        await using var createCmd = connection.CreateCommand();
        createCmd.CommandText = "CREATE TABLE numbers (value INTEGER)";
        await createCmd.ExecuteNonQueryAsync();

        // Insert test data
        await using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO numbers VALUES (10), (20), (12)";
        await insertCmd.ExecuteNonQueryAsync();

        // Register aggregate function
        var function = new AggregateFunction<int, int>(
            "my_sum",
            0,  // seed
            (accumulator, value) => accumulator + Convert.ToInt32(value)  // aggregate
        );
        function.Register(connection);

        await using var queryCmd = connection.CreateCommand();
        queryCmd.CommandText = "SELECT my_sum(value) FROM numbers";
        var result = await queryCmd.ExecuteScalarAsync();

        result.ShouldBe(42L);
    }

    [Fact]
    public async Task aggregate_function_string_concat()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        // Create test table
        await using var createCmd = connection.CreateCommand();
        createCmd.CommandText = "CREATE TABLE words (text TEXT)";
        await createCmd.ExecuteNonQueryAsync();

        // Insert test data
        await using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO words VALUES ('Hello'), (' '), ('World')";
        await insertCmd.ExecuteNonQueryAsync();

        // Register aggregate function
        var function = new AggregateFunction<string, string>(
            "concat_all",
            "",  // seed
            (accumulator, value) => accumulator + value?.ToString()  // aggregate
        );
        function.Register(connection);

        await using var queryCmd = connection.CreateCommand();
        queryCmd.CommandText = "SELECT concat_all(text) FROM words";
        var result = await queryCmd.ExecuteScalarAsync();

        result.ShouldBe("Hello World");
    }

    [Fact]
    public async Task function_is_deterministic_by_default()
    {
        var function = new ScalarFunction<int>("test", () => 42);
        function.IsDeterministic.ShouldBeTrue();
    }

    [Fact]
    public async Task can_set_function_as_non_deterministic()
    {
        var function = new ScalarFunction<int>("random_func", () => new Random().Next())
        {
            IsDeterministic = false
        };

        function.IsDeterministic.ShouldBeFalse();
    }
}
