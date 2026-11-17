using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Sqlite.Functions;
using Xunit;

namespace Weasel.Sqlite.Tests.Functions;

public class SqliteFunctionRegistryTests
{
    [Fact]
    public void can_create_empty_registry()
    {
        var registry = new SqliteFunctionRegistry();
        registry.Functions.ShouldBeEmpty();
    }

    [Fact]
    public void can_add_scalar_function_with_no_parameters()
    {
        var registry = new SqliteFunctionRegistry();

        registry.AddScalar("get_answer", () => 42);

        registry.Functions.Count.ShouldBe(1);
        registry.Functions[0].Name.ShouldBe("get_answer");
    }

    [Fact]
    public void can_add_scalar_function_with_parameters()
    {
        var registry = new SqliteFunctionRegistry();

        registry.AddScalar<int>("double_value", x => Convert.ToInt32(x) * 2);

        registry.Functions.Count.ShouldBe(1);
        registry.Functions[0].Name.ShouldBe("double_value");
    }

    [Fact]
    public void can_add_aggregate_function()
    {
        var registry = new SqliteFunctionRegistry();

        registry.AddAggregate<int, int>(
            "my_sum",
            0,
            (acc, val) => acc + Convert.ToInt32(val)
        );

        registry.Functions.Count.ShouldBe(1);
        registry.Functions[0].Name.ShouldBe("my_sum");
    }

    [Fact]
    public void can_add_multiple_functions()
    {
        var registry = new SqliteFunctionRegistry();

        registry.AddScalar("func1", () => 1);
        registry.AddScalar<int>("func2", x => 2);
        registry.AddAggregate<int, int>("func3", 0, (acc, val) => acc);

        registry.Functions.Count.ShouldBe(3);
        registry.Functions[0].Name.ShouldBe("func1");
        registry.Functions[1].Name.ShouldBe("func2");
        registry.Functions[2].Name.ShouldBe("func3");
    }

    [Fact]
    public void can_add_pre_created_function()
    {
        var registry = new SqliteFunctionRegistry();
        var function = new ScalarFunction<int>("test", () => 42);

        registry.Add(function);

        registry.Functions.Count.ShouldBe(1);
        registry.Functions[0].ShouldBe(function);
    }

    [Fact]
    public void add_validates_function_not_null()
    {
        var registry = new SqliteFunctionRegistry();

        Should.Throw<ArgumentNullException>(() => registry.Add(null!));
    }

    [Fact]
    public void can_clear_registry()
    {
        var registry = new SqliteFunctionRegistry();
        registry.AddScalar("func1", () => 1);
        registry.AddScalar("func2", () => 2);

        registry.Clear();

        registry.Functions.ShouldBeEmpty();
    }

    [Fact]
    public async Task register_all_throws_when_connection_is_null()
    {
        var registry = new SqliteFunctionRegistry();

        Should.Throw<ArgumentNullException>(() => registry.RegisterAll(null!));
    }

    [Fact]
    public async Task register_all_throws_when_connection_is_closed()
    {
        var registry = new SqliteFunctionRegistry();
        await using var connection = new SqliteConnection("Data Source=:memory:");

        Should.Throw<InvalidOperationException>(() => registry.RegisterAll(connection));
    }

    [Fact]
    public async Task register_all_succeeds_with_no_functions()
    {
        var registry = new SqliteFunctionRegistry();

        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        // Should not throw
        registry.RegisterAll(connection);
    }

    [Fact]
    public async Task register_all_registers_all_functions()
    {
        var registry = new SqliteFunctionRegistry();
        registry.AddScalar("get_one", () => 1);
        registry.AddScalar("get_two", () => 2);
        registry.AddScalar<int>("double_value", x => Convert.ToInt32(x) * 2);

        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        registry.RegisterAll(connection);

        // Test first function
        await using var cmd1 = connection.CreateCommand();
        cmd1.CommandText = "SELECT get_one()";
        var result1 = await cmd1.ExecuteScalarAsync();
        result1.ShouldBe(1L);

        // Test second function
        await using var cmd2 = connection.CreateCommand();
        cmd2.CommandText = "SELECT get_two()";
        var result2 = await cmd2.ExecuteScalarAsync();
        result2.ShouldBe(2L);

        // Test third function
        await using var cmd3 = connection.CreateCommand();
        cmd3.CommandText = "SELECT double_value(21)";
        var result3 = await cmd3.ExecuteScalarAsync();
        result3.ShouldBe(42L);
    }

    [Fact]
    public async Task register_all_async_works()
    {
        var registry = new SqliteFunctionRegistry();
        registry.AddScalar("get_answer", () => 42);

        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await registry.RegisterAllAsync(connection);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT get_answer()";
        var result = await cmd.ExecuteScalarAsync();
        result.ShouldBe(42L);
    }

    [Fact]
    public async Task helper_methods_return_function()
    {
        var registry = new SqliteFunctionRegistry();

        var func1 = registry.AddScalar("func1", () => 1);
        var func2 = registry.AddScalar<int>("func2", x => 2);
        var func3 = registry.AddAggregate<int, int>("func3", 0, (acc, val) => acc);

        func1.ShouldNotBeNull();
        func2.ShouldNotBeNull();
        func3.ShouldNotBeNull();

        func1.Name.ShouldBe("func1");
        func2.Name.ShouldBe("func2");
        func3.Name.ShouldBe("func3");
    }

    private static int compareAsNumbers(string x, string y) => decimal.Parse(x).CompareTo(decimal.Parse(y));

    [Fact]
    public void can_add_collation()
    {
        var registry = new SqliteFunctionRegistry();

        var collation = registry.AddCollation("numeric_text", compareAsNumbers);

        collation.Name.ShouldBe("numeric_text");
        registry.Collations.ShouldHaveSingleItem().ShouldBeSameAs(collation);
        registry.Functions.ShouldBeEmpty();
    }

    [Fact]
    public void add_collation_validates_its_arguments()
    {
        var registry = new SqliteFunctionRegistry();

        Should.Throw<ArgumentException>(() => registry.AddCollation(" ", compareAsNumbers));
        Should.Throw<ArgumentNullException>(() => registry.AddCollation("numeric_text", null!));
        registry.Collations.ShouldBeEmpty();
    }

    [Fact]
    public void clear_removes_collations_too()
    {
        var registry = new SqliteFunctionRegistry();
        registry.AddCollation("numeric_text", compareAsNumbers);

        registry.Clear();

        registry.Collations.ShouldBeEmpty();
    }

    [Fact]
    public async Task order_by_collate_uses_a_registered_collation()
    {
        var registry = new SqliteFunctionRegistry();
        registry.AddCollation("numeric_text", compareAsNumbers);

        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        registry.RegisterAll(connection);

        await using (var create = connection.CreateCommand())
        {
            create.CommandText =
                "create table amounts (value text not null); " +
                "insert into amounts (value) values ('10'), ('9.5'), ('100'), ('-2');";
            await create.ExecuteNonQueryAsync();
        }

        // Plain TEXT ordering compares byte by byte
        (await readValuesAsync(connection, "select value from amounts order by value"))
            .ShouldBe(["-2", "10", "100", "9.5"]);

        (await readValuesAsync(connection, "select value from amounts order by value collate numeric_text"))
            .ShouldBe(["-2", "9.5", "10", "100"]);
    }

    [Fact]
    public async Task a_comparison_with_collate_uses_a_registered_collation()
    {
        var registry = new SqliteFunctionRegistry();
        registry.AddCollation("numeric_text", compareAsNumbers);

        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await registry.RegisterAllAsync(connection);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "select '10' > '9', '10' > '9' collate numeric_text, '1.50' = '1.5' collate numeric_text";
        await using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        reader.GetInt64(0).ShouldBe(0L);
        reader.GetInt64(1).ShouldBe(1L);
        reader.GetInt64(2).ShouldBe(1L);
    }

    private static async Task<List<string>> readValuesAsync(SqliteConnection connection, string sql)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync();
        var values = new List<string>();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }
}
