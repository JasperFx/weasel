using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Sqlite;
using Xunit;

namespace Weasel.Sqlite.Tests;

public class CommandBuilderTests
{
    [Fact]
    public void append_simple_text()
    {
        var builder = new CommandBuilder();
        builder.Append("SELECT * FROM users");

        builder.ToString().ShouldBe("SELECT * FROM users");
    }

    [Fact]
    public void append_parameters_with_one_at_the_end()
    {
        var builder = new CommandBuilder();

        builder.Append("SELECT data FROM table WHERE ");
        builder.AppendWithParameters("foo = ?")
            .Length.ShouldBe(1);

        builder.ToString().ShouldBe("SELECT data FROM table WHERE foo = @p0");
    }

    [Fact]
    public void preview_last_appended_parameter()
    {
        var builder = new CommandBuilder();
        builder.LastParameterName.ShouldBeNull();

        builder.Append("SELECT data FROM table WHERE ");
        builder.AppendWithParameters("foo = ?")
            .Length.ShouldBe(1);

        builder.LastParameterName.ShouldBe("p0");
    }

    [Fact]
    public void append_parameters_with_multiples_at_end()
    {
        var builder = new CommandBuilder();

        builder.Append("SELECT data FROM table WHERE ");
        builder.AppendWithParameters("foo = ? AND bar = ?")
            .Length.ShouldBe(2);

        builder.ToString().ShouldBe("SELECT data FROM table WHERE foo = @p0 AND bar = @p1");
    }

    [Fact]
    public void append_parameters_with_multiples_in_the_middle()
    {
        var builder = new CommandBuilder();

        builder.Append("SELECT data FROM table WHERE ");
        builder.AppendWithParameters("foo = ? AND bar = ? ORDER BY baz")
            .Length.ShouldBe(2);

        builder.ToString().ShouldBe("SELECT data FROM table WHERE foo = @p0 AND bar = @p1 ORDER BY baz");
    }

    [Fact]
    public void add_parameter_returns_parameter()
    {
        var builder = new CommandBuilder();
        var param = builder.AddParameter("test_value");

        param.ShouldNotBeNull();
        param.ParameterName.ShouldBe("p0");
        param.Value.ShouldBe("test_value");
    }

    [Fact]
    public void add_named_parameter()
    {
        var builder = new CommandBuilder();
        var param = builder.AddNamedParameter("userId", 123);

        param.ShouldNotBeNull();
        param.ParameterName.ShouldBe("userId");
        param.Value.ShouldBe(123);
    }

    [Fact]
    public void build_command_with_connection()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        var command = connection.CreateCommand();
        var builder = new CommandBuilder(command);
        builder.Append("SELECT * FROM users WHERE id = ");
        builder.AppendParameter(42);

        var compiled = builder.Compile();

        compiled.ShouldNotBeNull();
        compiled.CommandText.ShouldContain("SELECT * FROM users WHERE id = @p0");
        compiled.Parameters.Count.ShouldBe(1);
        compiled.Parameters[0].Value.ShouldBe(42);
    }

    [Fact]
    public void append_multiple_times()
    {
        var builder = new CommandBuilder();
        builder.Append("SELECT ");
        builder.Append("* ");
        builder.Append("FROM ");
        builder.Append("users");

        builder.ToString().ShouldBe("SELECT * FROM users");
    }
}
