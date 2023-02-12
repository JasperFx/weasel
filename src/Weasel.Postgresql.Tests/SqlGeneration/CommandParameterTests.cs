using System.Linq.Expressions;
using Npgsql;
using NpgsqlTypes;
using Shouldly;
using Weasel.Postgresql.SqlGeneration;
using Xunit;

namespace Weasel.Postgresql.Tests.SqlGeneration;

public class CommandParameterTests
{
    [Fact]
    public void build_from_constant_expression()
    {
        var parameter = new CommandParameter(Expression.Constant("hello"));

        parameter.Value.ShouldBe("hello");
        parameter.DbType.ShouldBe(NpgsqlDbType.Text);
    }

    [Fact]
    public void build_from_value()
    {
        var parameter = new CommandParameter(44);
        parameter.Value.ShouldBe(44);
        parameter.DbType.ShouldBe(NpgsqlDbType.Integer);
    }


    [Fact]
    public void append_parameter()
    {
        var command = new NpgsqlCommand();
        var builder = new CommandBuilder(command);

        var parameter = new CommandParameter(44);
        parameter.AddParameter(builder);

        command.Parameters[0].Value.ShouldBe(parameter.Value);
        command.Parameters[0].NpgsqlDbType.ShouldBe(parameter.DbType);
    }

    [Fact]
    public void apply()
    {
        var command = new NpgsqlCommand();
        var builder = new CommandBuilder(command);

        var parameter = new CommandParameter(44);
        parameter.Apply(builder);

        var dbParameter = command.Parameters[0];
        dbParameter.Value.ShouldBe(parameter.Value);
        dbParameter.NpgsqlDbType.ShouldBe(parameter.DbType);

        builder.ToString().ShouldEndWith(":" + dbParameter.ParameterName);
    }
}
