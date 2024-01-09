using System.Collections.ObjectModel;
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
        var builder = new BatchBuilder();

        var parameter = new CommandParameter(44);
        parameter.AddParameter(builder);

        var command = builder.Compile().BatchCommands[0];

        command.Parameters[0].Value.ShouldBe(parameter.Value);
        command.Parameters[0].NpgsqlDbType.ShouldBe(parameter.DbType!.Value);
    }

    [Fact]
    public void apply()
    {
        var builder = new BatchBuilder();

        var parameter = new CommandParameter(44);
        parameter.Apply(builder);

        var command = builder.Compile().BatchCommands[0];

        var dbParameter = command.Parameters[0];
        dbParameter.Value.ShouldBe(parameter.Value);
        dbParameter.NpgsqlDbType.ShouldBe(parameter.DbType!.Value);

        command.CommandText.ShouldEndWith("$1");
    }

    [Theory]
    [MemberData(nameof(Enumerables))]
    public void handles_IEnumerable_as_array(IEnumerable<string> enumerable)
    {
        var parameter = new CommandParameter(enumerable);

        parameter.DbType.ShouldBe(NpgsqlDbType.Array | NpgsqlDbType.Text);
    }

    public static TheoryData<IEnumerable<string>> Enumerables =>
        new()
        {
            new[] { Guid.NewGuid().ToString() },
            new List<string> { Guid.NewGuid().ToString() },
            new HashSet<string> { Guid.NewGuid().ToString() },
            new Collection<string> { Guid.NewGuid().ToString() }
        };

    [Fact]
    public void falls_back_to_npgsql_mapping_for_unknown_type()
    {
        var unknown = new UnknownType();
        var parameter = new CommandParameter(unknown);

        parameter.DbType.ShouldBe(null);
    }
}

class UnknownType
{
}
