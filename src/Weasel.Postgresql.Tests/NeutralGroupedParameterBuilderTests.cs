using Npgsql;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

// DB-free unit tests for the dialect-neutral parameter seam added for the closed-shape
// storage runtime (marten#4821 event side, E1). Exercises Weasel.Core.ICommandBuilder /
// Weasel.Core.IGroupedParameterBuilder against the PostgreSQL command builder.
public class NeutralGroupedParameterBuilderTests
{
    [Fact]
    public void neutral_command_builder_appends_a_grouped_run_of_parameters()
    {
        var builder = new CommandBuilder();
        Weasel.Core.ICommandBuilder neutral = builder;

        neutral.Append("select ");
        var group = neutral.CreateGroupedParameterBuilder(',');
        group.AppendParameter(1);
        group.AppendParameter("blue");
        var nullParam = group.AppendParameter(null);

        var command = builder.Compile();
        command.CommandText.ShouldBe("select :p0,:p1,:p2");
        command.Parameters.Count.ShouldBe(3);
        nullParam.ShouldBeOfType<NpgsqlParameter>();
        nullParam.Value.ShouldBe(DBNull.Value);
    }

    [Fact]
    public void neutral_append_parameter_returns_the_dialect_neutral_parameter()
    {
        var builder = new CommandBuilder();
        Weasel.Core.ICommandBuilder neutral = builder;

        neutral.Append("select ");
        var parameter = neutral.AppendParameter(42);

        parameter.ShouldBeOfType<NpgsqlParameter>();
        parameter.Value.ShouldBe(42);
        builder.Compile().CommandText.ShouldBe("select :p0");
    }
}
