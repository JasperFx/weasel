using Shouldly;
using Xunit;

namespace Weasel.MySql.Tests;

/// <summary>
/// weasel#423: Weasel.MySql.CommandBuilder shipped without the non-generic
/// Weasel.Core.ICommandBuilder, which is the surface every Weasel.Storage closed-shape operation
/// configures itself against. SQLite was the provider where this actually blocked a consumer, but
/// MySql had the identical gap and would have hit it the moment a Weasel.Storage consumer targeted
/// it. These guard the members the base class cannot supply on its own.
/// </summary>
public class CommandBuilderNeutralContractTests
{
    [Fact]
    public void implements_the_dialect_neutral_command_builder()
    {
        typeof(Weasel.Core.ICommandBuilder).IsAssignableFrom(typeof(CommandBuilder)).ShouldBeTrue();
    }

    [Fact]
    public void append_parameter_returns_the_created_parameter()
    {
        Weasel.Core.ICommandBuilder builder = new CommandBuilder();

        builder.Append("select * from foo where bar = ");
        var parameter = builder.AppendParameter("baz");

        parameter.ShouldNotBeNull();
        parameter.Value.ShouldBe("baz");
        builder.ToString().ShouldBe("select * from foo where bar = @p0");
    }

    [Fact]
    public void append_parameters_writes_each_value_comma_separated()
    {
        Weasel.Core.ICommandBuilder builder = new CommandBuilder();

        builder.Append("select * from foo where bar in (");
        builder.AppendParameters("one", "two", "three");
        builder.Append(")");

        builder.ToString().ShouldBe("select * from foo where bar in (@p0, @p1, @p2)");
    }

    [Fact]
    public void append_parameters_rejects_an_empty_set()
    {
        Weasel.Core.ICommandBuilder builder = new CommandBuilder();

        Should.Throw<ArgumentOutOfRangeException>(() => builder.AppendParameters());
    }

    [Fact]
    public void creates_a_grouped_parameter_builder()
    {
        Weasel.Core.ICommandBuilder builder = new CommandBuilder();

        builder.CreateGroupedParameterBuilder().ShouldNotBeNull();
    }

    [Fact]
    public void carries_a_tenant_id()
    {
        Weasel.Core.ICommandBuilder builder = new CommandBuilder { TenantId = "tenant-a" };

        builder.TenantId.ShouldBe("tenant-a");
    }
}
