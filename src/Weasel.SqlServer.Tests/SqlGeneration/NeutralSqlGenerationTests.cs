using System.Data.Common;
using System.Linq;
using Shouldly;
using Weasel.SqlServer.SqlGeneration;
using Xunit;
using CoreSql = Weasel.Core.SqlGeneration;

namespace Weasel.SqlServer.Tests.SqlGeneration;

/// <summary>
///     Proves that the SQL Server SQL-generation types satisfy the shared, dialect-neutral
///     Weasel.Core substrate (weasel#327) so a database-agnostic consumer can compose and apply
///     fragments without referencing Weasel.SqlServer.
/// </summary>
public class NeutralSqlGenerationTests
{
    [Fact]
    public void dialect_command_builders_are_core_command_builders()
    {
        // The concrete CommandBuilder now also satisfies the neutral surface, matching BatchBuilder
        new CommandBuilder().ShouldBeAssignableTo<Weasel.Core.ICommandBuilder>();
        new BatchBuilder().ShouldBeAssignableTo<Weasel.Core.ICommandBuilder>();
    }

    [Fact]
    public void dialect_command_builder_interface_derives_from_core()
    {
        typeof(Weasel.Core.ICommandBuilder)
            .IsAssignableFrom(typeof(Weasel.SqlServer.ICommandBuilder)).ShouldBeTrue();
    }

    [Fact]
    public void dialect_fragment_is_a_core_fragment()
    {
        new WhereFragment("id = 1").ShouldBeAssignableTo<CoreSql.ISqlFragment>();
    }

    [Fact]
    public void apply_fragment_through_the_neutral_surface()
    {
        // Hold everything as the dialect-neutral Weasel.Core abstractions. SQL Server fragments are
        // applied against the concrete CommandBuilder, so the neutral consumer supplies one.
        CoreSql.ISqlFragment fragment = new WhereFragment("id = 1");
        var builder = new CommandBuilder();
        Weasel.Core.ICommandBuilder neutral = builder;

        // Neutral Apply is satisfied by the default interface method that forwards
        // to the SQL Server Apply(CommandBuilder) overload
        fragment.Apply(neutral);

        builder.Compile().CommandText.Trim().ShouldBe("id = 1");
    }

    [Fact]
    public void compound_fragment_is_walkable_as_a_neutral_compound()
    {
        var compound = CompoundWhereFragment.And(
            new WhereFragment("a = 1"),
            new WhereFragment("b = 2"));

        CoreSql.ICompoundFragment neutral = compound;
        neutral.Children.Count().ShouldBe(2);
    }

    [Fact]
    public void neutral_consumer_can_fill_parameter_slots_via_db_parameters()
    {
        var builder = new CommandBuilder();
        Weasel.Core.ICommandBuilder neutral = builder;

        DbParameter[] parameters = neutral.AppendWithDbParameters("id = ?");
        parameters.Length.ShouldBe(1);
        parameters[0].Value = 7;

        builder.Compile().CommandText.Trim().ShouldBe("id = @p0");
    }
}
