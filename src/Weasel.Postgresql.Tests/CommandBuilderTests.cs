using System.Data.Common;
using Npgsql;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

public class CommandBuilderTests
{
    [Fact]
    public void append_parameters_with_one_at_the_end()
    {
        var builder = new CommandBuilder(new NpgsqlCommand());

        builder.Append("select data from table where ");
        builder.AppendWithParameters("foo = ?")
            .Length.ShouldBe(1);

        builder.ToString().ShouldBe("select data from table where foo = :p0");
    }

    [Fact]
    public void append_with_db_parameters_returns_neutral_db_parameters()
    {
        ICommandBuilder builder = new CommandBuilder(new NpgsqlCommand());

        builder.Append("select data from table where ");
        DbParameter[] parameters = builder.AppendWithDbParameters("foo = ? and bar = ?");

        parameters.Length.ShouldBe(2);
        parameters.ShouldAllBe(x => x is NpgsqlParameter);

        builder.ToString().ShouldBe("select data from table where foo = :p0 and bar = :p1");
    }

    [Fact]
    public void append_with_db_parameters_honors_custom_placeholder()
    {
        ICommandBuilder builder = new CommandBuilder(new NpgsqlCommand());

        builder.Append("select data from table where ");
        DbParameter[] parameters = builder.AppendWithDbParameters("foo = ^ and bar = ^", '^');

        parameters.Length.ShouldBe(2);
        builder.ToString().ShouldBe("select data from table where foo = :p0 and bar = :p1");
    }

    [Fact]
    public void batch_builder_append_with_db_parameters_returns_neutral_db_parameters()
    {
        ICommandBuilder builder = new BatchBuilder();

        builder.Append("select data from table where ");
        DbParameter[] parameters = builder.AppendWithDbParameters("foo = ? and bar = ?");

        parameters.Length.ShouldBe(2);
        parameters.ShouldAllBe(x => x is NpgsqlParameter);
    }

    [Fact]
    public void preview_last_appended_parameter()
    {
        var builder = new CommandBuilder(new NpgsqlCommand());
        builder.LastParameterName.ShouldBeNull();

        builder.Append("select data from table where ");
        builder.AppendWithParameters("foo = ?")
            .Length.ShouldBe(1);

        builder.LastParameterName.ShouldBe("p0");
    }

    [Fact]
    public void starting_with_existing_command()
    {
        var command = new NpgsqlCommand("select data from table");
        var builder = new CommandBuilder(command);

        builder.Append(" where ");
        builder.AppendWithParameters("foo = ?")
            .Length.ShouldBe(1);

        builder.ToString().ShouldBe("select data from table where foo = :p0");
    }

    [Fact]
    public void append_parameters_with_multiples_at_end()
    {
        var builder = new CommandBuilder(new NpgsqlCommand());

        builder.Append("select data from table where ");
        builder.AppendWithParameters("foo = ? and bar = ?")
            .Length.ShouldBe(2);

        builder.ToString().ShouldBe("select data from table where foo = :p0 and bar = :p1");
    }


    [Fact]
    public void append_parameters_with_multiples_in_the_middle()
    {
        var builder = new CommandBuilder(new NpgsqlCommand());

        builder.Append("select data from table where ");
        builder.AppendWithParameters("foo = ? and bar = ? order by baz")
            .Length.ShouldBe(2);

        builder.ToString().ShouldBe("select data from table where foo = :p0 and bar = :p1 order by baz");
    }
}
