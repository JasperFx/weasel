using System.Data.Common;
using Microsoft.Data.SqlClient;
using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests;

public class CommandBuilderTests
{
    [Fact]
    public void append_parameters_with_one_at_the_end()
    {
        var builder = new CommandBuilder(new SqlCommand());

        builder.Append("select data from table where ");
        builder.AppendWithParameters("foo = ?")
            .Length.ShouldBe(1);

        builder.ToString().ShouldBe("select data from table where foo = @p0");
    }

    [Fact]
    public void append_with_db_parameters_returns_neutral_db_parameters()
    {
        ICommandBuilder builder = new BatchBuilder();

        builder.Append("select data from table where ");
        DbParameter[] parameters = builder.AppendWithDbParameters("foo = ? and bar = ?");

        parameters.Length.ShouldBe(2);
        parameters.ShouldAllBe(x => x is SqlParameter);
    }

    [Fact]
    public void append_with_db_parameters_honors_custom_placeholder()
    {
        ICommandBuilder builder = new BatchBuilder();

        builder.Append("select data from table where ");
        DbParameter[] parameters = builder.AppendWithDbParameters("foo = ^ and bar = ^", '^');

        parameters.Length.ShouldBe(2);
    }

    [Fact]
    public void append_parameters_with_multiples_at_end()
    {
        var builder = new CommandBuilder(new SqlCommand());

        builder.Append("select data from table where ");
        builder.AppendWithParameters("foo = ? and bar = ?")
            .Length.ShouldBe(2);

        builder.ToString().ShouldBe("select data from table where foo = @p0 and bar = @p1");
    }


    [Fact]
    public void append_parameters_with_multiples_in_the_middle()
    {
        var builder = new CommandBuilder(new SqlCommand());

        builder.Append("select data from table where ");
        builder.AppendWithParameters("foo = ? and bar = ? order by baz")
            .Length.ShouldBe(2);

        builder.ToString().ShouldBe("select data from table where foo = @p0 and bar = @p1 order by baz");
    }




    [Fact]
    public void append_parameters_with_one_at_the_end_with_caret()
    {
        var builder = new CommandBuilder(new SqlCommand());

        builder.Append("select data from table where ");
        builder.AppendWithParameters("foo = ^", '^')
            .Length.ShouldBe(1);

        builder.ToString().ShouldBe("select data from table where foo = @p0");
    }

    [Fact]
    public void append_parameters_with_multiples_at_end_with_caret()
    {
        var builder = new CommandBuilder(new SqlCommand());

        builder.Append("select data from table where ");
        builder.AppendWithParameters("foo = ^ and bar = ^", '^')
            .Length.ShouldBe(2);

        builder.ToString().ShouldBe("select data from table where foo = @p0 and bar = @p1");
    }


    [Fact]
    public void append_parameters_with_multiples_in_the_middle_with_caret()
    {
        var builder = new CommandBuilder(new SqlCommand());

        builder.Append("select data from table where ");
        builder.AppendWithParameters("foo = ^ and bar = ^ order by baz", '^')
            .Length.ShouldBe(2);

        builder.ToString().ShouldBe("select data from table where foo = @p0 and bar = @p1 order by baz");
    }
}
