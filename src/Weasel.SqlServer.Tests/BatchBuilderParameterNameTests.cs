using System.Data;
using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     Pure unit tests — no database. The parameter names the SQL Server BatchBuilder stamps
///     onto its parameters now come from the precomputed table in Weasel.Core (weasel#556);
///     these pin down that the names, and the SQL text that embeds them, are exactly what the
///     old per-parameter concatenation produced.
/// </summary>
public class BatchBuilderParameterNameTests
{
    [Fact]
    public void every_append_parameter_overload_names_by_position()
    {
        var builder = new BatchBuilder();

        builder.Append("select ");
        builder.AppendParameter("one");                          // AppendParameter<T>(T)
        builder.Append(", ");
        builder.AppendParameter("two", SqlDbType.NVarChar);      // AppendParameter<T>(T, SqlDbType)
        builder.Append(", ");
        builder.AppendParameter((object)3);                      // AppendParameter(object)
        builder.Append(", ");
        builder.AppendParameter((object)4, SqlDbType.Int);       // AppendParameter(object?, SqlDbType?)

        var batch = builder.Compile();
        var command = batch.BatchCommands[0];

        command.CommandText.ShouldBe("select @p0, @p1, @p2, @p3");

        command.Parameters.Count.ShouldBe(4);
        for (var i = 0; i < command.Parameters.Count; i++)
        {
            command.Parameters[i].ParameterName.ShouldBe("p" + i);
        }
    }

    [Fact]
    public void numbering_restarts_with_each_new_command()
    {
        var builder = new BatchBuilder();

        builder.Append("select ");
        builder.AppendParameter(1);
        builder.StartNewCommand();
        builder.Append("select ");
        builder.AppendParameter(2);
        builder.Append(", ");
        builder.AppendParameter(3);

        var batch = builder.Compile();

        batch.BatchCommands[0].Parameters[0].ParameterName.ShouldBe("p0");
        batch.BatchCommands[1].Parameters[0].ParameterName.ShouldBe("p0");
        batch.BatchCommands[1].Parameters[1].ParameterName.ShouldBe("p1");
        batch.BatchCommands[1].CommandText.ShouldBe("select @p0, @p1");
    }

    [Fact]
    public void names_stay_correct_past_the_precomputed_table()
    {
        var builder = new BatchBuilder();

        builder.Append("select ");
        builder.AppendParameter(0);
        for (var i = 1; i < 600; i++)
        {
            builder.Append(", ");
            builder.AppendParameter(i);
        }

        var batch = builder.Compile();
        var command = batch.BatchCommands[0];

        command.Parameters.Count.ShouldBe(600);
        command.Parameters[599].ParameterName.ShouldBe("p599");
        command.CommandText.ShouldEndWith("@p598, @p599");
    }
}
