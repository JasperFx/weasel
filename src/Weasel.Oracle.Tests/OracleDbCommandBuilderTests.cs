using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Shouldly;
using Xunit;

namespace Weasel.Oracle.Tests;

public class OracleDbCommandBuilderTests
{
    [Fact]
    public void uses_the_oracle_bind_marker_rather_than_the_generic_one()
    {
        var builder = new OracleDbCommandBuilder();

        builder.Append("delete from messages where keep_until <= ");
        builder.AppendParameter(DateTimeOffset.UtcNow);

        builder.ToString().ShouldBe("delete from messages where keep_until <= :p0");
    }

    [Fact]
    public void binds_by_name()
    {
        var command = new OracleCommand();
        _ = new OracleDbCommandBuilder(command);

        command.BindByName.ShouldBeTrue();
    }

    [Fact]
    public void a_single_statement_compiles_to_a_single_command()
    {
        var builder = new OracleDbCommandBuilder();

        builder.Append("delete from incoming where id = ");
        builder.AppendParameter(Guid.NewGuid());

        var commands = builder.CompileCommands();

        commands.Count.ShouldBe(1);
        commands[0].CommandText.ShouldBe("delete from incoming where id = :p0");
        commands[0].Parameters.Count.ShouldBe(1);
    }

    [Fact]
    public void no_statements_at_all_compiles_to_nothing()
    {
        new OracleDbCommandBuilder().CompileCommands().Count.ShouldBe(0);
    }

    [Fact]
    public void splits_at_every_start_new_command_boundary()
    {
        var builder = new OracleDbCommandBuilder();

        builder.Append("select destination from outgoing");
        builder.StartNewCommand();
        builder.Append("delete from incoming");
        builder.StartNewCommand();
        builder.Append("update nodes set active = 1");

        var commands = builder.CompileCommands();

        commands.Select(x => x.CommandText).ShouldBe([
            "select destination from outgoing",
            "delete from incoming",
            "update nodes set active = 1"
        ]);
    }

    [Fact]
    public void strips_the_trailing_semicolon_callers_write_for_other_providers()
    {
        var builder = new OracleDbCommandBuilder();

        builder.Append("delete from incoming;");
        builder.StartNewCommand();
        builder.Append("delete from outgoing;");

        builder.CompileCommands().Select(x => x.CommandText).ShouldBe([
            "delete from incoming",
            "delete from outgoing"
        ]);
    }

    [Fact]
    public void a_semicolon_only_statement_is_not_a_statement()
    {
        var builder = new OracleDbCommandBuilder();

        builder.Append(";");
        builder.StartNewCommand();
        builder.Append("delete from incoming;");

        builder.CompileCommands().Count.ShouldBe(1);
    }

    [Fact]
    public void exposes_the_oracle_bind_marker()
    {
        new OracleDbCommandBuilder().ParameterPrefix.ShouldBe(':');
    }

    [Fact]
    public void empty_statements_do_not_produce_commands()
    {
        var builder = new OracleDbCommandBuilder();

        builder.StartNewCommand();
        builder.Append("delete from incoming");
        builder.StartNewCommand();
        builder.StartNewCommand();

        var commands = builder.CompileCommands();

        commands.Count.ShouldBe(1);
        commands[0].CommandText.ShouldBe("delete from incoming");
    }

    [Fact]
    public void each_split_command_only_carries_the_parameters_its_own_statement_bound()
    {
        var first = Guid.NewGuid();
        var cutoff = DateTimeOffset.UtcNow;

        var builder = new OracleDbCommandBuilder();

        builder.Append("delete from incoming where id = ");
        builder.AppendParameter(first);

        builder.StartNewCommand();

        builder.Append("delete from dead_letters where expires <= ");
        builder.AppendParameter(cutoff);
        builder.Append(" and node = ");
        builder.AppendParameter(5);

        var commands = builder.CompileCommands();

        commands.Count.ShouldBe(2);

        commands[0].CommandText.ShouldBe("delete from incoming where id = :p0");
        commands[0].Parameters.Count.ShouldBe(1);
        commands[0].Parameters[0].ParameterName.ShouldBe("p0");
        commands[0].Parameters[0].Value.ShouldBe(first.ToByteArray());

        commands[1].CommandText.ShouldBe("delete from dead_letters where expires <= :p1 and node = :p2");
        commands[1].Parameters.Count.ShouldBe(2);
        commands[1].Parameters[0].ParameterName.ShouldBe("p1");
        commands[1].Parameters[0].Value.ShouldBe(cutoff);
        commands[1].Parameters[1].ParameterName.ShouldBe("p2");
        commands[1].Parameters[1].Value.ShouldBe(5);
    }

    [Fact]
    public void split_commands_bind_by_name()
    {
        var builder = new OracleDbCommandBuilder();

        builder.Append("delete from incoming");
        builder.StartNewCommand();
        builder.Append("delete from outgoing");

        builder.CompileCommands().OfType<OracleCommand>()
            .ShouldAllBe(x => x.BindByName);
    }

    [Fact]
    public void command_count_reports_the_open_statement_too()
    {
        var builder = new OracleDbCommandBuilder();
        builder.CommandCount.ShouldBe(0);

        builder.Append("delete from incoming");
        builder.CommandCount.ShouldBe(1);

        builder.StartNewCommand();
        builder.CommandCount.ShouldBe(1);

        builder.Append("delete from outgoing");
        builder.CommandCount.ShouldBe(2);
    }

    [Fact]
    public void guids_are_bound_as_raw()
    {
        var id = Guid.NewGuid();
        var builder = new OracleDbCommandBuilder();

        builder.Append("select 1 from dual where id = ");
        builder.AppendParameter(id);

        var parameter = (OracleParameter)builder.CompileCommands()[0].Parameters[0];

        parameter.OracleDbType.ShouldBe(OracleDbType.Raw);
        parameter.Value.ShouldBe(id.ToByteArray());
    }

    [Fact]
    public void booleans_are_bound_as_oracle_numbers()
    {
        var builder = new OracleDbCommandBuilder();

        builder.Append("update dead_letters set replayable = ");
        builder.AddNamedParameter("replayable", true);

        var parameter = (OracleParameter)builder.CompileCommands()[0].Parameters["replayable"];

        parameter.OracleDbType.ShouldBe(OracleDbType.Int16);
        parameter.Value.ShouldBe(1);
    }

    [Fact]
    public void date_time_offsets_keep_their_oracle_type()
    {
        var builder = new OracleDbCommandBuilder();

        builder.Append("delete from incoming where expires <= ");
        builder.AppendParameter(DateTimeOffset.UtcNow);

        var parameter = (OracleParameter)builder.CompileCommands()[0].Parameters[0];

        parameter.OracleDbType.ShouldBe(OracleDbType.TimeStampTZ);
    }

    [Fact]
    public void null_values_are_bound_as_db_null()
    {
        var builder = new OracleDbCommandBuilder();

        builder.Append("select 1 from dual where description = ");
        builder.AppendParameter((object?)null);

        builder.CompileCommands()[0].Parameters[0].Value.ShouldBe(DBNull.Value);
    }

    [Fact]
    public void append_with_db_parameters_uses_the_oracle_marker()
    {
        var builder = new OracleDbCommandBuilder();

        builder.Append("select data from messages where ");
        DbParameter[] parameters = builder.AppendWithDbParameters("foo = ? and bar = ?");

        parameters.Length.ShouldBe(2);
        builder.ToString().ShouldBe("select data from messages where foo = :p0 and bar = :p1");
    }
}
