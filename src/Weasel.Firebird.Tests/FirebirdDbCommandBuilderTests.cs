using System.Data.Common;
using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Types;
using Shouldly;
using Xunit;

namespace Weasel.Firebird.Tests;

public class FirebirdDbCommandBuilderTests
{
    [Fact]
    public void uses_the_at_sign_bind_marker()
    {
        var builder = new FirebirdDbCommandBuilder();

        builder.Append("delete from messages where id = ");
        builder.AppendParameter(1);

        builder.ToString().ShouldBe("delete from messages where id = @p0");
        builder.ParameterPrefix.ShouldBe('@');
    }

    [Fact]
    public void a_single_statement_compiles_to_the_command_it_was_built_on()
    {
        var command = new FbCommand();
        var builder = new FirebirdDbCommandBuilder(command);

        builder.Append("delete from incoming where id = ");
        builder.AppendParameter(Guid.NewGuid());

        var commands = builder.CompileCommands();

        commands.Count.ShouldBe(1);
        commands[0].ShouldBeSameAs(command);
        commands[0].CommandText.ShouldBe("delete from incoming where id = @p0");
        commands[0].Parameters.Count.ShouldBe(1);
    }

    [Fact]
    public void no_statements_at_all_compiles_to_nothing()
    {
        new FirebirdDbCommandBuilder().CompileCommands().Count.ShouldBe(0);
    }

    /// <summary>
    ///     Firebird refuses two statements in one command, so this is what lets a schema object
    ///     register more than one introspection query.
    /// </summary>
    [Fact]
    public void splits_at_every_start_new_command_boundary()
    {
        var builder = new FirebirdDbCommandBuilder();

        builder.Append("select 1 from rdb$database");
        builder.StartNewCommand();
        builder.Append("select 2 from rdb$database");
        builder.StartNewCommand();
        builder.Append("select 3 from rdb$database");

        builder.CompileCommands().Select(x => x.CommandText).ShouldBe([
            "select 1 from rdb$database",
            "select 2 from rdb$database",
            "select 3 from rdb$database"
        ]);
    }

    [Fact]
    public void strips_the_trailing_semicolon_callers_write_for_other_providers()
    {
        var builder = new FirebirdDbCommandBuilder();

        builder.Append("delete from incoming;");
        builder.StartNewCommand();
        builder.Append("delete from outgoing;");

        builder.CompileCommands().Select(x => x.CommandText).ShouldBe([
            "delete from incoming",
            "delete from outgoing"
        ]);
    }

    [Fact]
    public void empty_statements_do_not_produce_commands()
    {
        var builder = new FirebirdDbCommandBuilder();

        builder.StartNewCommand();
        builder.Append(";");
        builder.StartNewCommand();
        builder.Append("delete from incoming");
        builder.StartNewCommand();

        var commands = builder.CompileCommands();

        commands.Count.ShouldBe(1);
        commands[0].CommandText.ShouldBe("delete from incoming");
    }

    [Fact]
    public void each_split_command_only_carries_the_parameters_its_own_statement_bound()
    {
        var builder = new FirebirdDbCommandBuilder();

        builder.Append("delete from incoming where id = ");
        builder.AppendParameter(1);

        builder.StartNewCommand();

        builder.Append("delete from dead_letters where node = ");
        builder.AppendParameter(5);
        builder.Append(" and name = ");
        builder.AppendParameter("x");

        var commands = builder.CompileCommands();

        commands.Count.ShouldBe(2);

        commands[0].Parameters.Count.ShouldBe(1);
        commands[0].Parameters[0].ParameterName.ShouldBe("p0");
        commands[0].Parameters[0].Value.ShouldBe(1);

        commands[1].CommandText.ShouldBe("delete from dead_letters where node = @p1 and name = @p2");
        commands[1].Parameters.Cast<DbParameter>().Select(x => x.ParameterName).ShouldBe(["p1", "p2"]);
    }

    [Fact]
    public void a_named_parameter_shared_by_two_statements_is_bound_to_both()
    {
        var builder = new FirebirdDbCommandBuilder();

        builder.Append("select 1 from rdb$relations where rdb$relation_name = @table");
        builder.AddNamedParameter("table", "ORDERS");

        builder.StartNewCommand();

        builder.Append("select 1 from rdb$indices where rdb$relation_name = @table");

        var commands = builder.CompileCommands();

        commands[0].Parameters["table"].Value.ShouldBe("ORDERS");
        commands[1].Parameters["table"].Value.ShouldBe("ORDERS");
        commands[1].Parameters["table"].ShouldNotBeSameAs(commands[0].Parameters["table"],
            "an FbParameter cannot belong to two collections, so the shared one is cloned");
    }

    [Fact]
    public void a_parameter_is_not_shared_just_because_its_name_is_a_prefix_of_another()
    {
        var builder = new FirebirdDbCommandBuilder();

        builder.Append("delete from incoming where a = ");
        builder.AppendParameter(1);
        builder.Append(" and b = ");
        builder.AppendParameter(2);

        builder.StartNewCommand();

        builder.Append("delete from outgoing where c = @p11");

        var commands = builder.CompileCommands();

        commands[0].Parameters.Count.ShouldBe(2);
        commands[1].Parameters.Count.ShouldBe(0);
    }

    [Fact]
    public void split_commands_share_the_connection()
    {
        var connection = new FbConnection();
        var builder = new FirebirdDbCommandBuilder(connection);

        builder.Append("select 1 from rdb$database");
        builder.StartNewCommand();
        builder.Append("select 2 from rdb$database");

        builder.CompileCommands().ShouldAllBe(x => x.Connection == connection);
    }

    /// <summary>
    ///     A single statement executes the very command that was built, options and all, so several
    ///     have to carry the same options -- the gap upstream's #660 closed for Oracle's split commands.
    /// </summary>
    [Fact]
    public void split_commands_carry_the_options_set_on_the_command_being_built()
    {
        var command = new FbCommand { CommandTimeout = 17, FetchSize = 50 };
        var builder = new FirebirdDbCommandBuilder(command);

        builder.Append("select 1 from rdb$database");
        builder.StartNewCommand();
        builder.Append("select 2 from rdb$database");

        var commands = builder.CompileCommands().Cast<FbCommand>().ToArray();

        commands.Length.ShouldBe(2);
        commands.ShouldAllBe(x => x.CommandTimeout == 17);
        commands.ShouldAllBe(x => x.FetchSize == 50);
    }

    [Fact]
    public void command_count_reports_the_open_statement_too()
    {
        var builder = new FirebirdDbCommandBuilder();
        builder.CommandCount.ShouldBe(0);

        builder.Append("delete from incoming");
        builder.CommandCount.ShouldBe(1);

        builder.StartNewCommand();
        builder.CommandCount.ShouldBe(1);

        builder.Append("delete from outgoing");
        builder.CommandCount.ShouldBe(2);
    }

    [Fact]
    public void guids_are_bound_as_firebird_guids()
    {
        var builder = new FirebirdDbCommandBuilder();

        builder.Append("select 1 from rdb$database where ");
        builder.AppendParameter(Guid.NewGuid());

        ((FbParameter)builder.CompileCommands()[0].Parameters[0]).FbDbType.ShouldBe(FbDbType.Guid);
    }

    [Fact]
    public void date_time_offsets_are_bound_as_zoned_timestamps()
    {
        var builder = new FirebirdDbCommandBuilder();

        builder.Append("delete from incoming where expires <= ");
        builder.AppendParameter(DateTimeOffset.UtcNow);

        var parameter = (FbParameter)builder.CompileCommands()[0].Parameters[0];

        parameter.FbDbType.ShouldBe(FbDbType.TimeStampTZ);
        parameter.Value.ShouldBeOfType<FbZonedDateTime>();
    }

    [Fact]
    public void null_values_are_bound_as_db_null()
    {
        var builder = new FirebirdDbCommandBuilder();

        builder.Append("select 1 from rdb$database where 1 = ");
        builder.AppendParameter((object?)null);

        builder.CompileCommands()[0].Parameters[0].Value.ShouldBe(DBNull.Value);
    }

    [Fact]
    public void append_with_db_parameters_uses_the_at_sign_marker()
    {
        var builder = new FirebirdDbCommandBuilder();

        builder.Append("select data from messages where ");
        DbParameter[] parameters = builder.AppendWithDbParameters("foo = ? and bar = ?");

        parameters.Length.ShouldBe(2);
        builder.ToString().ShouldBe("select data from messages where foo = @p0 and bar = @p1");
    }

    [Fact]
    public void a_builder_without_an_open_connection_knows_no_server_version()
    {
        new FirebirdDbCommandBuilder().ServerVersion.ShouldBeNull();
        new FirebirdDbCommandBuilder(new FbConnection()).ServerVersion.ShouldBeNull();
    }
}
