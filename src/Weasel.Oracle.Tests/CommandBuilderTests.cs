using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Shouldly;
using Xunit;

namespace Weasel.Oracle.Tests;

public class CommandBuilderTests
{
    [Fact]
    public void uses_the_oracle_bind_marker()
    {
        var builder = new CommandBuilder();

        builder.Append("select data from messages where ");
        builder.AppendWithParameters("foo = ?").Length.ShouldBe(1);

        builder.ToString().ShouldBe("select data from messages where foo = :p0");
    }

    [Fact]
    public void binds_by_name()
    {
        var command = new OracleCommand();
        _ = new CommandBuilder(command);

        command.BindByName.ShouldBeTrue();
    }

    /// <summary>
    ///     The Guid conversion used to be a `new` member, so every one of the base class's typed
    ///     AppendParameter overloads routed straight past it through AddParameter and handed a raw
    ///     Guid to OracleParameter.Value. Regression guard for that.
    /// </summary>
    [Fact]
    public void typed_guid_overload_converts_to_raw()
    {
        var id = Guid.NewGuid();
        var builder = new CommandBuilder();

        builder.Append("select 1 from dual where id = ");
        builder.AppendParameter(id);

        var parameter = builder.Compile().Parameters[0];
        parameter.OracleDbType.ShouldBe(OracleDbType.Raw);
        parameter.Value.ShouldBe(id.ToByteArray());
    }

    [Fact]
    public void boxed_guid_converts_to_raw()
    {
        var id = Guid.NewGuid();
        var builder = new CommandBuilder();

        builder.Append("select 1 from dual where id = ");
        builder.AppendParameter((object)id);

        var parameter = builder.Compile().Parameters[0];
        parameter.OracleDbType.ShouldBe(OracleDbType.Raw);
        parameter.Value.ShouldBe(id.ToByteArray());
    }

    [Fact]
    public void named_boolean_parameter_converts_to_an_oracle_number()
    {
        var builder = new CommandBuilder();

        builder.Append("update dead_letters set replayable = ");
        builder.AddNamedParameter("replayable", true);

        var parameter = builder.Compile().Parameters["replayable"];
        parameter.OracleDbType.ShouldBe(OracleDbType.Int16);
        parameter.Value.ShouldBe(1);
    }

    [Fact]
    public void named_guid_parameter_converts_to_raw()
    {
        var id = Guid.NewGuid();
        var builder = new CommandBuilder();

        builder.AddNamedParameter("id", id);

        var parameter = builder.Compile().Parameters["id"];
        parameter.OracleDbType.ShouldBe(OracleDbType.Raw);
        parameter.Value.ShouldBe(id.ToByteArray());
    }

    [Fact]
    public void implements_the_dialect_neutral_command_builder()
    {
        Weasel.Core.ICommandBuilder builder = new CommandBuilder();

        builder.Append("select data from messages where foo = ");
        var parameter = builder.AppendParameter(5);

        parameter.ShouldBeOfType<OracleParameter>();
        builder.ToString().ShouldBe("select data from messages where foo = :p0");
    }

    [Fact]
    public void append_with_db_parameters_returns_neutral_db_parameters()
    {
        Weasel.Core.ICommandBuilder builder = new CommandBuilder();

        builder.Append("select data from messages where ");
        DbParameter[] parameters = builder.AppendWithDbParameters("foo = ? and bar = ?");

        parameters.Length.ShouldBe(2);
        builder.ToString().ShouldBe("select data from messages where foo = :p0 and bar = :p1");
    }

    [Fact]
    public void grouped_parameter_builder_appends_a_separated_run()
    {
        Weasel.Core.ICommandBuilder builder = new CommandBuilder();

        builder.Append("select data from messages where id in (");
        var grouped = builder.CreateGroupedParameterBuilder(',');
        grouped.AppendParameter(1);
        grouped.AppendParameter(2);
        builder.Append(")");

        builder.ToString().ShouldBe("select data from messages where id in (:p0,:p1)");
    }

    /// <summary>
    ///     Oracle is the one provider that splits a batch. Everything else concatenates, so
    ///     StartNewCommand has to stay free for them.
    /// </summary>
    [Fact]
    public void start_new_command_is_a_no_op_on_the_plain_command_builder()
    {
        var builder = new CommandBuilder();

        builder.Append("delete from incoming");
        builder.StartNewCommand();
        builder.Append(";delete from outgoing");

        builder.CommandCount.ShouldBe(1);
        builder.CompileCommands().Count.ShouldBe(1);
        builder.CompileCommands()[0].CommandText.ShouldBe("delete from incoming;delete from outgoing");
    }
}
