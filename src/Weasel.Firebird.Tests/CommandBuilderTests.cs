using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Types;
using Shouldly;
using Xunit;

namespace Weasel.Firebird.Tests;

public class CommandBuilderTests
{
    [Fact]
    public void appends_parameters_with_the_at_sign_marker()
    {
        var builder = new CommandBuilder();

        builder.Append("select * from orders where id = ");
        builder.AppendParameter(3);

        var command = builder.Compile();

        command.CommandText.ShouldBe("select * from orders where id = @p0");
        command.Parameters[0].FbDbType.ShouldBe(FbDbType.Integer);
    }

    /// <summary>
    ///     <c>Compile()</c> is not virtual, so a splitting typed builder would hand its callers only the
    ///     last statement. This one keeps one command, whatever it is told.
    /// </summary>
    [Fact]
    public void never_splits()
    {
        var builder = new CommandBuilder();

        builder.Append("select 1 from rdb$database");
        builder.StartNewCommand();
        builder.Append(" union all select 2 from rdb$database");

        builder.CommandCount.ShouldBe(1);
        builder.Compile().CommandText.ShouldBe("select 1 from rdb$database union all select 2 from rdb$database");
    }

    [Fact]
    public void typed_append_parameter_returns_the_firebird_parameter()
    {
        ICommandBuilder builder = new CommandBuilder();

        builder.Append("select 1 from rdb$database where x = ");
        var parameter = builder.AppendParameter("name");

        parameter.ShouldBeOfType<FbParameter>();
        parameter.Value.ShouldBe("name");
    }

    [Fact]
    public void append_parameter_with_an_explicit_type()
    {
        var builder = new CommandBuilder();

        var parameter = builder.AppendParameter(Guid.NewGuid(), FbDbType.Guid);

        parameter.FbDbType.ShouldBe(FbDbType.Guid);
    }

    [Fact]
    public void appends_several_parameters_separated_by_commas()
    {
        Weasel.Core.ICommandBuilder builder = new CommandBuilder();

        builder.Append("select 1 from rdb$database where x in (");
        builder.AppendParameters(1, 2, 3);
        builder.Append(")");

        ((CommandBuilder)builder).Compile().CommandText
            .ShouldBe("select 1 from rdb$database where x in (@p0, @p1, @p2)");
    }

    [Fact]
    public void appending_no_parameters_is_refused()
    {
        Weasel.Core.ICommandBuilder builder = new CommandBuilder();

        Should.Throw<ArgumentOutOfRangeException>(() => builder.AppendParameters());
    }

    [Fact]
    public void a_named_date_time_offset_travels_as_a_zoned_timestamp()
    {
        var builder = new CommandBuilder();

        var parameter = builder.AddNamedParameter("at", DateTimeOffset.UtcNow);

        parameter.Value.ShouldBeOfType<FbZonedDateTime>();
        parameter.FbDbType.ShouldBe(FbDbType.TimeStampTZ);
    }

    [Fact]
    public void a_positional_date_time_offset_travels_as_a_zoned_timestamp()
    {
        var builder = new CommandBuilder();

        builder.AppendParameter((object)DateTimeOffset.UtcNow);

        builder.Compile().Parameters[0].Value.ShouldBeOfType<FbZonedDateTime>();
    }

    [Fact]
    public void grouped_parameters_use_the_at_sign_marker()
    {
        var builder = new CommandBuilder();

        var group = builder.CreateGroupedParameterBuilder(',');
        group.AppendParameter(1);
        group.AppendParameter(2);

        builder.Compile().CommandText.ShouldBe("@p0,@p1");
    }

    [Fact]
    public void tenant_id_defaults_to_empty()
    {
        new CommandBuilder().TenantId.ShouldBe(string.Empty);
    }

    [Fact]
    public void creates_a_command_on_a_connection_with_its_transaction()
    {
        var connection = new FbConnection();

        var command = connection.CreateCommand("select 1 from rdb$database");

        command.CommandText.ShouldBe("select 1 from rdb$database");
        command.Connection.ShouldBeSameAs(connection);
    }

    [Fact]
    public void with_finds_or_adds_a_named_parameter()
    {
        var command = new FbCommand("select 1 from rdb$database where a = @a")
            .With("a", 1)
            .With("a", 2);

        command.Parameters.Count.ShouldBe(1);
        command.Parameters["a"].Value.ShouldBe(1);
    }
}
