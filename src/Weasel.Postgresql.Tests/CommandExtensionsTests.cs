using System.Data;
using Npgsql;
using NpgsqlTypes;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Postgresql.Tests;

using static PostgresqlProvider;

public class CommandExtensionsTests
{
    [Fact]
    public void add_first_parameter()
    {
        var command = new NpgsqlCommand();

        var param = command.AddParameter("a");

        param.Value.ShouldBe("a");
        param.ParameterName.ShouldBe("p0");

        // AddParameter was given no explicit type, so this asserts *Npgsql's* inference from
        // the value rather than anything Weasel stamped on. That inference reads Npgsql's
        // process-global type mapper and answers Unknown until the mapper has been seeded,
        // which TestSetup does once at module load. Do not delete that warm-up. weasel#398.
        param.NpgsqlDbType.ShouldBe(NpgsqlDbType.Text);

        command.Parameters.ShouldContain(param);
    }

    [Fact]
    public void add_parameter_honors_an_explicit_type()
    {
        var command = new NpgsqlCommand();

        var param = command.AddParameter("a", NpgsqlDbType.Varchar);

        // The explicit-type path is the one Weasel actually controls, so unlike the test
        // above it holds no matter what state Npgsql's global type mapper is in.
        param.NpgsqlDbType.ShouldBe(NpgsqlDbType.Varchar);
    }

    [Fact]
    public void add_parameter_without_a_type_defers_to_npgsql_rather_than_weasels_mapping()
    {
        var command = new NpgsqlCommand();

        var param = command.AddParameter(new DateTime(2026, 7, 30, 12, 0, 0, DateTimeKind.Utc));

        // Weasel must NOT start stamping its own CLR-type mapping onto untyped parameters.
        // Weasel maps DateTime to "timestamp without time zone" for every value, while
        // Npgsql resolves per value: a Kind=Utc DateTime is "timestamp with time zone", and
        // writing one as "timestamp without time zone" throws at execution time. weasel#398.
        param.NpgsqlDbType.ShouldBe(NpgsqlDbType.TimestampTz);
        Instance.ToParameterType(typeof(DateTime)).ShouldBe(NpgsqlDbType.Timestamp);
    }

    [Fact]
    public void add_second_parameter()
    {
        var command = new NpgsqlCommand();

        command.AddParameter("a");
        var param = command.AddParameter("b");

        param.ParameterName.ShouldBe("p1");
    }

    [Fact]
    public void Sql_extension_method()
    {
        var command = new NpgsqlCommand();
        command.Sql("select 1").ShouldBeSameAs(command);

        command.CommandText.ShouldBe("select 1");
    }

    [Fact]
    public void CallsSproc_extension_method()
    {
        var command = new NpgsqlCommand();
        command.CallsSproc(new PostgresqlObjectName("foo", "proc")).ShouldBeSameAs(command);
        command.CommandType.ShouldBe(CommandType.StoredProcedure);
        command.CommandText.ShouldBe("foo.proc");
    }

    [Fact]
    public void returns_extension_method()
    {
        var command = new NpgsqlCommand();
        command.Returns("returnValue", NpgsqlDbType.Double).ShouldBeSameAs(command);

        var returnParam = command.Parameters.Single();
        returnParam.Direction.ShouldBe(ParameterDirection.ReturnValue);
        returnParam.ParameterName.ShouldBe("returnValue");
    }

    [Fact]
    public void CallsSproc_extension_method_by_string()
    {
        var command = new NpgsqlCommand();
        command.CallsSproc("foo.proc").ShouldBeSameAs(command);
        command.CommandType.ShouldBe(CommandType.StoredProcedure);
        command.CommandText.ShouldBe("foo.proc");
    }
}
