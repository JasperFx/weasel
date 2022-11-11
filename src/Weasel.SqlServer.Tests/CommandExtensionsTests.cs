using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests
{
    public class CommandExtensionsTests
    {
        [Fact]
        public void add_first_parameter()
        {
            var command = new SqlCommand();

            var param = command.AddParameter("a");

            param.Value.ShouldBe("a");
            param.ParameterName.ShouldBe("p0");

            param.SqlDbType.ShouldBe(SqlDbType.NVarChar);

            command.Parameters.OfType<SqlParameter>().ShouldContain(param);
        }

        [Fact]
        public void add_second_parameter()
        {
            var command = new SqlCommand();

            command.AddParameter("a");
            var param = command.AddParameter("b");

            param.ParameterName.ShouldBe("p1");
        }

        [Fact]
        public void Sql_extension_method()
        {
            var command = new SqlCommand();
            command.Sql("select 1").ShouldBeSameAs(command);

            command.CommandText.ShouldBe("select 1");

        }

        [Fact]
        public void CallsSproc_extension_method()
        {
            var command = new SqlCommand();
            command.CallsSproc(DbObjectName.Parse(SqlServerProvider.Instance, "foo.proc")).ShouldBeSameAs(command);
            command.CommandType.ShouldBe(CommandType.StoredProcedure);
            command.CommandText.ShouldBe("foo.proc");
        }

        [Fact]
        public void returns_extension_method()
        {
            var command = new SqlCommand();
            command.Returns("returnValue", SqlDbType.Float).ShouldBeSameAs(command);

            var returnParam = command.Parameters.OfType<SqlParameter>().Single();
            returnParam.Direction.ShouldBe(ParameterDirection.ReturnValue);
            returnParam.ParameterName.ShouldBe("returnValue");

        }

        [Fact]
        public void CallsSproc_extension_method_by_string()
        {
            var command = new SqlCommand();
            command.CallsSproc("foo.proc").ShouldBeSameAs(command);
            command.CommandType.ShouldBe(CommandType.StoredProcedure);
            command.CommandText.ShouldBe("foo.proc");
        }
    }
}
