using System.Data;
using System.Data.Odbc;
using System.Linq;
using Npgsql;
using NpgsqlTypes;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    public class CommandExtensionsTests
    {
        [Fact]
        public void add_first_parameter()
        {
            var command = new NpgsqlCommand();

            var param = command.AddParameter("a");

            param.Value.ShouldBe("a");
            param.ParameterName.ShouldBe("p0");

            param.NpgsqlDbType.ShouldBe(NpgsqlDbType.Text);

            command.Parameters.ShouldContain(param);
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
            command.CallsSproc(DbObjectName.Parse("foo.func")).ShouldBeSameAs(command);
            command.CommandType.ShouldBe(CommandType.StoredProcedure);
            command.CommandText.ShouldBe("foo.func");
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
            command.CallsSproc("foo.func").ShouldBeSameAs(command);
            command.CommandType.ShouldBe(CommandType.StoredProcedure);
            command.CommandText.ShouldBe("foo.func");
        }
    }
}
