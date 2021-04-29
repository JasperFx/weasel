using System;
using System.IO;
using System.Runtime.ExceptionServices;
using Npgsql;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    public class SchemaPatchTester
    {
        private void handleException(NpgsqlCommand cmd, Exception ex)
        {
            ExceptionDispatchInfo.Capture(ex).Throw();
        }

        [Fact]
        public void translates_the_file_name()
        {
            SchemaPatch.ToDropFileName("update.sql").ShouldBe("update.drop.sql");
            SchemaPatch.ToDropFileName("1.update.sql").ShouldBe("1.update.drop.sql");
            SchemaPatch.ToDropFileName("folder\\1.update.sql").ShouldBe("folder\\1.update.drop.sql");
        }

        [Fact]
        public void write_transactional_script_with_no_role()
        {
            var rules = new DdlRules();
            rules.Role.ShouldBeNull();

            var patch = new SchemaPatch(rules, handleException);

            var writer = new StringWriter();

            patch.WriteScript(writer, w =>
            {
                w.WriteLine("Hello.");
            });

            writer.ToString().ShouldNotContain("SET ROLE");
            writer.ToString().ShouldNotContain("RESET ROLE;");
        }

        [Fact]
        public void write_transactional_script_with_a_role()
        {
            var rules = new DdlRules();
            rules.Role = "OCD_DBA";

            var patch = new SchemaPatch(rules, handleException);

            var writer = new StringWriter();

            patch.WriteScript(writer, w =>
            {
                w.WriteLine("Hello.");
            });

            writer.ToString().ShouldContain("SET ROLE OCD_DBA;");
            writer.ToString().ShouldContain("RESET ROLE;");
        }

        [Fact]
        public void difference_is_none_by_default()
        {
            new SchemaPatch(new DdlRules(), handleException).Difference.ShouldBe(SchemaPatchDifference.None);
        }

        [Fact]
        public void invalid_wins_over_all_else()
        {
            var patch = new SchemaPatch(new DdlRules(), handleException);
            var table1 = new Table(new DbObjectName("public", "sometable1"));
            var table2 = new Table(new DbObjectName("public", "sometable2"));
            var table3 = new Table(new DbObjectName("public", "sometable3"));
            var table4 = new Table(new DbObjectName("public", "sometable4"));

            patch.Log(table1, SchemaPatchDifference.Invalid);
            patch.Log(table2, SchemaPatchDifference.Create);
            patch.Log(table3, SchemaPatchDifference.None);
            patch.Log(table4, SchemaPatchDifference.Update);

            patch.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        }


        [Fact]
        public void update_is_second_in_priority()
        {
            var patch = new SchemaPatch(new DdlRules(), handleException);
            var table1 = new Table(new DbObjectName("public", "sometable1"));
            var table2 = new Table(new DbObjectName("public", "sometable2"));
            var table3 = new Table(new DbObjectName("public", "sometable3"));
            var table4 = new Table(new DbObjectName("public", "sometable4"));

            //patch.Log(table1, SchemaPatchDifference.Invalid);
            patch.Log(table2, SchemaPatchDifference.Create);
            patch.Log(table3, SchemaPatchDifference.None);
            patch.Log(table4, SchemaPatchDifference.Update);

            patch.Difference.ShouldBe(SchemaPatchDifference.Update);
        }

        [Fact]
        public void create_takes_precedence_over_none()
        {
            var patch = new SchemaPatch(new DdlRules(), handleException);
            var table1 = new Table(new DbObjectName("public", "sometable1"));
            var table2 = new Table(new DbObjectName("public", "sometable2"));
            var table3 = new Table(new DbObjectName("public", "sometable3"));
            var table4 = new Table(new DbObjectName("public", "sometable4"));

            //patch.Log(table1, SchemaPatchDifference.Invalid);
            patch.Log(table2, SchemaPatchDifference.Create);
            patch.Log(table3, SchemaPatchDifference.None);
            //patch.Log(table4, SchemaPatchDifference.Update);

            patch.Difference.ShouldBe(SchemaPatchDifference.Create);
        }

        [Fact]
        public void return_none_if_no_changes_detected()
        {
            var patch = new SchemaPatch(new DdlRules(), handleException);
            var table1 = new Table(new DbObjectName("public", "sometable1"));
            var table2 = new Table(new DbObjectName("public", "sometable2"));
            var table3 = new Table(new DbObjectName("public", "sometable3"));
            var table4 = new Table(new DbObjectName("public", "sometable4"));

            patch.Log(table1, SchemaPatchDifference.None);
            patch.Log(table2, SchemaPatchDifference.None);
            patch.Log(table3, SchemaPatchDifference.None);
            patch.Log(table4, SchemaPatchDifference.None);

            patch.Difference.ShouldBe(SchemaPatchDifference.None);
        }

        [Theory]
        [InlineData(SchemaPatchDifference.Invalid, AutoCreate.CreateOnly)]
        [InlineData(SchemaPatchDifference.Update, AutoCreate.CreateOnly)]
        [InlineData(SchemaPatchDifference.Invalid, AutoCreate.CreateOrUpdate)]
        public void should_throw_exception_on_assertion(SchemaPatchDifference difference, AutoCreate autoCreate)
        {
            var patch = new SchemaPatch(new DdlRules(), handleException);
            var table1 = new Table(new DbObjectName("public", "sometable1"));
            patch.Log(table1, difference);

            Should.Throw<InvalidOperationException>(() =>
            {
                patch.AssertPatchingIsValid(autoCreate);
            });

        }

        [Theory]
        [InlineData(SchemaPatchDifference.Create, AutoCreate.CreateOnly)]
        [InlineData(SchemaPatchDifference.Create, AutoCreate.CreateOrUpdate)]
        [InlineData(SchemaPatchDifference.Create, AutoCreate.All)]
        [InlineData(SchemaPatchDifference.Invalid, AutoCreate.All)] // drop and replace
        [InlineData(SchemaPatchDifference.Update, AutoCreate.CreateOrUpdate)]
        public void should_not_throw_exception_on_assertion(SchemaPatchDifference difference, AutoCreate autoCreate)
        {
            var patch = new SchemaPatch(new DdlRules(), handleException);
            var table1 = new Table(new DbObjectName("public", "sometable1"));
            patch.Log(table1, difference);

            patch.AssertPatchingIsValid(autoCreate);
        }
    }
}
