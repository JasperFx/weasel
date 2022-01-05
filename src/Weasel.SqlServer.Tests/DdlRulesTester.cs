using System.IO;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests
{
    public class DdlRulesTester
    {
        [Fact]
        public void is_transactional_is_true_by_default()
        {
            new SqlServerMigrator().IsTransactional.ShouldBeTrue();
        }

        [Fact]
        public void role_is_null_by_default()
        {
            new SqlServerMigrator().Role.ShouldBeNull();
        }

        [Fact]
        public void table_creation_is_drop_then_create_by_default()
        {
            new SqlServerMigrator().TableCreation.ShouldBe(CreationStyle.DropThenCreate);
        }

        [Fact]
        public void upsert_rights_are_by_invoker_by_default()
        {
            new SqlServerMigrator().UpsertRights.ShouldBe(SecurityRights.Invoker);
        }

        [Fact]
        public void write_transactional_script_with_no_role()
        {
            var rules = new SqlServerMigrator();
            rules.Role.ShouldBeNull();

            var writer = new StringWriter();



            rules.WriteScript(writer, (r, w) =>
            {
                w.WriteLine("Hello.");
            });

            writer.ToString().ShouldNotContain("SET ROLE");
            writer.ToString().ShouldNotContain("RESET ROLE;");
        }

    }
}
