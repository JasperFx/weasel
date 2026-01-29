using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Postgresql.Tests;

public class MigratorTester
{
    [Fact]
    public void is_transactional_is_true_by_default()
    {
        new PostgresqlMigrator().IsTransactional.ShouldBeTrue();
    }

    [Fact]
    public void role_is_null_by_default()
    {
        new PostgresqlMigrator().Role.ShouldBeNull();
    }

    [Fact]
    public void table_creation_is_create_if_not_exists_by_default()
    {
        new PostgresqlMigrator().TableCreation.ShouldBe(CreationStyle.CreateIfNotExists);
    }

    [Fact]
    public void upsert_rights_are_by_invoker_by_default()
    {
        new PostgresqlMigrator().UpsertRights.ShouldBe(SecurityRights.Invoker);
    }

    [Fact]
    public void write_transactional_script_with_no_role()
    {
        var rules = new PostgresqlMigrator();
        rules.Role.ShouldBeNull();

        var writer = new StringWriter();


        rules.WriteScript(writer, (r, w) =>
        {
            w.WriteLine("Hello.");
        });

        writer.ToString().ShouldNotContain("SET ROLE");
        writer.ToString().ShouldNotContain("RESET ROLE;");
    }

    [Fact]
    public void write_transactional_script_with_a_role()
    {
        var rules = new PostgresqlMigrator();
        rules.Role = "OCD_DBA";

        var writer = new StringWriter();

        rules.WriteScript(writer, (r, w) =>
        {
            w.WriteLine("Hello.");
        });

        writer.ToString().ShouldContain("SET ROLE OCD_DBA;");
        writer.ToString().ShouldContain("RESET ROLE;");
    }
}
