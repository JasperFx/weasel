using JasperFx;
using JasperFx.Core;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

public class rolling_back_table_deltas: IntegrationContext
{
    private Table initial;
    private Table configured;

    public rolling_back_table_deltas(): base("rollbacks")
    {
        initial = new Table("rollbacks.people");
        initial.AddColumn<int>("id").AsPrimaryKey();
        initial.AddColumn<string>("first_name");
        initial.AddColumn<string>("last_name");
        initial.AddColumn<string>("user_name");
        initial.AddColumn("data", "text");

        configured = new Table("rollbacks.people");
        configured.AddColumn<int>("id").AsPrimaryKey();
        configured.AddColumn<string>("first_name");
        configured.AddColumn<string>("last_name");
        configured.AddColumn<string>("user_name");
        configured.AddColumn("data", "text");
    }

    private async Task AssertRollbackIsSuccessful(params Table[] otherTables)
    {
        await ResetSchema();

        foreach (var table in otherTables)
        {
            await CreateSchemaObjectInDatabase(table);
        }

        await CreateSchemaObjectInDatabase(initial);

        await Task.Delay(100.Milliseconds());

        var delta = await configured.FindDeltaAsync(theConnection);

        var migration = new SchemaMigration(new ISchemaObjectDelta[] { delta });

        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        await Task.Delay(100.Milliseconds());

        await migration.RollbackAllAsync(theConnection, new SqlServerMigrator());

        var delta2 = await initial.FindDeltaAsync(theConnection);
        delta2.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task columns_forward_and_backwards()
    {
        initial.AddColumn<string>("random");
        configured.AddColumn<string>("other");

        await AssertRollbackIsSuccessful();
    }

    /// <summary>
    ///     weasel#668 on the way back. Rolling the drop out re-adds the column and then recreates
    ///     the filtered index on it; a predicate is an expression, so SQL Server binds it when it
    ///     compiles the batch, and without a separator after the column the whole rollback failed
    ///     to compile with "Invalid column name" -- leaving neither the column nor the index.
    /// </summary>
    [Fact]
    public async Task a_filtered_index_on_a_dropped_column_comes_back()
    {
        initial.AddColumn<DateTimeOffset>("expires");

        var index = new IndexDefinition("idx_people_expires") { Predicate = "[expires] IS NOT NULL" };
        index.AgainstColumns("expires");
        initial.Indexes.Add(index);

        await AssertRollbackIsSuccessful();
    }

    /// <summary>
    ///     weasel#670. The forward pass adds the check constraints the model declares, and the
    ///     rollback had no counterpart at all, so a rolled-back migration left the new constraint
    ///     in place.
    /// </summary>
    /// <remarks>
    ///     Asserted against the catalog rather than through a delta, and that is the whole reason
    ///     this went unnoticed: SQL Server's comparison deliberately never treats an actual check
    ///     constraint the expected table does not declare as an extra to drop, so the leftover
    ///     constraint is invisible to <c>FindDeltaAsync</c> -- <c>initial</c> declares none, and the
    ///     rollback looks complete while the constraint is still on the table.
    /// </remarks>
    [Fact]
    public async Task a_check_constraint_added_by_the_migration_is_dropped_again()
    {
        initial.AddColumn<int>("rating");
        configured.AddColumn<int>("rating");
        configured.CheckConstraints.Add(new TableCheckConstraint("ck_people_rating", "[rating] > 0"));

        await AssertRollbackIsSuccessful();

        (await checkConstraintNames()).ShouldBeEmpty();
    }

    /// <summary>
    ///     The check constraints actually on <c>rollbacks.people</c>, read straight from the catalog.
    /// </summary>
    private async Task<string[]> checkConstraintNames()
    {
        var names = new List<string>();

        await using var reader = await theConnection
            .CreateCommand(
                "select name from sys.check_constraints where parent_object_id = object_id('rollbacks.people')")
            .ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            names.Add(reader.GetString(0));
        }

        return names.ToArray();
    }

    /// <summary>
    ///     weasel#670, the worse half: the original expression was simply gone, so the next delta
    ///     saw the replacement as the actual state and reported no difference.
    /// </summary>
    [Fact]
    public async Task a_replaced_check_constraint_expression_is_restored()
    {
        initial.AddColumn<int>("rating");
        initial.CheckConstraints.Add(new TableCheckConstraint("ck_people_rating", "[rating] > 0"));

        configured.AddColumn<int>("rating");
        configured.CheckConstraints.Add(new TableCheckConstraint("ck_people_rating", "[rating] > 10"));

        await AssertRollbackIsSuccessful();
    }

    /// <summary>
    ///     The two halves of weasel#670 and weasel#668 together: the rollback has to drop the check
    ///     before it drops the column the check is on, and the restored check names a column the
    ///     rollback itself re-adds, which needs the batch separator between them.
    /// </summary>
    [Fact]
    public async Task a_check_constraint_on_a_column_the_migration_added_rolls_back()
    {
        configured.AddColumn<int>("rating");
        configured.CheckConstraints.Add(new TableCheckConstraint("ck_people_rating", "[rating] > 0"));

        await AssertRollbackIsSuccessful();
    }

    [Fact]
    public async Task indexes_forward_and_backwards()
    {
        initial.ModifyColumn("user_name").AddIndex();
        configured.ModifyColumn("last_name").AddIndex();

        await AssertRollbackIsSuccessful();
    }

    [Fact]
    public async Task changed_index()
    {
        initial.ModifyColumn("user_name").AddIndex();
        configured.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);

        await AssertRollbackIsSuccessful();
    }

    [Fact]
    public async Task new_fkey_should_be_dropped_on_rollback()
    {
        var states = new Table("rollbacks.states");
        states.AddColumn<int>("id").AsPrimaryKey();

        configured.AddColumn<int>("state_id")
            .ForeignKeyTo(states, "id");

        await AssertRollbackIsSuccessful(states);
    }

    [Fact]
    public async Task if_an_fkey_is_removed_rollback_should_put_it_back()
    {
        var states = new Table("rollbacks.states");
        states.AddColumn<int>("id").AsPrimaryKey();

        initial.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await AssertRollbackIsSuccessful(states);
    }

    [Fact]
    public async Task changed_primary_key()
    {
        configured.AddColumn<string>("tenant_id");

        await AssertRollbackIsSuccessful();
    }

}

public class table_delta_rollback_sql_ordering
{
    [Fact]
    public void rollback_sql_drops_pk_constraint_before_altering_changed_pk_column_type()
    {
        var initial = new Table("rollbacks.string_pk_ordering");
        initial.AddColumn("id", "varchar(100)").AsPrimaryKey();
        initial.AddColumn<string>("first_name");

        var configured = new Table("rollbacks.string_pk_ordering");
        configured.AddColumn("id", "nvarchar(100)").AsPrimaryKey();
        configured.AddColumn<string>("first_name");

        var delta = new TableDelta(configured, initial);

        var writer = new StringWriter();
        delta.WriteRollback(new SqlServerMigrator(), writer);

        var sql = writer.ToString();
        var drop = $"alter table {configured.Identifier} drop constraint if exists {configured.PrimaryKeyName};";
        var alter = $"alter table {configured.Identifier} alter column id varchar(100) NOT NULL;";
        var add = $"alter table {configured.Identifier} add CONSTRAINT {initial.PrimaryKeyName} PRIMARY KEY (id);";

        sql.IndexOf(drop, StringComparison.OrdinalIgnoreCase).ShouldBeGreaterThanOrEqualTo(0);
        sql.IndexOf(alter, StringComparison.OrdinalIgnoreCase)
            .ShouldBeGreaterThan(sql.IndexOf(drop, StringComparison.OrdinalIgnoreCase));
        sql.IndexOf(add, StringComparison.OrdinalIgnoreCase)
            .ShouldBeGreaterThan(sql.IndexOf(alter, StringComparison.OrdinalIgnoreCase));
    }
}

public class table_delta_rollback_index_sql
{
    [Fact]
    public void rollback_of_a_new_index_drops_it_only_if_it_exists()
    {
        var initial = new Table("rollbacks.people");
        initial.AddColumn<int>("id").AsPrimaryKey();
        initial.AddColumn<string>("user_name");

        var configured = new Table("rollbacks.people");
        configured.AddColumn<int>("id").AsPrimaryKey();
        configured.AddColumn<string>("user_name").AddIndex();

        var delta = new TableDelta(configured, initial);
        delta.Indexes.Missing.Single().Name.ShouldBe("idx_people_user_name");

        var writer = new StringWriter();
        delta.WriteRollback(new SqlServerMigrator(), writer);

        writer.ToString().ShouldContain("drop index if exists idx_people_user_name on rollbacks.people;");
    }
}
