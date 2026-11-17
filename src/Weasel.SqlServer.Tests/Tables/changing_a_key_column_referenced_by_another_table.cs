using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     weasel#870: widening a key column from varchar to nvarchar on two tables linked by a foreign key.
///     The referenced table's primary key can't be dropped, nor its column retyped, while the other table's
///     key points at it, and the other table's key can't go back until both sides have the same type.
/// </summary>
public class changing_a_key_column_referenced_by_another_table: IntegrationContext
{
    public changing_a_key_column_referenced_by_another_table(): base("keyfk")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private static Table customers(string type)
    {
        var table = new Table("keyfk.customers");
        table.AddColumn("tenant_id", type).AsPrimaryKey().NotNull();
        table.AddColumn("id", type).AsPrimaryKey().NotNull();
        table.AddColumn<string>("name");
        return table;
    }

    private static Table orders(string type, CascadeAction onDelete = CascadeAction.NoAction)
    {
        var table = new Table("keyfk.orders");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn("tenant_id", type).NotNull();
        table.AddColumn("customer_id", type).NotNull();
        table.ForeignKeys.Add(new ForeignKey("fk_orders_customer")
        {
            LinkedTable = new SqlServerObjectName("keyfk", "customers"),
            ColumnNames = ["tenant_id", "customer_id"],
            LinkedNames = ["tenant_id", "id"],
            OnDelete = onDelete
        });
        return table;
    }

    private async Task applyAsync(params Table[] tables)
    {
        var migration = await SchemaMigration.DetermineAsync(theConnection, tables);
        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, JasperFx.AutoCreate.CreateOrUpdate);
    }

    private async Task seedAsync()
    {
        await applyAsync(customers("varchar(250)"), orders("varchar(250)"));
        await executeAsync("""
            insert into keyfk.customers (tenant_id, id, name) values ('red', 'c1', 'one'), ('blue', 'c1', 'two');
            insert into keyfk.orders (id, tenant_id, customer_id) values (newid(), 'red', 'c1'), (newid(), 'blue', 'c1');
            """);
    }

    private async Task executeAsync(string sql)
    {
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<int> countAsync(string sql)
    {
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    private async Task theKeyIsBackAndEnforcedAsync()
    {
        (await countAsync("select count(*) from sys.foreign_keys where name = 'fk_orders_customer'")).ShouldBe(1);
        (await countAsync("select count(*) from keyfk.orders")).ShouldBe(2);

        var refused = await Should.ThrowAsync<SqlException>(() =>
            executeAsync("insert into keyfk.orders (id, tenant_id, customer_id) values (newid(), N'東京', N'c1');"));
        refused.Number.ShouldBe(547);

        await executeAsync("""
            insert into keyfk.customers (tenant_id, id, name) values (N'東京', N'c1', N'three');
            insert into keyfk.orders (id, tenant_id, customer_id) values (newid(), N'東京', N'c1');
            """);
        (await countAsync("select count(*) from keyfk.orders where tenant_id = N'東京'")).ShouldBe(1);
    }

    [Fact]
    public async Task the_referenced_table_reads_the_keys_that_reference_it()
    {
        await applyAsync(customers("varchar(250)"), orders("varchar(250)", CascadeAction.Cascade));

        var existing = await customers("varchar(250)").FetchExistingAsync(theConnection);

        var referencing = existing!.ReferencingForeignKeys.Single();
        referencing.Table.QualifiedName.ShouldBe("keyfk.orders");
        referencing.ForeignKey.Name.ShouldBe("fk_orders_customer");
        referencing.ForeignKey.ColumnNames.ShouldBe(["tenant_id", "customer_id"]);
        referencing.ForeignKey.LinkedNames.ShouldBe(["tenant_id", "id"]);
        referencing.ForeignKey.OnDelete.ShouldBe(CascadeAction.Cascade);

        // A table's own keys are not among them
        (await orders("varchar(250)").FetchExistingAsync(theConnection))!.ReferencingForeignKeys.ShouldBeEmpty();
    }

    [Fact]
    public async Task both_tables_in_one_migration_referenced_table_first()
    {
        await seedAsync();

        await applyAsync(customers("nvarchar(250)"), orders("nvarchar(250)"));

        (await customers("nvarchar(250)").FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await orders("nvarchar(250)").FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        await theKeyIsBackAndEnforcedAsync();
    }

    [Fact]
    public async Task both_tables_in_one_migration_referencing_table_first()
    {
        await seedAsync();

        await applyAsync(orders("nvarchar(250)"), customers("nvarchar(250)"));

        (await orders("nvarchar(250)").FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await customers("nvarchar(250)").FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        await theKeyIsBackAndEnforcedAsync();
    }

    [Fact]
    public async Task one_table_at_a_time_referenced_table_first()
    {
        await seedAsync();

        await applyAsync(customers("nvarchar(250)"));

        // Off until the other side is the same type
        (await countAsync("select count(*) from sys.foreign_keys where name = 'fk_orders_customer'")).ShouldBe(0);

        await applyAsync(orders("nvarchar(250)"));

        (await orders("nvarchar(250)").FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        await theKeyIsBackAndEnforcedAsync();
    }

    [Fact]
    public async Task one_table_at_a_time_referencing_table_first()
    {
        await seedAsync();

        await applyAsync(orders("nvarchar(250)"));
        (await countAsync("select count(*) from sys.foreign_keys where name = 'fk_orders_customer'")).ShouldBe(0);

        // The referencing side's own delta says what is missing in the meantime
        (await orders("nvarchar(250)").FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await applyAsync(customers("nvarchar(250)"));

        // Nothing referenced customers by the time it changed, so the key is the referencing side's to add,
        // as it would be for any key its table declares and the database doesn't have
        await applyAsync(orders("nvarchar(250)"));

        (await orders("nvarchar(250)").FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        await theKeyIsBackAndEnforcedAsync();
    }

    [Fact]
    public async Task a_reference_to_a_column_that_does_not_change_is_left_alone()
    {
        await seedAsync();

        // Only name changes, so nothing about the key is touched
        var renamed = new Table("keyfk.customers");
        renamed.AddColumn("tenant_id", "varchar(250)").AsPrimaryKey().NotNull();
        renamed.AddColumn("id", "varchar(250)").AsPrimaryKey().NotNull();
        renamed.AddColumn("name", "nvarchar(500)");

        var delta = await renamed.FindDeltaAsync(theConnection);
        var sql = new StringWriter();
        delta.WriteUpdate(new SqlServerMigrator(), sql);
        sql.ToString().ShouldNotContain("fk_orders_customer", Case.Insensitive);
    }

    [Fact]
    public async Task a_rendered_script_runs_in_either_order()
    {
        await seedAsync();

        var migration = await SchemaMigration.DetermineAsync(theConnection,
            [orders("nvarchar(250)"), customers("nvarchar(250)")]);
        var script = new StringWriter();
        migration.WriteAllUpdates(script, new SqlServerMigrator(), JasperFx.AutoCreate.CreateOrUpdate);

        foreach (var batch in new SqlServerMigrator().SplitIntoBatches(script.ToString()))
        {
            if (batch.Trim().IsNotEmpty()) await executeAsync(batch);
        }

        await theKeyIsBackAndEnforcedAsync();
    }
}
