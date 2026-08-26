using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     Nobody upgrading should be handed a migration they did not have before.
/// </summary>
/// <remarks>
///     The ordering fixes in this change make Weasel read composite key order faithfully, which is
///     necessary — but reading it must not turn into <em>comparing</em> it for models that never
///     expressed an order. Those users would get a primary key drop and recreate (a table rebuild on
///     a clustered key) or a foreign key drop and recreate with its validation scan, purely from
///     upgrading. Each test here sets up a database in the state older Weasel would have left it and
///     declares the model the way a user would have written it. Most assert no delta; the ones named
///     for opting in, or for a genuinely different constraint, assert that the difference IS still
///     reported -- a gate that never reports anything would pass every other test here too.
/// </remarks>
public class upgrade_causes_no_new_migrations: IntegrationContext
{
    public upgrade_causes_no_new_migrations(): base("upgrade")
    {
    }

    [Fact]
    public async Task a_composite_primary_key_in_a_different_order_is_not_a_difference()
    {
        await ResetSchema();

        // The database has the key as (c, a). A hand-written table derives its key list from column
        // order and has no way to say that, so it will offer (a, c).
        await theConnection.CreateCommand(
                "create table upgrade.pkorder (a int not null, b int not null, c int not null, constraint pk_pkorder primary key (c, a))")
            .ExecuteNonQueryAsync();

        var expected = new Table("upgrade.pkorder");
        expected.AddColumn<int>("a").AsPrimaryKey();
        expected.AddColumn<int>("b").NotNull();
        expected.AddColumn<int>("c").AsPrimaryKey();
        expected.PrimaryKeyName = "pk_pkorder";

        var delta = await expected.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task pinning_the_order_explicitly_does_compare_it()
    {
        await ResetSchema();
        await theConnection.CreateCommand(
                "create table upgrade.pkpinned (a int not null, b int not null, c int not null, constraint pk_pkpinned primary key (c, a))")
            .ExecuteNonQueryAsync();

        var expected = new Table("upgrade.pkpinned");
        expected.AddColumn<int>("a").AsPrimaryKey();
        expected.AddColumn<int>("b").NotNull();
        expected.AddColumn<int>("c").AsPrimaryKey();
        expected.PrimaryKeyName = "pk_pkpinned";

        // Opting in: now the order is part of the contract, and this one is wrong.
        expected.SetPrimaryKeyOrder(["a", "c"]);

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        // ...and stating the true order is clean again.
        expected.SetPrimaryKeyOrder(["c", "a"]);
        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_composite_foreign_key_declared_in_another_order_is_not_a_difference()
    {
        await ResetSchema();
        await theConnection.CreateCommand(
                "create table upgrade.fkparent (x int not null, y int not null, constraint pk_fkparent primary key (x, y))")
            .ExecuteNonQueryAsync();
        // The shape older Weasel would have written, having sorted both sides.
        await theConnection.CreateCommand(
                "create table upgrade.fkchild (a int not null, b int not null, constraint pk_fkchild primary key (a, b), constraint fk_ch foreign key (a, b) references upgrade.fkparent (x, y))")
            .ExecuteNonQueryAsync();

        var expected = new Table("upgrade.fkchild");
        expected.AddColumn<int>("a").AsPrimaryKey();
        expected.AddColumn<int>("b").AsPrimaryKey();
        expected.PrimaryKeyName = "pk_fkchild";
        // Same pairs, stated in the other order: b->y and a->x. Same constraint.
        expected.ForeignKeys.Add(new ForeignKey("fk_ch")
        {
            LinkedTable = new SqlServerObjectName("upgrade", "fkparent"),
            ColumnNames = ["b", "a"],
            LinkedNames = ["y", "x"]
        });

        var delta = await expected.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_foreign_key_pairing_different_columns_is_still_a_difference()
    {
        await ResetSchema();
        await theConnection.CreateCommand(
                "create table upgrade.p2 (x int not null, y int not null, constraint pk_p2 primary key (x, y))")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand(
                "create table upgrade.c2 (a int not null, b int not null, constraint pk_c2 primary key (a, b), constraint fk_c2 foreign key (a, b) references upgrade.p2 (x, y))")
            .ExecuteNonQueryAsync();

        var expected = new Table("upgrade.c2");
        expected.AddColumn<int>("a").AsPrimaryKey();
        expected.AddColumn<int>("b").AsPrimaryKey();
        expected.PrimaryKeyName = "pk_c2";
        // a->y and b->x is genuinely a different constraint, and must not be masked by
        // order-insensitivity.
        expected.ForeignKeys.Add(new ForeignKey("fk_c2")
        {
            LinkedTable = new SqlServerObjectName("upgrade", "p2"),
            ColumnNames = ["a", "b"],
            LinkedNames = ["y", "x"]
        });

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task an_all_descending_index_is_not_a_difference()
    {
        await ResetSchema();
        await theConnection.CreateCommand(
                "create table upgrade.ad (id int not null constraint pk_ad primary key, a int not null, b int not null)")
            .ExecuteNonQueryAsync();
        // Created outside Weasel, every key column descending.
        await theConnection.CreateCommand("create index ix_ad on upgrade.ad (a desc, b desc)")
            .ExecuteNonQueryAsync();

        var expected = new Table("upgrade.ad");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<int>("a").NotNull();
        expected.AddColumn<int>("b").NotNull();
        expected.PrimaryKeyName = "pk_ad";
        // All a hand-written model can say. Comparing the database's per-column truth against it
        // would drop the index and recreate it as (a, b DESC) -- silently flipping a to ASC.
        expected.Indexes.Add(new IndexDefinition("ix_ad") { Columns = ["a", "b"], SortOrder = SortOrder.Desc });

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_ordinary_table_still_reports_no_delta_against_itself()
    {
        await ResetSchema();

        var table = new Table("upgrade.ordinary");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<decimal>("amount").AllowNulls();
        table.Indexes.Add(new IndexDefinition("ix_ordinary_name") { Columns = ["name"] });
        table.CheckConstraints.Add(new TableCheckConstraint("ck_ordinary", "amount is null or amount > 0"));

        await CreateSchemaObjectInDatabase(table);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
