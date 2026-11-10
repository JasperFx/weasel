using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

public class primary_key_clustering: IntegrationContext
{
    public primary_key_clustering(): base("pk_clustering")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private async Task createNonclusteredKeyBesideClusteredIndex()
    {
        await theConnection.CreateCommand(
                "create table pk_clustering.orders (id int not null, code varchar(10) not null, constraint pk_orders primary key nonclustered (id))")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand("create clustered index ix_orders_code on pk_clustering.orders (code)")
            .ExecuteNonQueryAsync();
    }

    private async Task createClusteredKey()
        => await theConnection.CreateCommand(
                "create table pk_clustering.orders (id int not null, code varchar(10) not null, constraint pk_orders primary key clustered (id))")
            .ExecuteNonQueryAsync();

    private static Table model(bool? clustered = null, bool clusteredIndexOnCode = false)
    {
        var table = new Table("pk_clustering.orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("code", "varchar(10)").NotNull();
        table.PrimaryKeyName = "pk_orders";
        if (clustered.HasValue) table.PrimaryKeyIsClustered = clustered;
        if (clusteredIndexOnCode) table.Indexes.Add(new IndexDefinition("ix_orders_code") { Columns = ["code"], IsClustered = true });
        return table;
    }

    private async Task<string?> primaryKeyIndexType()
        => (string?)await theConnection.CreateCommand(
                "select i.type_desc from sys.indexes i where i.object_id = object_id('pk_clustering.orders') and i.is_primary_key = 1")
            .ExecuteScalarAsync();

    private async Task apply(Table table)
    {
        var migration = new SchemaMigration(await table.FindDeltaAsync(theConnection));
        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);
    }

    [Fact]
    public async Task a_table_read_back_recreates_its_nonclustered_key()
    {
        await createNonclusteredKeyBesideClusteredIndex();
        var existing = (await new Table("pk_clustering.orders").FetchExistingAsync(theConnection))!;

        await theConnection.CreateCommand("drop table pk_clustering.orders").ExecuteNonQueryAsync();
        var writer = new StringWriter();
        existing.WriteCreateStatement(new SqlServerMigrator(), writer);
        await theConnection.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();

        (await primaryKeyIndexType()).ShouldBe("NONCLUSTERED");
    }

    [Fact]
    public async Task the_clustering_of_the_key_is_read_back()
    {
        await createNonclusteredKeyBesideClusteredIndex();
        (await new Table("pk_clustering.orders").FetchExistingAsync(theConnection))!
            .PrimaryKeyIsClustered.ShouldBe(false);

        await ResetSchema();
        await createClusteredKey();
        (await new Table("pk_clustering.orders").FetchExistingAsync(theConnection))!
            .PrimaryKeyIsClustered.ShouldBe(true);
    }

    [Fact]
    public async Task a_table_without_a_key_reads_back_unpinned()
    {
        await theConnection.CreateCommand("create table pk_clustering.heap (id int not null)").ExecuteNonQueryAsync();

        (await new Table("pk_clustering.heap").FetchExistingAsync(theConnection))!
            .PrimaryKeyIsClustered.ShouldBeNull();
    }

    [Fact]
    public void an_unpinned_key_renders_as_before()
    {
        model().PrimaryKeyDeclaration().ShouldBe("CONSTRAINT pk_orders PRIMARY KEY (id)");
    }

    [Fact]
    public void a_pinned_key_renders_its_clustering()
    {
        model(false).PrimaryKeyDeclaration().ShouldBe("CONSTRAINT pk_orders PRIMARY KEY NONCLUSTERED (id)");
        model(true).PrimaryKeyDeclaration().ShouldBe("CONSTRAINT pk_orders PRIMARY KEY CLUSTERED (id)");
    }

    [Fact]
    public async Task an_unpinned_model_is_unchanged_against_a_nonclustered_key()
    {
        await createNonclusteredKeyBesideClusteredIndex();

        (await model(clusteredIndexOnCode: true).FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_unpinned_model_weasel_created_is_unchanged()
    {
        await apply(model());

        (await model().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_pinned_key_matching_the_database_is_unchanged()
    {
        await createNonclusteredKeyBesideClusteredIndex();
        (await model(false, clusteredIndexOnCode: true).FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        await ResetSchema();
        await createClusteredKey();
        (await model(true).FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task pinning_nonclustered_against_a_clustered_key_is_drift_that_converges()
    {
        await createClusteredKey();

        var delta = await model(false).FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);

        await apply(model(false));

        (await primaryKeyIndexType()).ShouldBe("NONCLUSTERED");
        (await model(false).FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task pinning_clustered_against_a_nonclustered_key_is_drift_that_converges()
    {
        await theConnection.CreateCommand(
                "create table pk_clustering.orders (id int not null, code varchar(10) not null, constraint pk_orders primary key nonclustered (id))")
            .ExecuteNonQueryAsync();

        (await model(true).FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await apply(model(true));

        (await primaryKeyIndexType()).ShouldBe("CLUSTERED");
        (await model(true).FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task rolling_back_restores_a_nonclustered_key()
    {
        await createNonclusteredKeyBesideClusteredIndex();

        var delta = await model(true, clusteredIndexOnCode: true).FindDeltaAsync(theConnection);
        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);
        var writer = new StringWriter();
        delta.WriteRollback(new SqlServerMigrator(), writer);

        writer.ToString().ShouldContain("add CONSTRAINT pk_orders PRIMARY KEY NONCLUSTERED (id)");
    }

    [Fact]
    public async Task rolling_back_a_changed_key_restores_it_nonclustered()
    {
        await createNonclusteredKeyBesideClusteredIndex();

        var widened = new Table("pk_clustering.orders");
        widened.AddColumn<int>("id").AsPrimaryKey();
        widened.AddColumn("code", "varchar(10)").NotNull().AsPrimaryKey();
        widened.PrimaryKeyName = "pk_orders";
        widened.Indexes.Add(new IndexDefinition("ix_orders_code") { Columns = ["code"], IsClustered = true });

        var delta = await widened.FindDeltaAsync(theConnection);
        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);
        var writer = new StringWriter();
        delta.WriteRollback(new SqlServerMigrator(), writer);

        writer.ToString().ShouldContain("add CONSTRAINT pk_orders PRIMARY KEY NONCLUSTERED (id)");
    }
}
