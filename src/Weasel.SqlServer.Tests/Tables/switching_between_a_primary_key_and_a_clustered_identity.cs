using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     A table that moves between a clustered primary key on its id and a clustered identity column with
///     a unique index on the id, in either direction, on a table that already holds rows.
/// </summary>
/// <remarks>
///     <para>
///         Reported from Wolverine (JasperFx/wolverine#4989), whose SQL Server queue tables switch between
///         exactly these two layouts with <c>OptimizeQueueThroughput()</c>. Neither direction could be
///         migrated:
///     </para>
///     <list type="bullet">
///         <item>
///             Adding the identity: <c>column 'seq' cannot be added to an existing table</c>. SQL Server
///             adds an identity column to a populated table and numbers the existing rows, so it is
///             always addable.
///         </item>
///         <item>
///             Removing the primary key, or adding one to a table that had none:
///             <c>'pkey_orders_' is not a constraint</c>. A table without primary key columns still
///             reported the default primary key name, so the delta tried to drop a constraint that did
///             not exist.
///         </item>
///     </list>
/// </remarks>
public class switching_between_a_primary_key_and_a_clustered_identity: IntegrationContext
{
    private const string Schema = "pk_identity_switch";

    public switching_between_a_primary_key_and_a_clustered_identity(): base(Schema)
    {
    }

    private static Table keyedById()
    {
        var table = new Table(new SqlServerObjectName(Schema, "orders"));
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn("body", "varbinary(max)").NotNull();
        table.AddColumn<DateTimeOffset>("timestamp").DefaultValueByExpression("SYSDATETIMEOFFSET()");

        table.Indexes.Add(new IndexDefinition("idx_orders_timestamp") { Columns = ["timestamp"] });
        return table;
    }

    private static Table clusteredOnIdentity()
    {
        var table = new Table(new SqlServerObjectName(Schema, "orders"));
        table.AddColumn<Guid>("id").NotNull();
        table.AddColumn("body", "varbinary(max)").NotNull();
        table.AddColumn<DateTimeOffset>("timestamp").DefaultValueByExpression("SYSDATETIMEOFFSET()");
        table.AddColumn<long>("seq").AutoNumber().NotNull();

        table.Indexes.Add(new IndexDefinition("cidx_orders_seq") { Columns = ["seq"], IsClustered = true });
        table.Indexes.Add(new IndexDefinition("uidx_orders_id") { Columns = ["id"], IsUnique = true });
        return table;
    }

    private async Task createWithRowsAsync(Table table)
    {
        await ResetSchema();
        await new SqlServerMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, table), AutoCreate.CreateOrUpdate);

        await theConnection.CreateCommand(
                $"insert into {Schema}.orders (id, body) values (newid(), 0x01), (newid(), 0x02), (newid(), 0x03)")
            .ExecuteNonQueryAsync();
    }

    private async Task migrateAsync(Table table)
    {
        var migration = await SchemaMigration.DetermineAsync(theConnection, table);
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        (await SchemaMigration.DetermineAsync(theConnection, table)).Difference
            .ShouldBe(SchemaPatchDifference.None);
    }

    private async Task<long> scalarAsync(string sql)
    {
        return Convert.ToInt64(await theConnection.CreateCommand(sql).ExecuteScalarAsync());
    }

    private Task<long> primaryKeyCountAsync() => scalarAsync(
        $"select count(*) from sys.key_constraints where type = 'PK' and parent_object_id = object_id('{Schema}.orders')");

    private Task<long> clusteredIndexCountAsync() => scalarAsync(
        $"select count(*) from sys.indexes where type = 1 and object_id = object_id('{Schema}.orders')");

    [Fact]
    public async Task a_populated_table_moves_from_a_primary_key_to_a_clustered_identity()
    {
        await theConnection.OpenAsync();
        await createWithRowsAsync(keyedById());

        await migrateAsync(clusteredOnIdentity());

        (await scalarAsync($"select count(*) from {Schema}.orders")).ShouldBe(3);
        (await scalarAsync($"select count(distinct seq) from {Schema}.orders")).ShouldBe(3);
        (await primaryKeyCountAsync()).ShouldBe(0);
        (await clusteredIndexCountAsync()).ShouldBe(1);
    }

    [Fact]
    public async Task a_populated_table_moves_from_a_clustered_identity_to_a_primary_key()
    {
        await theConnection.OpenAsync();
        await createWithRowsAsync(clusteredOnIdentity());

        await migrateAsync(keyedById());

        (await scalarAsync($"select count(*) from {Schema}.orders")).ShouldBe(3);
        (await primaryKeyCountAsync()).ShouldBe(1);
        (await clusteredIndexCountAsync()).ShouldBe(1);
        (await scalarAsync(
                $"select count(*) from sys.columns where object_id = object_id('{Schema}.orders') and name = 'seq'"))
            .ShouldBe(0);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_switch_rolls_back(bool fromPrimaryKey)
    {
        await theConnection.OpenAsync();
        var initial = fromPrimaryKey ? keyedById() : clusteredOnIdentity();
        var configured = fromPrimaryKey ? clusteredOnIdentity() : keyedById();
        await createWithRowsAsync(initial);

        var migration = new SchemaMigration(new ISchemaObjectDelta[] { await configured.FindDeltaAsync(theConnection) });
        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        await migration.RollbackAllAsync(theConnection, new SqlServerMigrator());

        (await initial.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await scalarAsync($"select count(*) from {Schema}.orders")).ShouldBe(3);
    }

    [Fact]
    public void a_table_without_primary_key_columns_has_no_primary_key_name()
    {
        clusteredOnIdentity().PrimaryKeyName.ShouldBeEmpty();
        keyedById().PrimaryKeyName.ShouldBe("pkey_orders_id");
    }

    [Fact]
    public void an_identity_column_can_be_added_to_an_existing_table()
    {
        clusteredOnIdentity().ColumnFor("seq")!.CanAdd().ShouldBeTrue();
    }
}
