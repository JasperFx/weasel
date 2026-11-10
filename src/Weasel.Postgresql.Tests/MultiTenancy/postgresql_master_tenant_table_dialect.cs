using JasperFx;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Core.MultiTenancy;
using Weasel.Postgresql.MultiTenancy;
using Xunit;

namespace Weasel.Postgresql.Tests.MultiTenancy;

/// <summary>
/// weasel#742: the PostgreSQL <see cref="IMasterTenantTableDialect" />, run through
/// <see cref="MasterTableTenancyBase{TDatabase,TDataSource}" /> against a real server, and against
/// the table Marten already provisions through its migration pipeline.
/// </summary>
[Collection("tenant_table_dialect")]
public class postgresql_master_tenant_table_dialect: IntegrationContext
{
    private readonly List<TestTenancy> _tenancies = [];

    public postgresql_master_tenant_table_dialect(): base("tenant_table_dialect")
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await ResetSchema();
        await theConnection.DropSchemaAsync(SchemaName);
    }

    public override async ValueTask DisposeAsync()
    {
        foreach (var tenancy in _tenancies) await tenancy.DisposeAsync();
        await base.DisposeAsync();
    }

    private TestTenancy TenancyFor()
    {
        var tenancy = new TestTenancy(new MasterTableTenancyOptions<NpgsqlDataSource>(PostgresqlProvider.Instance)
        {
            ConnectionString = ConnectionSource.ConnectionString, SchemaName = SchemaName
        });

        _tenancies.Add(tenancy);
        return tenancy;
    }

    private async Task<List<(string TenantId, string ConnectionString, bool Disabled)>> RowsAsync()
    {
        var rows = new List<(string, string, bool)>();
        await using var cmd = theConnection.CreateCommand(
            $"select tenant_id, connection_string, disabled from {SchemaName}.mt_tenant_databases order by tenant_id");
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetBoolean(2)));
        }

        return rows;
    }

    [Fact]
    public async Task the_whole_tenant_lifecycle_runs_against_a_provisioned_table()
    {
        var tenancy = TenancyFor();

        await tenancy.AddDatabaseRecordAsync("red", "Host=red");
        await tenancy.AddDatabaseRecordAsync("blue", "Host=blue");

        (await tenancy.BuildDatabasesAsync()).Select(x => x.TenantId).OrderBy(x => x)
            .ShouldBe(["blue", "red"]);
        // The base applies the application name to what it hands back.
        (await tenancy.LookupConnectionStringAsync("red")).ShouldNotBeNull().ShouldStartWith("Host=red");

        await tenancy.DisableTenantAsync("red");
        (await tenancy.AllDisabledAsync()).ShouldBe(["red"]);
        (await tenancy.LookupConnectionStringAsync("red")).ShouldBeNull();
        (await tenancy.TryFindDatabaseAsync("red")).ShouldBeNull();

        await tenancy.EnableTenantAsync("red");
        (await tenancy.AllDisabledAsync()).ShouldBeEmpty();
        (await tenancy.TryFindDatabaseAsync("red")).ShouldNotBeNull().ConnectionString.ShouldStartWith("Host=red");

        await tenancy.DeleteDatabaseRecordAsync("blue");
        (await RowsAsync()).Select(x => x.TenantId).ShouldBe(["red"]);

        await tenancy.ClearAllDatabaseRecordsAsync();
        (await RowsAsync()).ShouldBeEmpty();
    }

    [Fact]
    public async Task re_adding_a_disabled_tenant_updates_its_connection_but_leaves_it_disabled()
    {
        // Marten's upsert, kept: on conflict it sets the connection string and nothing else.
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("red", "Host=old");
        await tenancy.DisableTenantAsync("red");

        await tenancy.AddDatabaseRecordAsync("red", "Host=new");

        (await RowsAsync()).ShouldBe([("red", "Host=new", true)]);
    }

    [Fact]
    public async Task the_rendered_table_is_the_one_weasels_migration_pipeline_would_build()
    {
        // Provision through the dialect, then diff the table definition a store migrating through
        // Weasel would register. Any drift (column type, default, constraint name) is a permanent
        // delta on every deploy.
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("red", "Host=red");

        var delta = await PostgresqlMasterTenantTableDialect
            .BuildControlTable(SchemaName, PostgresqlMasterTenantTableDialect.DefaultTableName)
            .FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_table_marten_already_migrated_is_read_and_written_as_it_stands()
    {
        // The deployed case: the table exists with an operator-entered row before this dialect
        // ever runs. Provisioning must be a no-op and the row must resolve.
        await CreateSchemaObjectInDatabase(
            PostgresqlMasterTenantTableDialect.BuildControlTable(SchemaName,
                PostgresqlMasterTenantTableDialect.DefaultTableName));

        await using (var insert = theConnection.CreateCommand(
                         $"insert into {SchemaName}.mt_tenant_databases (tenant_id, connection_string) values ('green', 'Host=green')"))
        {
            await insert.ExecuteNonQueryAsync();
        }

        var tenancy = TenancyFor();

        (await tenancy.BuildDatabasesAsync()).Single().TenantId.ShouldBe("green");
        await tenancy.AddDatabaseRecordAsync("red", "Host=red");
        (await RowsAsync()).ShouldBe([("green", "Host=green", false), ("red", "Host=red", false)]);
    }

    [Fact]
    public void the_qualified_name_quotes_what_needs_quoting()
    {
        var dialect = PostgresqlMasterTenantTableDialect.Instance;

        dialect.QualifiedTableName("tenants", "mt_tenant_databases").ShouldBe("tenants.mt_tenant_databases");
        dialect.QualifiedTableName("Tenants", "mt_tenant_databases").ShouldBe("\"Tenants\".mt_tenant_databases");
    }

    public sealed record TestDatabase(string TenantId, string ConnectionString);

    private sealed class TestTenancy(MasterTableTenancyOptions<NpgsqlDataSource> options)
        : MasterTableTenancyBase<TestDatabase, NpgsqlDataSource>(options,
            PostgresqlMasterTenantTableDialect.DefaultTableName, PostgresqlMasterTenantTableDialect.Instance)
    {
        protected override NpgsqlDataSource BuildDataSource(string connectionString) =>
            NpgsqlDataSource.Create(connectionString);

        protected override TestDatabase BuildDatabase(string tenantId, string connectionString) =>
            new(tenantId, connectionString);
    }
}
