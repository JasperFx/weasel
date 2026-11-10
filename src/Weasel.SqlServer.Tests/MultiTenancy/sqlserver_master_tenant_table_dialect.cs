using System.Data.Common;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core.MultiTenancy;
using Weasel.SqlServer.MultiTenancy;
using Xunit;

namespace Weasel.SqlServer.Tests.MultiTenancy;

/// <summary>
/// weasel#742: the SQL Server <see cref="IMasterTenantTableDialect" />, run through
/// <see cref="MasterTableTenancyBase{TDatabase,TDataSource}" /> against a real server, and against
/// the table Polecat already provisions with its own DDL.
/// </summary>
[Collection("integration")]
public class sqlserver_master_tenant_table_dialect: IntegrationContext
{
    private const string Schema = "tenant_table_dialect";

    // A schema name that breaks anything concatenating it unescaped, in either a bracketed
    // identifier or an N'...' literal.
    private const string HostileSchema = "tenant_dialect_o'brien]x";

    private readonly List<TestTenancy> _tenancies = [];

    public sqlserver_master_tenant_table_dialect(): base(Schema)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await theConnection.OpenAsync();
        await DropAsync(Schema);
        await DropAsync(HostileSchema);
    }

    public override async ValueTask DisposeAsync()
    {
        foreach (var tenancy in _tenancies) await tenancy.DisposeAsync();
        await DropAsync(HostileSchema);
        await base.DisposeAsync();
    }

    private async Task DropAsync(string schema)
    {
        var qualified = SqlServerMasterTenantTableDialect.Instance.QualifiedTableName(schema,
            SqlServerMasterTenantTableDialect.DefaultTableName);

        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = $"""
            IF OBJECT_ID(@qualified, 'U') IS NOT NULL DROP TABLE {qualified};
            IF SCHEMA_ID(@schema) IS NOT NULL
            BEGIN
                DECLARE @drop NVARCHAR(MAX) = N'DROP SCHEMA ' + QUOTENAME(@schema);
                EXEC sp_executesql @drop;
            END
            """;
        cmd.Parameters.AddWithValue("@qualified", qualified);
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }

    private TestTenancy TenancyFor(string schema = Schema)
    {
        var tenancy = new TestTenancy(new MasterTableTenancyOptions<DbDataSource>(SqlServerProvider.Instance)
        {
            ConnectionString = ConnectionSource.ConnectionString, SchemaName = schema
        });

        _tenancies.Add(tenancy);
        return tenancy;
    }

    private async Task<List<(string TenantId, string ConnectionString, bool Disabled)>> RowsAsync(string schema = Schema)
    {
        var rows = new List<(string, string, bool)>();
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText =
            $"SELECT tenant_id, connection_string, is_disabled FROM {SqlServerMasterTenantTableDialect.Instance.QualifiedTableName(schema, "pc_tenants")} ORDER BY tenant_id";

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

        await tenancy.AddDatabaseRecordAsync("red", "Server=red");
        await tenancy.AddDatabaseRecordAsync("blue", "Server=blue");

        (await tenancy.BuildDatabasesAsync()).Select(x => x.TenantId).OrderBy(x => x)
            .ShouldBe(["blue", "red"]);
        (await tenancy.LookupConnectionStringAsync("red")).ShouldNotBeNull().ShouldContain("red");

        await tenancy.DisableTenantAsync("red");
        (await tenancy.AllDisabledAsync()).ShouldBe(["red"]);
        (await tenancy.LookupConnectionStringAsync("red")).ShouldBeNull();
        (await tenancy.TryFindDatabaseAsync("red")).ShouldBeNull();

        await tenancy.EnableTenantAsync("red");
        (await tenancy.AllDisabledAsync()).ShouldBeEmpty();
        (await tenancy.TryFindDatabaseAsync("red")).ShouldNotBeNull();

        await tenancy.DeleteDatabaseRecordAsync("blue");
        (await RowsAsync()).Select(x => x.TenantId).ShouldBe(["red"]);

        await tenancy.ClearAllDatabaseRecordsAsync();
        (await RowsAsync()).ShouldBeEmpty();
    }

    [Fact]
    public async Task re_adding_a_disabled_tenant_re_enables_it()
    {
        // Polecat's MERGE, kept: a match clears the disabled flag.
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("red", "Server=old");
        await tenancy.DisableTenantAsync("red");

        await tenancy.AddDatabaseRecordAsync("red", "Server=new");

        (await RowsAsync()).ShouldBe([("red", "Server=new", false)]);
    }

    [Fact]
    public async Task a_table_polecat_already_created_is_read_and_written_as_it_stands()
    {
        // Polecat's own provisioning statement, verbatim, run the way Polecat runs it.
        var qualified = SqlServerMasterTenantTableDialect.Instance.QualifiedTableName(Schema, "pc_tenants");
        await using (var cmd = theConnection.CreateCommand())
        {
            cmd.CommandText = $"""
                IF SCHEMA_ID(@schema) IS NULL
                BEGIN
                    DECLARE @createSchema NVARCHAR(MAX) = N'CREATE SCHEMA ' + QUOTENAME(@schema);
                    EXEC sp_executesql @createSchema;
                END
                IF OBJECT_ID(@qualified, 'U') IS NULL
                    CREATE TABLE {qualified} (
                        tenant_id NVARCHAR(200) NOT NULL CONSTRAINT pk_pc_tenants PRIMARY KEY,
                        connection_string NVARCHAR(MAX) NOT NULL,
                        is_disabled BIT NOT NULL CONSTRAINT df_pc_tenants_is_disabled DEFAULT 0
                    );
                INSERT INTO {qualified} (tenant_id, connection_string) VALUES ('green', 'Server=green');
                """;
            cmd.Parameters.AddWithValue("@schema", Schema);
            cmd.Parameters.AddWithValue("@qualified", qualified);
            await cmd.ExecuteNonQueryAsync();
        }

        var tenancy = TenancyFor();

        (await tenancy.BuildDatabasesAsync()).Single().TenantId.ShouldBe("green");
        await tenancy.AddDatabaseRecordAsync("red", "Server=red");
        (await RowsAsync()).ShouldBe([("green", "Server=green", false), ("red", "Server=red", false)]);
    }

    [Fact]
    public async Task a_schema_name_with_a_quote_and_a_bracket_is_escaped_not_trusted()
    {
        // The base runs the DDL unparameterized, so the schema reaches it as an N'...' literal
        // and as a bracketed identifier. Either one unescaped is a syntax error or worse.
        var tenancy = TenancyFor(HostileSchema);

        await tenancy.AddDatabaseRecordAsync("red", "Server=red");

        (await RowsAsync(HostileSchema)).ShouldBe([("red", "Server=red", false)]);
    }

    [Fact]
    public void the_qualified_name_is_always_bracketed()
    {
        SqlServerMasterTenantTableDialect.Instance.QualifiedTableName("dbo", "pc_tenants")
            .ShouldBe("[dbo].[pc_tenants]");
        SqlServerMasterTenantTableDialect.Instance.QualifiedTableName("a]b", "pc_tenants")
            .ShouldBe("[a]]b].[pc_tenants]");
    }

    public sealed record TestDatabase(string TenantId, string ConnectionString);

    private sealed class TestTenancy(MasterTableTenancyOptions<DbDataSource> options)
        : MasterTableTenancyBase<TestDatabase, DbDataSource>(options,
            SqlServerMasterTenantTableDialect.DefaultTableName, SqlServerMasterTenantTableDialect.Instance)
    {
        protected override DbDataSource BuildDataSource(string connectionString) =>
            SqlClientFactory.Instance.CreateDataSource(connectionString);

        protected override TestDatabase BuildDatabase(string tenantId, string connectionString) =>
            new(tenantId, connectionString);
    }
}
