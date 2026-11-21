using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     weasel#905: SQL Server caches the plan for the table introspection batch per database, so every
///     new database (each tenant database, each test database) compiles it again. Compiling the catalog
///     reads cost about a second of CPU there, most of it before the first table even existed. These run
///     against a database of their own, so the plan cache they look at holds nothing but what they did.
/// </summary>
public class introspection_in_a_new_database: IAsyncLifetime
{
    private readonly string _databaseName = $"weasel_905_{Guid.NewGuid():N}";

    private string connectionString(string catalog) =>
        new SqlConnectionStringBuilder(ConnectionSource.ConnectionString) { InitialCatalog = catalog, Pooling = false }
            .ConnectionString;

    public async ValueTask InitializeAsync()
    {
        await using var master = new SqlConnection(connectionString("master"));
        await master.OpenAsync();
        await master.CreateCommand($"create database [{_databaseName}]").ExecuteNonQueryAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await using var master = new SqlConnection(connectionString("master"));
        await master.OpenAsync();
        await master.CreateCommand(
                $"alter database [{_databaseName}] set single_user with rollback immediate; drop database [{_databaseName}]")
            .ExecuteNonQueryAsync();
    }

    private static Table[] tables(int count) => Enumerable.Range(0, count).Select(i =>
    {
        var table = new Table($"dbo.thing{i}");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").AllowNulls();
        table.AddColumn<DateTimeOffset>("last_modified").DefaultValueByExpression("sysdatetimeoffset()");
        table.Indexes.Add(new IndexDefinition($"ix_thing{i}_last_modified") { Columns = ["last_modified"] });
        return table;
    }).ToArray();

    /// <summary>
    ///     The cached plans in this database that read the foreign key catalog: the catalog reads, but not
    ///     the small wrapper that resolves the table's name to its id, whose text carries theirs as a
    ///     literal.
    /// </summary>
    private static async Task<List<int>> catalogReadPlanUseCounts(SqlConnection conn)
    {
        await using var cmd = conn.CreateCommand("""
            select cp.usecounts
            from sys.dm_exec_cached_plans cp
                cross apply sys.dm_exec_sql_text(cp.plan_handle) st
            where st.dbid = db_id()
                and st.text like N'%sys.foreign_key_columns%'
                and st.text not like N'%quotename(@schema)%'
            """);

        var counts = new List<int>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) counts.Add(reader.GetInt32(0));
        return counts;
    }

    [Fact]
    public async Task a_database_without_the_tables_never_compiles_the_catalog_reads()
    {
        await using var conn = new SqlConnection(connectionString(_databaseName));
        await conn.OpenAsync();

        var migration = await SchemaMigration.DetermineAsync(conn, new SqlServerMigrator(), default, tables(5));

        migration.Deltas.Count.ShouldBe(5);
        migration.Deltas.ShouldAllBe(x => x.Difference == SchemaPatchDifference.Create);

        (await catalogReadPlanUseCounts(conn)).ShouldBeEmpty();
    }

    [Fact]
    public async Task the_catalog_reads_compile_once_for_every_table_in_the_database()
    {
        await using var conn = new SqlConnection(connectionString(_databaseName));
        await conn.OpenAsync();

        foreach (var table in tables(5))
        {
            await table.CreateAsync(conn);
        }

        var migration = await SchemaMigration.DetermineAsync(conn, new SqlServerMigrator(), default, tables(5));

        migration.Deltas.ShouldAllBe(x => x.Difference == SchemaPatchDifference.None);

        // One plan in this database, used by all five tables.
        (await catalogReadPlanUseCounts(conn)).ShouldBe([5]);
    }

    [Fact]
    public async Task a_view_with_the_table_name_is_not_read_as_the_table()
    {
        await using var conn = new SqlConnection(connectionString(_databaseName));
        await conn.OpenAsync();

        await conn.CreateCommand("create view dbo.thing0 as select 1 as id, cast('a' as nvarchar(100)) as name")
            .ExecuteNonQueryAsync();

        var table = tables(1)[0];

        (await table.FetchExistingAsync(conn)).ShouldBeNull();
    }

    [Fact]
    public async Task a_default_bound_with_sp_bindefault_is_not_read_as_the_column_default()
    {
        // Only a default CONSTRAINT is a column default as far as Weasel is concerned. A legacy default
        // object bound to the column was never read as one, and still is not.
        await using var conn = new SqlConnection(connectionString(_databaseName));
        await conn.OpenAsync();

        var table = new Table("dbo.bound");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("amount").AllowNulls();
        table.AddColumn<int>("constrained").DefaultValueByExpression("42");
        await table.CreateAsync(conn);

        await conn.CreateCommand("create default dbo.weasel_905_zero as 0").ExecuteNonQueryAsync();
        await conn.CreateCommand("exec sp_bindefault 'dbo.weasel_905_zero', 'dbo.bound.amount'").ExecuteNonQueryAsync();

        var existing = (await table.FetchExistingAsync(conn)).ShouldNotBeNull();

        existing.ColumnFor("amount")!.DefaultExpression.ShouldBeNull();
        existing.ColumnFor("constrained")!.DefaultExpression.ShouldBe("((42))");
    }
}
