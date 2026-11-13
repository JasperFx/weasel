using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx;
using JasperFx.MultiTenancy;
using Microsoft.Data.Sqlite;
using NSubstitute;
using Shouldly;
using Weasel.Core.MultiTenancy;
using Weasel.Sqlite;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
/// The lifted <see cref="MasterTableTenancyBase{TDatabase,TDataSource}" /> (weasel#567), exercised
/// against a real control table so the provisioning, the cache and the runtime tenant lifecycle are
/// checked against a database rather than against a mock that agrees with them.
/// </summary>
/// <remarks>
/// SQLite because it needs no server. The base only ever touches <see cref="System.Data.Common.DbDataSource" />
/// and <see cref="System.Data.Common.DbConnection" />, and the dialect here renders SQL the way
/// Marten's and Polecat's would.
/// </remarks>
public class master_table_tenancy: IAsyncDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"weasel_tenancy_{Guid.NewGuid():n}.db");
    private readonly List<TestTenancy> _tenancies = [];
    private readonly string _connectionString;

    public master_table_tenancy() => _connectionString = $"Data Source={_path}";

    public async ValueTask DisposeAsync()
    {
        foreach (var tenancy in _tenancies) await tenancy.DisposeAsync();

        SqliteConnection.ClearPool(new SqliteConnection(_connectionString));
        if (File.Exists(_path)) File.Delete(_path);
        GC.SuppressFinalize(this);
    }

    private TestTenancy TenancyFor(Action<MasterTableTenancyOptions<SqliteDataSource>>? configure = null)
    {
        var options = new MasterTableTenancyOptions<SqliteDataSource>(SqliteProvider.Instance)
        {
            ConnectionString = _connectionString, SchemaName = "main"
        };

        configure?.Invoke(options);

        var tenancy = new TestTenancy(options);
        _tenancies.Add(tenancy);
        return tenancy;
    }

    private async Task<List<(string TenantId, string ConnectionString, bool Disabled)>> RowsAsync()
    {
        var rows = new List<(string, string, bool)>();

        await using var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select tenant_id, connection_string, is_disabled from tenants order by tenant_id";

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetInt64(2) != 0));
        }

        return rows;
    }

    private async Task<bool> ControlTableExistsAsync()
    {
        await using var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select count(*) from sqlite_master where type = 'table' and name = 'tenants'";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync()) > 0;
    }

    // ------------------------------------------------------------------------------------------
    // Configuration
    // ------------------------------------------------------------------------------------------

    [Fact]
    public void neither_a_data_source_nor_a_connection_string_is_refused()
    {
        var options = new MasterTableTenancyOptions<SqliteDataSource>(SqliteProvider.Instance);

        Should.Throw<ArgumentOutOfRangeException>(() => new TestTenancy(options));
    }

    [Fact]
    public async Task a_supplied_data_source_is_not_disposed_by_the_tenancy()
    {
        // Somebody else built it, so somebody else disposes it. Getting this wrong surfaces as an
        // ObjectDisposedException somewhere unrelated.
        var dataSource = new SqliteDataSource(_connectionString);

        var tenancy = new TestTenancy(new MasterTableTenancyOptions<SqliteDataSource>(SqliteProvider.Instance)
        {
            DataSource = dataSource, SchemaName = "main"
        });

        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.DisposeAsync();

        // Still usable.
        await using var connection = await dataSource.OpenConnectionAsync();
        connection.State.ShouldBe(System.Data.ConnectionState.Open);
        await dataSource.DisposeAsync();
    }

    // ------------------------------------------------------------------------------------------
    // Control table provisioning
    // ------------------------------------------------------------------------------------------

    [Fact]
    public async Task the_control_table_is_created_on_first_use()
    {
        (await ControlTableExistsAsync()).ShouldBeFalse();

        await TenancyFor().BuildDatabasesAsync();

        (await ControlTableExistsAsync()).ShouldBeTrue();
    }

    [Fact]
    public async Task provisioning_is_idempotent_across_instances()
    {
        // The semaphore guards one process; a deployment has several, which is why the DDL itself
        // has to be idempotent rather than guarded by a prior existence check.
        await TenancyFor().BuildDatabasesAsync();
        await TenancyFor().BuildDatabasesAsync();

        (await ControlTableExistsAsync()).ShouldBeTrue();
    }

    [Fact]
    public async Task provisioning_runs_once_per_instance()
    {
        var tenancy = TenancyFor();

        await tenancy.BuildDatabasesAsync();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.AllDisabledAsync();

        tenancy.ProvisioningAttempts.ShouldBe(1);
    }

    [Fact]
    public async Task auto_create_none_never_emits_ddl()
    {
        // The schema is the operator's, and the control table is exactly the object a
        // least-privilege deployment provisions by hand.
        var tenancy = TenancyFor(x => x.AutoCreate = AutoCreate.None);

        await Should.ThrowAsync<SqliteException>(() => tenancy.BuildDatabasesAsync());

        (await ControlTableExistsAsync()).ShouldBeFalse();
        tenancy.ProvisioningAttempts.ShouldBe(0);
    }

    // ------------------------------------------------------------------------------------------
    // The runtime lifecycle
    // ------------------------------------------------------------------------------------------

    [Fact]
    public async Task adding_a_tenant_records_it_and_caches_it_eagerly()
    {
        var tenancy = TenancyFor();

        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");

        (await RowsAsync()).ShouldBe([("blue", "Data Source=blue.db", false)]);

        // Cached without a second read: the tenant is usable by the next session immediately.
        tenancy.FindCachedDatabase("blue").ShouldNotBeNull();
    }

    [Fact]
    public async Task adding_a_tenant_twice_is_an_upsert()
    {
        var tenancy = TenancyFor();

        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=one.db");
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=two.db");

        (await RowsAsync()).ShouldBe([("blue", "Data Source=two.db", false)]);
        tenancy.FindCachedDatabase("blue")!.ConnectionString.ShouldBe("Data Source=two.db");
    }

    [Fact]
    public async Task re_pointing_a_tenant_disposes_the_database_it_replaced()
    {
        var tenancy = TenancyFor();

        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=one.db");
        var first = tenancy.FindCachedDatabase("blue")!;

        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=two.db");

        first.Disposed.ShouldBeTrue();
        tenancy.FindCachedDatabase("blue")!.Disposed.ShouldBeFalse();
    }

    [Fact]
    public async Task building_databases_is_idempotent()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");

        var built = await tenancy.BuildDatabasesAsync();
        var again = await tenancy.BuildDatabasesAsync();

        // The same instance, not an equal one. A refresh on a schedule must not churn every
        // tenant's connection pool.
        built.Single().ShouldBeSameAs(again.Single());
    }

    [Fact]
    public async Task building_databases_rebuilds_a_tenant_whose_connection_string_changed()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=one.db");
        var first = tenancy.FindCachedDatabase("blue")!;

        // Somebody else re-pointed the tenant.
        await using (var conn = new SqliteConnection(_connectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "update tenants set connection_string = 'Data Source=two.db' where tenant_id = 'blue'";
            await cmd.ExecuteNonQueryAsync();
        }

        await tenancy.BuildDatabasesAsync();

        tenancy.FindCachedDatabase("blue")!.ConnectionString.ShouldBe("Data Source=two.db");
        first.Disposed.ShouldBeTrue();
    }

    [Fact]
    public async Task an_unknown_tenant_resolves_to_nothing()
    {
        var tenancy = TenancyFor();
        await tenancy.BuildDatabasesAsync();

        (await tenancy.TryFindDatabaseAsync("nobody")).ShouldBeNull();
    }

    [Fact]
    public async Task a_tenant_is_resolved_from_the_control_table_when_it_is_not_cached()
    {
        var writer = TenancyFor();
        await writer.AddDatabaseRecordAsync("blue", "Data Source=blue.db");

        // A second process, with an empty cache.
        var reader = TenancyFor();
        var database = await reader.TryFindDatabaseAsync("blue");

        database.ShouldNotBeNull();
        database.ConnectionString.ShouldBe("Data Source=blue.db");
    }

    [Fact]
    public async Task disabling_a_tenant_stops_it_resolving_and_evicts_it()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        var database = tenancy.FindCachedDatabase("blue")!;

        await tenancy.DisableTenantAsync("blue");

        // The eviction is what makes disabling take effect at all -- the cache does not consult
        // the control table, so a cached database would go on serving a switched-off tenant.
        tenancy.FindCachedDatabase("blue").ShouldBeNull();
        database.Disposed.ShouldBeTrue();
        (await tenancy.TryFindDatabaseAsync("blue")).ShouldBeNull();

        // The record survives -- disabling is not deleting.
        (await RowsAsync()).ShouldBe([("blue", "Data Source=blue.db", true)]);
    }

    [Fact]
    public async Task a_disabled_tenant_is_excluded_from_the_build()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");
        await tenancy.DisableTenantAsync("green");

        var databases = await tenancy.BuildDatabasesAsync();

        databases.Select(x => x.TenantId).ShouldBe(["blue"]);
    }

    [Fact]
    public async Task enabling_a_tenant_makes_it_resolve_again()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.DisableTenantAsync("blue");

        await tenancy.EnableTenantAsync("blue");

        (await tenancy.TryFindDatabaseAsync("blue")).ShouldNotBeNull();
        (await RowsAsync()).ShouldBe([("blue", "Data Source=blue.db", false)]);
    }

    [Fact]
    public async Task enabling_a_tenant_does_not_conjure_one_that_was_never_added()
    {
        var tenancy = TenancyFor();
        await tenancy.BuildDatabasesAsync();

        await tenancy.EnableTenantAsync("nobody");

        // Lazily loaded on next access, which is also what keeps this a no-op rather than a
        // phantom database for a tenant with no record.
        tenancy.FindCachedDatabase("nobody").ShouldBeNull();
        (await RowsAsync()).ShouldBeEmpty();
    }

    [Fact]
    public async Task all_disabled_reports_the_disabled_tenants()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");
        await tenancy.AddDatabaseRecordAsync("red", "Data Source=red.db");
        await tenancy.DisableTenantAsync("green");
        await tenancy.DisableTenantAsync("red");

        (await tenancy.AllDisabledAsync()).OrderBy(x => x).ShouldBe(["green", "red"]);
    }

    [Fact]
    public async Task deleting_a_tenant_removes_its_record_and_evicts_it()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        var database = tenancy.FindCachedDatabase("blue")!;

        await tenancy.DeleteDatabaseRecordAsync("blue");

        (await RowsAsync()).ShouldBeEmpty();
        tenancy.FindCachedDatabase("blue").ShouldBeNull();
        database.Disposed.ShouldBeTrue();
    }

    [Fact]
    public async Task clearing_removes_every_record_and_empties_the_cache()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");

        await tenancy.ClearAllDatabaseRecordsAsync();

        (await RowsAsync()).ShouldBeEmpty();
        tenancy.AllDatabases().ShouldBeEmpty();
    }

    // ------------------------------------------------------------------------------------------
    // The shared options -- what Polecat's inline re-implementation was missing
    // ------------------------------------------------------------------------------------------

    [Fact]
    public async Task seed_databases_are_written_into_the_control_table()
    {
        var tenancy = TenancyFor(x =>
        {
            x.SeedDatabases.Register("blue", "Data Source=blue.db");
            x.SeedDatabases.Register("green", "Data Source=green.db");
        });

        var databases = await tenancy.BuildDatabasesAsync();

        databases.Select(x => x.TenantId).OrderBy(x => x).ShouldBe(["blue", "green"]);
        (await RowsAsync()).Count.ShouldBe(2);
    }

    [Fact]
    public async Task seeding_happens_once()
    {
        var tenancy = TenancyFor(x => x.SeedDatabases.Register("blue", "Data Source=blue.db"));

        await tenancy.BuildDatabasesAsync();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=corrected.db");
        await tenancy.BuildDatabasesAsync();

        // A second seed would have put the configured string back over the runtime change.
        (await RowsAsync()).Single().ConnectionString.ShouldBe("Data Source=corrected.db");
    }

    [Fact]
    public async Task the_application_name_reaches_the_tenant_database()
    {
        // The reason a store wants the shared options rather than its own: every tenant connection
        // string gets the application name applied, for diagnostics. Polecat's inline options had
        // no equivalent at all.
        var provider = Substitute.For<IDatabaseProvider>();
        provider.DefaultDatabaseSchemaName.Returns("main");
        provider.AddApplicationNameToConnectionString(Arg.Any<string>(), Arg.Any<string>())
            .Returns(call => $"{call.ArgAt<string>(0)};App={call.ArgAt<string>(1)}");

        var tenancy = new TestTenancy(new MasterTableTenancyOptions<SqliteDataSource>(provider)
        {
            DataSource = new SqliteDataSource(_connectionString),
            SchemaName = "main",
            ApplicationName = "reporting"
        });
        _tenancies.Add(tenancy);

        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");

        tenancy.FindCachedDatabase("blue")!.ConnectionString.ShouldBe("Data Source=blue.db;App=reporting");

        // The stored record keeps the caller's string -- the application name is this process's
        // diagnostic label, not part of the tenant's identity.
        (await RowsAsync()).Single().ConnectionString.ShouldBe("Data Source=blue.db");
    }

    // ------------------------------------------------------------------------------------------
    // Seams
    // ------------------------------------------------------------------------------------------

    [Fact]
    public async Task every_control_plane_call_goes_through_the_execute_seam()
    {
        // Polecat wraps every control-table call in its Polly pipeline. The base has to route all
        // of them through one overridable point or the store gets a partly-resilient tenancy.
        var tenancy = TenancyFor();

        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.BuildDatabasesAsync();
        await tenancy.DisableTenantAsync("blue");
        await tenancy.EnableTenantAsync("blue");
        await tenancy.AllDisabledAsync();
        await tenancy.LookupConnectionStringAsync("blue");
        await tenancy.DeleteDatabaseRecordAsync("blue");
        await tenancy.ClearAllDatabaseRecordsAsync();

        tenancy.Executions.ShouldBeGreaterThanOrEqualTo(9);
    }

    [Fact]
    public async Task tenant_ids_are_corrected_on_every_entry_point()
    {
        // One place, applied everywhere, because the failure mode of missing one is a tenant that
        // resolves through one method and not another -- which reads as an intermittent.
        var tenancy = TenancyFor();
        tenancy.LowerCaseTenantIds = true;

        await tenancy.AddDatabaseRecordAsync("BLUE", "Data Source=blue.db");

        (await RowsAsync()).Single().TenantId.ShouldBe("blue");
        tenancy.FindCachedDatabase("Blue").ShouldNotBeNull();
        (await tenancy.TryFindDatabaseAsync("bLuE")).ShouldNotBeNull();

        await tenancy.DisableTenantAsync("Blue");
        (await tenancy.AllDisabledAsync()).ShouldBe(["blue"]);
    }

    [Fact]
    public async Task disposal_disposes_every_cached_database()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");

        var databases = tenancy.AllDatabases().ToList();
        await tenancy.DisposeAsync();

        databases.ShouldAllBe(x => x.Disposed);
    }

    // ------------------------------------------------------------------------------------------
    // IDynamicTenantSource<string> -- the block both stores wrote by hand
    // ------------------------------------------------------------------------------------------

    [Fact]
    public async Task the_dynamic_source_finds_a_tenants_connection_string()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");

        ITenantedSource<string> source = tenancy;

        (await source.FindAsync("blue")).ShouldBe("Data Source=blue.db");
        source.Cardinality.ShouldBe(JasperFx.Descriptors.DatabaseCardinality.DynamicMultiple);
    }

    [Fact]
    public async Task the_dynamic_source_refuses_an_unknown_or_disabled_tenant_the_same_way()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.DisableTenantAsync("blue");

        ITenantedSource<string> source = tenancy;

        await Should.ThrowAsync<UnknownTenantIdException>(async () => await source.FindAsync("blue"));
        await Should.ThrowAsync<UnknownTenantIdException>(async () => await source.FindAsync("nobody"));
    }

    [Fact]
    public async Task the_dynamic_source_reports_tenant_ids_never_connection_strings()
    {
        // Deliberately not the connection strings, which would put credentials on an admin
        // dashboard.
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db;Password=hunter2");

        ITenantedSource<string> source = tenancy;

        source.AllActive().ShouldBe(["blue"]);
        source.AllActiveByTenant().Single().ShouldBe(new Assignment<string>("blue", "blue"));
    }

    [Fact]
    public async Task refreshing_drops_a_tenant_that_has_since_been_removed()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");

        // Removed by another process.
        await using (var conn = new SqliteConnection(_connectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "delete from tenants where tenant_id = 'green'";
            await cmd.ExecuteNonQueryAsync();
        }

        ITenantedSource<string> source = tenancy;
        await source.RefreshAsync();

        // A merge would have gone on serving the removed tenant from cache forever.
        source.AllActive().ShouldBe(["blue"]);
    }

    [Fact]
    public async Task the_dynamic_lifecycle_drives_the_same_runtime()
    {
        IDynamicTenantSource<string> source = TenancyFor();

        await source.AddTenantAsync("blue", "Data Source=blue.db");
        (await RowsAsync()).ShouldBe([("blue", "Data Source=blue.db", false)]);

        await source.DisableTenantAsync("blue");
        (await source.AllDisabledAsync()).ShouldBe(["blue"]);

        await source.EnableTenantAsync("blue");
        (await source.AllDisabledAsync()).ShouldBeEmpty();

        await source.RemoveTenantAsync("blue");
        (await RowsAsync()).ShouldBeEmpty();
    }

    [Fact]
    public async Task auto_assignment_is_refused()
    {
        // There is no pool for a database-per-tenant model to assign from, so a caller has to
        // supply a connection string. Left on the interface's throwing default rather than
        // answered wrongly.
        IDynamicTenantSource<string> source = TenancyFor();

        await Should.ThrowAsync<NotSupportedException>(() => source.AddTenantAsync("blue"));
    }

    [Fact]
    public async Task the_host_level_convenience_surface_is_wired_up()
    {
        IMasterTableMultiTenancy tenancy = TenancyFor();

        (await tenancy.TryAddTenantDatabaseRecordsAsync("blue", "Data Source=blue.db")).ShouldBeTrue();
        (await RowsAsync()).Count.ShouldBe(1);

        (await tenancy.ClearAllDatabaseRecordsAsync()).ShouldBeTrue();
        (await RowsAsync()).ShouldBeEmpty();
    }

    // ------------------------------------------------------------------------------------------
    // weasel#809 -- a refresh evicts what another node removed or disabled
    // ------------------------------------------------------------------------------------------

    [Fact]
    public async Task building_databases_evicts_a_tenant_removed_by_another_node()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");
        var green = tenancy.FindCachedDatabase("green")!;

        // Another node removes it. This one's cache knows nothing about that.
        await TenancyFor().DeleteDatabaseRecordAsync("green");
        tenancy.FindCachedDatabase("green").ShouldBeSameAs(green);

        var databases = await tenancy.BuildDatabasesAsync();

        // polecat#871 / #849: this used to only add, so a removed tenant went on resolving here
        // from cache until the process restarted.
        databases.Select(x => x.TenantId).ShouldBe(["blue"]);
        tenancy.FindCachedDatabase("green").ShouldBeNull();
        (await tenancy.TryFindDatabaseAsync("green")).ShouldBeNull();
        green.Disposed.ShouldBeTrue();
    }

    [Fact]
    public async Task building_databases_evicts_a_tenant_disabled_by_another_node()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");
        var blue = tenancy.FindCachedDatabase("blue")!;
        var green = tenancy.FindCachedDatabase("green")!;

        await TenancyFor().DisableTenantAsync("green");

        var databases = await tenancy.BuildDatabasesAsync();

        databases.Select(x => x.TenantId).ShouldBe(["blue"]);
        tenancy.FindCachedDatabase("green").ShouldBeNull();
        green.Disposed.ShouldBeTrue();

        // The tenant that is still enabled keeps its database: evicting is not churning.
        tenancy.FindCachedDatabase("blue").ShouldBeSameAs(blue);
        blue.Disposed.ShouldBeFalse();
    }

    [Fact]
    public async Task the_stale_tenant_hook_is_called_for_each_evicted_tenant_and_only_those()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");
        await tenancy.AddDatabaseRecordAsync("red", "Data Source=red.db");
        var green = tenancy.FindCachedDatabase("green")!;
        var red = tenancy.FindCachedDatabase("red")!;

        var other = TenancyFor();
        await other.DisableTenantAsync("green");
        await other.DeleteDatabaseRecordAsync("red");

        await tenancy.BuildDatabasesAsync();

        tenancy.StaleEvictions.OrderBy(x => x.TenantId).ShouldBe([("green", green), ("red", red)]);

        // Nothing is stale on a second read.
        tenancy.StaleEvictions.Clear();
        await tenancy.BuildDatabasesAsync();
        tenancy.StaleEvictions.ShouldBeEmpty();
    }

    [Fact]
    public async Task an_override_that_skips_the_base_eviction_keeps_the_tenant_cached()
    {
        // The pre-#809 behaviour, for a store that wants it -- the hook decides.
        var tenancy = TenancyFor();
        tenancy.KeepStaleTenants = true;
        await tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");
        var green = tenancy.FindCachedDatabase("green")!;

        await TenancyFor().DeleteDatabaseRecordAsync("green");
        await tenancy.BuildDatabasesAsync();

        tenancy.StaleEvictions.Select(x => x.TenantId).ShouldBe(["green"]);
        tenancy.FindCachedDatabase("green").ShouldBeSameAs(green);
        green.Disposed.ShouldBeFalse();
    }

    [Fact]
    public async Task a_tenant_re_added_while_the_table_is_being_read_is_not_evicted()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");
        var first = tenancy.FindCachedDatabase("green")!;

        await TenancyFor().DeleteDatabaseRecordAsync("green");

        // Re-added on this node after the read has come back without it, and before the read's
        // result is applied. The read cannot have seen it, so it must not evict it.
        tenancy.AfterNextExecute = () => tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");

        await tenancy.BuildDatabasesAsync();

        var current = tenancy.FindCachedDatabase("green");
        current.ShouldNotBeNull();
        current.ShouldNotBeSameAs(first);
        current.Disposed.ShouldBeFalse();
        tenancy.StaleEvictions.ShouldBeEmpty();
    }

    [Fact]
    public async Task refreshing_keeps_a_live_tenants_database()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        var blue = tenancy.FindCachedDatabase("blue")!;

        await ((ITenantedSource<string>)tenancy).RefreshAsync();

        // A refresh used to evict everything and rebuild, disposing a live tenant's database (and
        // its connection pool) out from under sessions still using it.
        tenancy.FindCachedDatabase("blue").ShouldBeSameAs(blue);
        blue.Disposed.ShouldBeFalse();
    }

    [Fact]
    public async Task evicting_a_tenant_drops_the_cache_entry_and_leaves_the_record()
    {
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        var blue = tenancy.FindCachedDatabase("blue")!;

        (await tenancy.EvictForTestAsync("blue")).ShouldBeTrue();
        (await tenancy.EvictForTestAsync("blue")).ShouldBeFalse();

        tenancy.FindCachedDatabase("blue").ShouldBeNull();
        blue.Disposed.ShouldBeTrue();
        (await RowsAsync()).ShouldBe([("blue", "Data Source=blue.db", false)]);

        // A cache operation, not a lifecycle one: the tenant resolves again.
        (await tenancy.TryFindDatabaseAsync("blue")).ShouldNotBeNull();
    }

    // ------------------------------------------------------------------------------------------
    // weasel#809 -- disabled is not unknown
    // ------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_lookup_tells_unknown_disabled_and_enabled_apart(bool dialectRendersRowRead)
    {
        // false: a dialect written before SelectTenantRow existed, which the base answers with two
        // reads instead of one.
        var tenancy = TenancyFor();
        tenancy.DialectRendersRowRead = dialectRendersRowRead;
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");
        await tenancy.DisableTenantAsync("green");

        (await tenancy.LookupTenantAsync("blue"))
            .ShouldBe(new MasterTenantLookup(MasterTenantStatus.Enabled, "Data Source=blue.db"));

        // The connection string is withheld, so a caller that only null-checks cannot route to it.
        (await tenancy.LookupTenantAsync("green"))
            .ShouldBe(new MasterTenantLookup(MasterTenantStatus.Disabled, null));

        (await tenancy.LookupTenantAsync("nobody"))
            .ShouldBe(new MasterTenantLookup(MasterTenantStatus.Unknown, null));
    }

    [Fact]
    public async Task a_lookup_applies_the_tenant_id_correction()
    {
        var tenancy = TenancyFor();
        tenancy.LowerCaseTenantIds = true;
        tenancy.DialectRendersRowRead = false;
        await tenancy.AddDatabaseRecordAsync("green", "Data Source=green.db");
        await tenancy.DisableTenantAsync("green");

        (await tenancy.LookupTenantAsync("GREEN")).Status.ShouldBe(MasterTenantStatus.Disabled);
    }

    [Fact]
    public async Task resolving_a_tenant_says_why_there_is_nothing_to_route_to()
    {
        var writer = TenancyFor();
        await writer.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await writer.AddDatabaseRecordAsync("green", "Data Source=green.db");
        await writer.DisableTenantAsync("green");

        // A second node, with an empty cache.
        var tenancy = TenancyFor();

        var blue = await tenancy.ResolveTenantAsync("blue");
        blue.Status.ShouldBe(MasterTenantStatus.Enabled);
        blue.Database.ShouldNotBeNull().ConnectionString.ShouldBe("Data Source=blue.db");

        // Cached, as TryFindDatabaseAsync would have.
        tenancy.FindCachedDatabase("blue").ShouldBeSameAs(blue.Database);
        (await tenancy.ResolveTenantAsync("blue")).Database.ShouldBeSameAs(blue.Database);

        (await tenancy.ResolveTenantAsync("green"))
            .ShouldBe(new MasterTenantResolution<TestDatabase>(MasterTenantStatus.Disabled, null));
        (await tenancy.ResolveTenantAsync("nobody"))
            .ShouldBe(new MasterTenantResolution<TestDatabase>(MasterTenantStatus.Unknown, null));

        // Neither refusal is cached.
        tenancy.AllTenantIds().ShouldBe(["blue"]);
    }

    [Fact]
    public async Task the_refusal_hook_lets_a_store_refuse_a_disabled_tenant_as_disabled()
    {
        var tenancy = TenancyFor();
        tenancy.RefuseDisabledAsDisabled = true;
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.DisableTenantAsync("blue");

        ITenantedSource<string> source = tenancy;

        // What Marten and Polecat throw today (marten#5479, polecat#655). It derives from
        // UnknownTenantIdException, so the default's callers keep catching.
        await Should.ThrowAsync<DisabledTenantException>(async () => await source.FindAsync("blue"));

        var unknown = await Should.ThrowAsync<UnknownTenantIdException>(async () => await source.FindAsync("nobody"));
        unknown.ShouldNotBeOfType<DisabledTenantException>();

        tenancy.Refusals.ShouldBe([("blue", MasterTenantStatus.Disabled), ("nobody", MasterTenantStatus.Unknown)]);
    }

    [Fact]
    public async Task the_default_refusal_is_still_unknown_for_a_disabled_tenant()
    {
        // Additive only: a store that does not override the hook throws what it always did.
        var tenancy = TenancyFor();
        await tenancy.AddDatabaseRecordAsync("blue", "Data Source=blue.db");
        await tenancy.DisableTenantAsync("blue");

        ITenantedSource<string> source = tenancy;

        var refusal = await Should.ThrowAsync<UnknownTenantIdException>(async () => await source.FindAsync("blue"));
        refusal.ShouldBeOfType<UnknownTenantIdException>();
    }

    #region test doubles

    /// <summary>Stands in for a store's own database type.</summary>
    private sealed class TestDatabase(string tenantId, string connectionString): IAsyncDisposable
    {
        public string TenantId { get; } = tenantId;
        public string ConnectionString { get; } = connectionString;
        public bool Disposed { get; private set; }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return default;
        }
    }

    /// <summary>SQLite control-table SQL. SQLite has no schemas, so the schema folds into the name.</summary>
    private sealed class SqliteTenantDialect: IMasterTenantTableDialect
    {
        /// <summary>
        /// How many times the base actually rendered the control-table DDL. Counted here rather
        /// than around <c>ProvisionControlTableAsync</c>, which is called on every entry point and
        /// returns early once provisioned — so counting the calls would say nothing.
        /// </summary>
        public int CreateControlTableCalls { get; private set; }

        public string QualifiedTableName(string schemaName, string tableName) =>
            string.IsNullOrWhiteSpace(schemaName) || schemaName == "main"
                ? $"\"{tableName}\""
                : $"\"{schemaName}_{tableName}\"";

        public string CreateControlTable(string schemaName, string tableName, string qualifiedTableName)
        {
            CreateControlTableCalls++;
            return $"""
             create table if not exists {qualifiedTableName} (
                 tenant_id text not null primary key,
                 connection_string text not null,
                 is_disabled integer not null default 0
             );
             """;
        }

        public string SelectEnabledTenants(string qualifiedTableName) =>
            $"select tenant_id, connection_string from {qualifiedTableName} where is_disabled = 0";

        public string SelectDisabledTenantIds(string qualifiedTableName) =>
            $"select tenant_id from {qualifiedTableName} where is_disabled = 1";

        public string SelectConnectionString(string qualifiedTableName) =>
            $"select connection_string from {qualifiedTableName} where tenant_id = @id and is_disabled = 0";

        /// <summary>False stands in for a dialect written before the member existed.</summary>
        public bool RendersRowRead { get; set; } = true;

        public string? SelectTenantRow(string qualifiedTableName) => RendersRowRead
            ? $"select connection_string, is_disabled from {qualifiedTableName} where tenant_id = @id"
            : null;

        public string UpsertTenant(string qualifiedTableName) =>
            $"""
             insert into {qualifiedTableName} (tenant_id, connection_string, is_disabled)
             values (@id, @connection, 0)
             on conflict (tenant_id) do update set connection_string = excluded.connection_string
             """;

        public string DeleteTenant(string qualifiedTableName) =>
            $"delete from {qualifiedTableName} where tenant_id = @id";

        public string DeleteAllTenants(string qualifiedTableName) => $"delete from {qualifiedTableName}";

        public string SetTenantDisabled(string qualifiedTableName, bool disabled) =>
            $"update {qualifiedTableName} set is_disabled = {(disabled ? 1 : 0)} where tenant_id = @id";
    }

    private sealed class TestTenancy: MasterTableTenancyBase<TestDatabase, SqliteDataSource>
    {
        private readonly SqliteTenantDialect _dialect;
        private int _executions;

        public TestTenancy(MasterTableTenancyOptions<SqliteDataSource> options)
            : this(options, new SqliteTenantDialect())
        {
        }

        private TestTenancy(MasterTableTenancyOptions<SqliteDataSource> options, SqliteTenantDialect dialect)
            : base(options, "tenants", dialect) => _dialect = dialect;

        public int Executions => _executions;
        public int ProvisioningAttempts => _dialect.CreateControlTableCalls;
        public bool LowerCaseTenantIds { get; set; }

        public bool DialectRendersRowRead
        {
            get => _dialect.RendersRowRead;
            set => _dialect.RendersRowRead = value;
        }

        /// <summary>Every call to the stale-tenant hook.</summary>
        public List<(string TenantId, TestDatabase Database)> StaleEvictions { get; } = [];

        /// <summary>Skip the base eviction in the stale-tenant hook.</summary>
        public bool KeepStaleTenants { get; set; }

        /// <summary>Refuse a disabled tenant with DisabledTenantException, as Marten and Polecat do.</summary>
        public bool RefuseDisabledAsDisabled { get; set; }

        public List<(string TenantId, MasterTenantStatus Status)> Refusals { get; } = [];

        /// <summary>Run once, after the next control-plane call has completed.</summary>
        public Func<Task>? AfterNextExecute { get; set; }

        public ValueTask<bool> EvictForTestAsync(string tenantId) => EvictTenantAsync(tenantId);

        protected override SqliteDataSource BuildDataSource(string connectionString) => new(connectionString);

        protected override TestDatabase BuildDatabase(string tenantId, string connectionString) =>
            new(tenantId, connectionString);

        protected override string CorrectTenantId(string tenantId) =>
            LowerCaseTenantIds ? tenantId.ToLowerInvariant() : tenantId;

        protected override async ValueTask EvictStaleTenantAsync(
            string tenantId, TestDatabase database, CancellationToken token)
        {
            StaleEvictions.Add((tenantId, database));

            if (!KeepStaleTenants)
            {
                await base.EvictStaleTenantAsync(tenantId, database, token);
            }
        }

        protected override UnknownTenantIdException CreateTenantRefusal(string tenantId, MasterTenantStatus status)
        {
            Refusals.Add((tenantId, status));

            return RefuseDisabledAsDisabled && status == MasterTenantStatus.Disabled
                ? new DisabledTenantException(tenantId)
                : base.CreateTenantRefusal(tenantId, status);
        }

        protected override async Task<T> ExecuteAsync<T>(
            Func<CancellationToken, Task<T>> operation, CancellationToken token)
        {
            Interlocked.Increment(ref _executions);
            var result = await base.ExecuteAsync(operation, token);

            var after = AfterNextExecute;
            if (after is not null)
            {
                AfterNextExecute = null;
                await after();
            }

            return result;
        }
    }

    #endregion
}
