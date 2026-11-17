using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables.partitioning;

/// <summary>
///     weasel#877: a SPLIT gives up at <see cref="ManagedTenantPartitions.SplitCommandTimeout" /> while the
///     registry statements on the same connection keep the connection's command timeout.
/// </summary>
/// <remarks>
///     Another session holds a shared lock on the partitioned table, which is enough to keep the SPLIT waiting
///     for its schema modification lock. The connection's own limit is a minute, so a SPLIT that waited on it
///     instead of its own would take far longer than any bound here.
/// </remarks>
[Collection("integration")]
public class managed_tenant_partitions_split_timeout: IntegrationContext
{
    private const string Schema = "mtp_split";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // Partition functions and schemes are database-wide, so the tables get names of their own
    private readonly string _tableName = "split_" + Guid.NewGuid().ToString("N")[..8];

    public managed_tenant_partitions_split_timeout(): base(Schema)
    {
    }

    private string PartitionFunction => $"pf_{_tableName}_tenant_ordinal";
    private string PartitionScheme => $"ps_{_tableName}_tenant_ordinal";
    private string QualifiedTable => $"[{Schema}].[{_tableName}]";
    private const string QualifiedRegistry = $"[{Schema}].[tenant_registry]";

    public override async ValueTask InitializeAsync()
    {
        await ResetSchema();
    }

    public override async ValueTask DisposeAsync()
    {
        await ResetSchema();
        await theConnection.CreateCommand($@"
IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{PartitionScheme}')
    DROP PARTITION SCHEME [{PartitionScheme}];
IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{PartitionFunction}')
    DROP PARTITION FUNCTION [{PartitionFunction}];").ExecuteNonQueryAsync(Ct);

        await base.DisposeAsync();
    }

    private static string ConnectionStringWithCommandTimeout(int seconds)
        => new SqlConnectionStringBuilder(ConnectionSource.ConnectionString) { CommandTimeout = seconds }
            .ConnectionString;

    /// <summary>
    ///     A partitioned table with one tenant already in it, so the registry exists and the map is loaded,
    ///     which leaves the MERGE for the next tenant as the only registry statement.
    /// </summary>
    private async Task<(DatabaseWithTables, ManagedTenantPartitions)> ProvisionAsync(int commandTimeoutSeconds)
    {
        var database = new DatabaseWithTables("mtp_split",
            ConnectionStringWithCommandTimeout(commandTimeoutSeconds));
        var manager = new ManagedTenantPartitions("tenants", new DbObjectName(Schema, "tenant_registry"));

        var table = new Table(new DbObjectName(Schema, _tableName));
        table.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        table.AddColumn<int>("id").AsPrimaryKey();
        table.PartitionByManagedTenants(manager);
        database.AddTable(table);

        await CreateSchemaObjectInDatabase(table);

        await manager.AddPartitionToAllTables(database, "incumbent", Ct);
        await theConnection.CreateCommand($"INSERT INTO {QualifiedTable} (tenant_ordinal, id) VALUES (1, 1)")
            .ExecuteNonQueryAsync(Ct);

        return (database, manager);
    }

    [Fact]
    public async Task a_slow_registration_keeps_the_connections_limit_and_the_split_gives_up_at_its_own()
    {
        var (database, manager) = await ProvisionAsync(60);
        manager.SplitCommandTimeout = TimeSpan.FromSeconds(1);

        await using var tableLock = await SessionLock.TakeAsync($"SELECT COUNT(*) FROM {QualifiedTable} WITH (TABLOCK, HOLDLOCK)");

        TenantPartitionAddResult result;
        var stopwatch = Stopwatch.StartNew();
        await using (var registryLock = await SessionLock.TakeAsync($"SELECT COUNT(*) FROM {QualifiedRegistry} WITH (TABLOCKX, HOLDLOCK)"))
        {
            var adding = manager.AddPartitionsToAllTables(NullLogger.Instance, database, ["newcomer"], Ct);

            // The MERGE waits longer than the SPLIT is allowed to before it can go ahead
            await registryLock.WaitUntilSomethingIsBlockedAsync(Ct);
            await Task.Delay(TimeSpan.FromSeconds(2), Ct);
            await registryLock.ReleaseAsync();

            result = await adding;
        }

        stopwatch.Stop();

        result.Ordinals["newcomer"].ShouldBe(2);
        (await RegisteredOrdinalAsync("newcomer")).ShouldBe(2);

        result.Tables.Single().Status.ShouldBe(PartitionMigrationStatus.Failed);
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(30));
        (await ReadBoundariesAsync()).ShouldBe([0, 1]);

        // The usual handling of a failed SPLIT takes it from here
        await tableLock.ReleaseAsync();
        (await manager.MigrateAllTablesAsync(NullLogger.Instance, database, Ct))
            .Single().Status.ShouldBe(PartitionMigrationStatus.Complete);
        (await ReadBoundariesAsync()).ShouldBe([0, 1, 2]);
    }

    [Fact]
    public async Task the_limit_can_be_given_for_one_call()
    {
        var (database, manager) = await ProvisionAsync(60);
        manager.SplitCommandTimeout.ShouldBeNull();

        await using var tableLock = await SessionLock.TakeAsync($"SELECT COUNT(*) FROM {QualifiedTable} WITH (TABLOCK, HOLDLOCK)");

        var stopwatch = Stopwatch.StartNew();
        var result = await manager.AddPartitionsToAllTables(NullLogger.Instance, database, ["newcomer"],
            TimeSpan.FromSeconds(1), Ct);
        stopwatch.Stop();

        result.Ordinals["newcomer"].ShouldBe(2);
        result.Tables.Single().Status.ShouldBe(PartitionMigrationStatus.Failed);
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(30));
        (await ReadBoundariesAsync()).ShouldBe([0, 1]);
    }

    [Fact]
    public async Task without_a_split_limit_the_split_waits_on_the_connections()
    {
        var (database, manager) = await ProvisionAsync(60);

        var tableLock = await SessionLock.TakeAsync($"SELECT COUNT(*) FROM {QualifiedTable} WITH (TABLOCK, HOLDLOCK)");
        await using (tableLock)
        {
            var adding = manager.AddPartitionsToAllTables(NullLogger.Instance, database, ["newcomer"], Ct);

            await tableLock.WaitUntilSomethingIsBlockedAsync(Ct);
            await Task.Delay(TimeSpan.FromSeconds(2), Ct);
            await tableLock.ReleaseAsync();

            (await adding).Tables.Single().Status.ShouldBe(PartitionMigrationStatus.Complete);
        }

        (await ReadBoundariesAsync()).ShouldBe([0, 1, 2]);
    }

    [Fact]
    public async Task a_split_limit_longer_than_the_connections_is_the_one_the_split_gets()
    {
        // The registry statements are not blocked, so they are well inside a 1 second limit
        var (database, manager) = await ProvisionAsync(1);
        manager.SplitCommandTimeout = TimeSpan.FromSeconds(60);

        var tableLock = await SessionLock.TakeAsync($"SELECT COUNT(*) FROM {QualifiedTable} WITH (TABLOCK, HOLDLOCK)");
        await using (tableLock)
        {
            var adding = manager.AddPartitionsToAllTables(NullLogger.Instance, database, ["newcomer"], Ct);

            await tableLock.WaitUntilSomethingIsBlockedAsync(Ct);
            await Task.Delay(TimeSpan.FromSeconds(2), Ct);
            await tableLock.ReleaseAsync();

            (await adding).Tables.Single().Status.ShouldBe(PartitionMigrationStatus.Complete);
        }

        (await ReadBoundariesAsync()).ShouldBe([0, 1, 2]);
    }

    private async Task<int> RegisteredOrdinalAsync(string tenantId)
    {
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = $"SELECT ordinal FROM {QualifiedRegistry} WHERE tenant_id = @tenant";
        cmd.Parameters.AddWithValue("@tenant", tenantId);
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private async Task<int[]> ReadBoundariesAsync()
    {
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = @"
SELECT CAST(prv.value AS int)
FROM sys.partition_functions pf
JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id
WHERE pf.name = @pf
ORDER BY prv.boundary_id;";
        cmd.Parameters.AddWithValue("@pf", PartitionFunction);

        var list = new List<int>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct))
        {
            list.Add(reader.GetInt32(0));
        }

        return list.ToArray();
    }

    /// <summary>
    ///     Another session's open transaction holding whatever locks <c>sql</c> takes, until it is released.
    /// </summary>
    private sealed class SessionLock: IAsyncDisposable
    {
        private readonly SqlConnection _connection;
        private readonly SqlTransaction _transaction;
        private readonly short _sessionId;
        private bool _released;

        private SessionLock(SqlConnection connection, SqlTransaction transaction, short sessionId)
        {
            _connection = connection;
            _transaction = transaction;
            _sessionId = sessionId;
        }

        public static async Task<SessionLock> TakeAsync(string sql)
        {
            var connection = new SqlConnection(ConnectionSource.ConnectionString);
            await connection.OpenAsync(Ct);
            var transaction = (SqlTransaction)await connection.BeginTransactionAsync(Ct);

            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = sql;
                await command.ExecuteScalarAsync(Ct);
            }

            short sessionId;
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = "SELECT @@SPID";
                sessionId = (short)(await command.ExecuteScalarAsync(Ct))!;
            }

            return new SessionLock(connection, transaction, sessionId);
        }

        /// <summary>
        ///     Waits until another session is queued behind this one, so the wait the test then adds is
        ///     really spent blocked and not spent getting there.
        /// </summary>
        public async Task WaitUntilSomethingIsBlockedAsync(CancellationToken token)
        {
            await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
            await conn.OpenAsync(token);

            var deadline = Stopwatch.StartNew();
            while (true)
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id = @spid";
                cmd.Parameters.AddWithValue("@spid", _sessionId);
                if ((int)(await cmd.ExecuteScalarAsync(token))! > 0)
                {
                    return;
                }

                if (deadline.Elapsed > TimeSpan.FromSeconds(30))
                {
                    throw new TimeoutException($"Nothing was blocked behind session {_sessionId}");
                }

                await Task.Delay(50, token);
            }
        }

        public async Task ReleaseAsync()
        {
            if (_released) return;
            _released = true;

            await _transaction.RollbackAsync();
            await _transaction.DisposeAsync();
            await _connection.DisposeAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await ReleaseAsync();
        }
    }
}
