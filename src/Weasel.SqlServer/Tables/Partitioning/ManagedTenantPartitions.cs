using System.Collections.ObjectModel;
using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.SqlServer.Tables.Partitioning;

/// <summary>
///     Read-only view onto the tenant -&gt; ordinal map a
///     <see cref="ManagedTenantPartitions" /> is currently tracking. Callers
///     (application code, sharding routers, partition-aware indexers) consult
///     this to translate a tenant identifier into the integer they have to
///     insert into the partitioned column.
/// </summary>
public interface ITenantPartitionManager
{
    /// <summary>
    ///     Name of the integer column that the partition function is built over.
    ///     Every table that uses this strategy must declare a column with this
    ///     name and type <c>int</c>.
    /// </summary>
    string Column { get; }

    /// <summary>
    ///     The SQL Server data type of the partition function parameter. Always
    ///     <c>int</c> for managed tenant partitions.
    /// </summary>
    string SqlDataType { get; }

    /// <summary>
    ///     Current tenant_id -&gt; ordinal map. Snapshot of the in-memory state
    ///     after the last <see cref="ManagedTenantPartitions.InitializeAsync(SqlConnection,CancellationToken)" /> or
    ///     add/reset call. Application code should look up the ordinal here when
    ///     writing rows for a tenant.
    /// </summary>
    IReadOnlyDictionary<string, int> Ordinals { get; }
}

/// <summary>
///     A runtime-managed SQL Server partition strategy that mirrors Postgresql's
///     <c>ManagedListPartitions</c>: tenants register at runtime (e.g. when a new
///     tenant signs up), the strategy allocates a small integer ordinal, persists
///     the mapping in a backing registry table, and SPLITs every partitioned table
///     so the new tenant lands in its own partition.
///     <para>
///         Unlike Postgresql LIST partitioning (which uses child tables keyed
///         directly by tenant_id strings), SQL Server only offers RANGE
///         partitioning. To get one partition per tenant on SQL Server we use a
///         compact <c>int</c> ordinal as the partition column. Application code
///         looks the ordinal up via <see cref="ITenantPartitionManager.Ordinals" />
///         and writes it into the column for every row.
///     </para>
///     <para>
///         The partition function for each table is <c>RANGE RIGHT FOR VALUES
///         (1, 2, 3, ... N)</c>, so tenant ordinal <c>k</c> lands in the
///         partition <c>k &lt;= column &lt; k+1</c>. Adding tenant <c>N+1</c>
///         issues an <c>ALTER PARTITION FUNCTION ... SPLIT RANGE (N+1)</c>
///         against every table whose partitioning is wired to this instance.
///     </para>
///     <para>Polecat #163 / Weasel #301.</para>
/// </summary>
public class ManagedTenantPartitions: FeatureSchemaBase, ISqlServerPartitioning,
    ITenantPartitionManager, IDatabaseInitializer<SqlConnection>
{
    private readonly Table _registry;
    private readonly IndexDefinition _uniqueOrdinalIndex;
    private readonly Dictionary<string, int> _ordinals = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private bool _hasInitialized;

    /// <summary>
    ///     Create a managed tenant partition strategy backed by a registry table.
    /// </summary>
    /// <param name="identifier">
    ///     <see cref="IFeatureSchema" /> identifier — usually the registry table
    ///     name. Shown as the filename when a SQL script is exported.
    /// </param>
    /// <param name="registryTableName">
    ///     Schema-qualified name of the registry table that persists the
    ///     tenant_id -&gt; ordinal map.
    /// </param>
    /// <param name="column">
    ///     Name of the integer ordinal column on partitioned tables. Defaults to
    ///     <c>tenant_ordinal</c>. Every table assigned this strategy must have a
    ///     column of this name and type <c>int</c> in its primary key.
    /// </param>
    public ManagedTenantPartitions(
        string identifier,
        DbObjectName registryTableName,
        string column = "tenant_ordinal")
        : base(identifier, new SqlServerMigrator())
    {
        if (string.IsNullOrWhiteSpace(column))
        {
            throw new ArgumentException("column must not be null or blank", nameof(column));
        }

        Column = column;
        SqlDataType = "int";

        _registry = new Table(registryTableName);
        _registry.AddColumn("tenant_id", "varchar(200)").AsPrimaryKey().NotNull();
        _registry.AddColumn<int>("ordinal").NotNull();

        // Unique index on ordinal lets us safely allocate from the in-memory
        // map without re-querying the table; concurrent ManagedTenantPartitions
        // instances writing simultaneously will collide here rather than
        // silently producing two tenants in the same partition.
        _uniqueOrdinalIndex =
            new IndexDefinition($"uniq_{registryTableName.Name}_ordinal") { IsUnique = true };
        _uniqueOrdinalIndex.AgainstColumns("ordinal");
        _registry.Indexes.Add(_uniqueOrdinalIndex);
    }

    /// <summary>
    ///     Opt-in tenant bucketing: allow multiple tenant ids to share one
    ///     ordinal (and therefore one partition per table) through the
    ///     explicit-ordinal <c>AddPartitionsToAllTables</c> overloads. Off by
    ///     default — automatic allocation keeps one tenant per partition and a
    ///     unique index on the registry's ordinal column guards against
    ///     accidental sharing. Turning this on removes that unique index (an
    ///     existing index is dropped by the next registry migration) and is the
    ///     mitigation for SQL Server's 15,000-partition-per-table ceiling when
    ///     hosting many small tenants.
    /// </summary>
    public bool AllowOrdinalSharing
    {
        get => !_registry.Indexes.Contains(_uniqueOrdinalIndex);
        set
        {
            if (value)
            {
                _registry.Indexes.Remove(_uniqueOrdinalIndex);
            }
            else if (!_registry.Indexes.Contains(_uniqueOrdinalIndex))
            {
                _registry.Indexes.Add(_uniqueOrdinalIndex);
            }
        }
    }

    /// <inheritdoc />
    public string Column { get; }

    /// <inheritdoc />
    public string SqlDataType { get; }

    /// <summary>
    ///     Filegroup that every partition is mapped to. Defaults to PRIMARY.
    /// </summary>
    public string Filegroup { get; set; } = "PRIMARY";

    /// <summary>
    ///     Managed tenant partitioning is always RANGE RIGHT — each ordinal is
    ///     the lower bound of its partition. Exposed for delta detection.
    /// </summary>
    public bool IsRangeRight => true;

    /// <summary>
    ///     The schema-qualified registry table that holds the tenant_id -&gt;
    ///     ordinal map. Provisioned through the <see cref="IFeatureSchema" />
    ///     surface; callers usually don't touch it directly.
    /// </summary>
    public DbObjectName RegistryTableIdentifier => _registry.Identifier;

    /// <inheritdoc />
    public IReadOnlyDictionary<string, int> Ordinals =>
        new ReadOnlyDictionary<string, int>(_ordinals);

    /// <inheritdoc />
    protected override IEnumerable<ISchemaObject> schemaObjects()
    {
        yield return _registry;
    }

    // ---------------------------------------------------------------------
    // ISqlServerPartitioning
    // ---------------------------------------------------------------------

    /// <inheritdoc />
    public string PartitionFunctionName(Table parent)
        => $"pf_{parent.Identifier.Name}_{Column}";

    /// <inheritdoc />
    public string PartitionSchemeName(Table parent)
        => $"ps_{parent.Identifier.Name}_{Column}";

    /// <inheritdoc />
    public void WritePartitionDdl(TextWriter writer, Table parent)
    {
        var pfName = PartitionFunctionName(parent);
        var psName = PartitionSchemeName(parent);

        // Drop existing for idempotent creation (matches RangePartitioning).
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{psName}')");
        writer.WriteLine($"    DROP PARTITION SCHEME [{psName}];");
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{pfName}')");
        writer.WriteLine($"    DROP PARTITION FUNCTION [{pfName}];");

        var boundaries = OrderedBoundaries();

        writer.Write($"CREATE PARTITION FUNCTION [{pfName}] ({SqlDataType}) AS RANGE RIGHT");
        if (boundaries.Length > 0)
        {
            writer.Write(" FOR VALUES (");
            writer.Write(boundaries.Select(x => x.ToString()).Join(", "));
            writer.Write(")");
        }

        writer.WriteLine(";");
        writer.WriteLine(
            $"CREATE PARTITION SCHEME [{psName}] AS PARTITION [{pfName}] ALL TO ([{Filegroup}]);");
    }

    /// <inheritdoc />
    public void WriteOnClause(TextWriter writer, Table parent)
    {
        writer.Write($" ON [{PartitionSchemeName(parent)}]([{Column}])");
    }

    /// <inheritdoc />
    public void WriteDropDdl(TextWriter writer, Table parent)
    {
        var psName = PartitionSchemeName(parent);
        var pfName = PartitionFunctionName(parent);

        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{psName}')");
        writer.WriteLine($"    DROP PARTITION SCHEME [{psName}];");
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{pfName}')");
        writer.WriteLine($"    DROP PARTITION FUNCTION [{pfName}];");
    }

    /// <inheritdoc />
    public PartitionDelta CreateDelta(SqlServerPartitionInfo? actual)
    {
        if (actual == null)
        {
            return PartitionDelta.Rebuild;
        }

        if (!actual.Column.EqualsIgnoreCase(Column))
        {
            return PartitionDelta.Rebuild;
        }

        if (!actual.SqlDataType.EqualsIgnoreCase(SqlDataType))
        {
            return PartitionDelta.Rebuild;
        }

        if (actual.IsRangeRight != IsRangeRight)
        {
            return PartitionDelta.Rebuild;
        }

        var expected = new HashSet<string>(
            OrderedBoundaries().Select(x => x.ToString()),
            StringComparer.OrdinalIgnoreCase);
        var actualSet = new HashSet<string>(actual.BoundaryValues, StringComparer.OrdinalIgnoreCase);

        if (actualSet.SetEquals(expected))
        {
            return PartitionDelta.None;
        }

        // Managed strategy is purely additive at the boundary level: ordinals are
        // allocated monotonically and never recycled. If the actual database has
        // boundaries we don't expect, that's a rebuild — somebody hand-modified
        // the partition function out from under us.
        if (expected.IsSupersetOf(actualSet))
        {
            return PartitionDelta.Additive;
        }

        return PartitionDelta.Rebuild;
    }

    /// <summary>
    ///     Emit <c>ALTER PARTITION FUNCTION ... SPLIT RANGE</c> for every boundary
    ///     present in the in-memory map but missing from the database. Used by
    ///     <see cref="TableDelta" /> when the partition delta is
    ///     <see cref="PartitionDelta.Additive" />.
    /// </summary>
    public void WriteSplitStatements(TextWriter writer, Table parent, SqlServerPartitionInfo actual)
    {
        var pfName = PartitionFunctionName(parent);
        var psName = PartitionSchemeName(parent);
        var actualSet = new HashSet<string>(actual.BoundaryValues, StringComparer.OrdinalIgnoreCase);

        foreach (var ordinal in OrderedBoundaries())
        {
            var boundary = ordinal.ToString();
            if (actualSet.Contains(boundary))
            {
                continue;
            }

            writer.WriteLine($"ALTER PARTITION SCHEME [{psName}] NEXT USED [{Filegroup}];");
            writer.WriteLine($"ALTER PARTITION FUNCTION [{pfName}]() SPLIT RANGE ({boundary});");
        }
    }

    /// <summary>
    ///     Sentinel boundary value that is always present in every partition
    ///     function this strategy creates, regardless of how many tenants are
    ///     registered. SQL Server rejects a CREATE PARTITION FUNCTION with no
    ///     FOR VALUES clause; we keep <c>0</c> as the floor and allocate real
    ///     tenant ordinals from <c>1</c> upward. Delta detection treats the
    ///     sentinel as part of the expected boundary set so it round-trips
    ///     against <c>sys.partition_range_values</c>; <c>MERGE RANGE</c> never
    ///     touches it because <c>_ordinals</c> only holds allocated tenants.
    /// </summary>
    internal const int SentinelBoundary = 0;

    private int[] OrderedBoundaries()
    {
        var values = new SortedSet<int> { SentinelBoundary };
        foreach (var ordinal in _ordinals.Values)
        {
            values.Add(ordinal);
        }

        return values.ToArray();
    }

    // ---------------------------------------------------------------------
    // Runtime API — mirrors PG ManagedListPartitions
    // ---------------------------------------------------------------------

    /// <summary>
    ///     Provision the registry table (idempotent) and load the existing
    ///     tenant_id -&gt; ordinal map into memory. Safe to call multiple times;
    ///     subsequent calls are no-ops until <see cref="ForceReload" /> is called.
    /// </summary>
    public async Task InitializeAsync(SqlConnection conn, CancellationToken token)
    {
        if (_hasInitialized)
        {
            return;
        }

        await _semaphore.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_hasInitialized)
            {
                return;
            }

            // Ensure the registry table itself exists. This is idempotent: when
            // the table is already present FetchExistingAsync returns the same
            // structure and MigrateAsync is a no-op.
            await _registry.MigrateAsync(conn, token).ConfigureAwait(false);

            _ordinals.Clear();

            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"SELECT tenant_id, ordinal FROM {_registry.Identifier.QualifiedName}";
            await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                var tenantId = reader.GetString(0);
                var ordinal = reader.GetInt32(1);
                _ordinals[tenantId] = ordinal;
            }

            _hasInitialized = true;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    ///     Resets the in-memory cache so the next <see cref="InitializeAsync(SqlConnection,CancellationToken)" /> call
    ///     re-reads from the database. Useful when out-of-process actors have
    ///     mutated the registry table.
    /// </summary>
    public void ForceReload()
    {
        _hasInitialized = false;
    }

    /// <summary>
    ///     Open a connection, ensure the registry exists, and load tenant
    ///     mappings.
    /// </summary>
    public async Task InitializeAsync(IDatabase<SqlConnection> database, CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);
        await InitializeAsync(conn, token).ConfigureAwait(false);
        await conn.CloseAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     Register a tenant, allocate an ordinal if necessary, persist the
    ///     mapping, and SPLIT RANGE every partitioned table that uses this
    ///     strategy. Returns the ordinal assigned to the tenant. Idempotent —
    ///     repeating with the same tenant_id returns the previously assigned
    ///     ordinal and emits no DDL.
    /// </summary>
    public async Task<int> AddPartitionToAllTables(
        IDatabase<SqlConnection> database,
        string tenantId,
        CancellationToken token)
    {
        return await AddPartitionToAllTables(NullLogger.Instance, database, tenantId, token)
            .ConfigureAwait(false);
    }

    /// <summary>
    ///     Same as <see cref="AddPartitionToAllTables(IDatabase{SqlConnection},string,CancellationToken)" />
    ///     but with logging for individual table migrations.
    /// </summary>
    public async Task<int> AddPartitionToAllTables(
        ILogger logger,
        IDatabase<SqlConnection> database,
        string tenantId,
        CancellationToken token)
    {
        if (string.IsNullOrEmpty(tenantId))
        {
            throw new ArgumentException("tenantId must not be null or empty", nameof(tenantId));
        }

        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        await InitializeAsync(conn, token).ConfigureAwait(false);

        var ordinal = await upsertTenantAsync(conn, tenantId, token).ConfigureAwait(false);

        await splitTablesForNewOrdinalsAsync(logger, database, conn, token).ConfigureAwait(false);

        await conn.CloseAsync().ConfigureAwait(false);

        return ordinal;
    }

    /// <summary>
    ///     Register multiple tenants at once, persist their ordinals, and SPLIT
    ///     RANGE every partitioned table. Returns the assigned ordinal for each
    ///     tenant. Use <see cref="AddPartitionsToAllTables(ILogger,IDatabase{SqlConnection},IEnumerable{string},CancellationToken)" />
    ///     when the per-table migration statuses are needed as well.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, int>> AddPartitionToAllTables(
        ILogger logger,
        IDatabase<SqlConnection> database,
        IEnumerable<string> tenantIds,
        CancellationToken token)
    {
        var result = await AddPartitionsToAllTables(logger, database, tenantIds, token)
            .ConfigureAwait(false);
        return result.Ordinals;
    }

    /// <summary>
    ///     Register multiple tenants at once, persist their ordinals, and SPLIT
    ///     RANGE every partitioned table. Returns both the assigned ordinals and
    ///     the per-table migration statuses (parity with the PostgreSQL
    ///     <c>ManagedListPartitions</c> batch add) so callers can surface
    ///     partial failures.
    /// </summary>
    public async Task<TenantPartitionAddResult> AddPartitionsToAllTables(
        ILogger logger,
        IDatabase<SqlConnection> database,
        IEnumerable<string> tenantIds,
        CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        await InitializeAsync(conn, token).ConfigureAwait(false);

        var assigned = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var tenantId in tenantIds)
        {
            if (string.IsNullOrEmpty(tenantId))
            {
                continue;
            }

            assigned[tenantId] =
                await upsertTenantAsync(conn, tenantId, token).ConfigureAwait(false);
        }

        var statuses = await splitTablesForNewOrdinalsAsync(logger, database, conn, token)
            .ConfigureAwait(false);

        await conn.CloseAsync().ConfigureAwait(false);

        return new TenantPartitionAddResult(assigned, statuses.ToArray());
    }

    /// <summary>
    ///     Register tenants with explicitly assigned ordinals — the tenant
    ///     bucketing seam. With <see cref="AllowOrdinalSharing" /> enabled,
    ///     multiple tenant ids may map to the same ordinal so small tenants
    ///     share a partition (and the strategy stays clear of SQL Server's
    ///     15,000-partition ceiling). Re-registering a tenant with its current
    ///     ordinal is a no-op; re-registering with a different ordinal throws,
    ///     because existing rows would keep the old ordinal.
    /// </summary>
    public async Task<TenantPartitionAddResult> AddPartitionsToAllTables(
        ILogger logger,
        IDatabase<SqlConnection> database,
        IReadOnlyDictionary<string, int> tenantOrdinals,
        CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        await InitializeAsync(conn, token).ConfigureAwait(false);

        var assigned = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var pair in tenantOrdinals)
        {
            if (string.IsNullOrEmpty(pair.Key))
            {
                continue;
            }

            assigned[pair.Key] =
                await upsertTenantAsync(conn, pair.Key, token, pair.Value).ConfigureAwait(false);
        }

        var statuses = await splitTablesForNewOrdinalsAsync(logger, database, conn, token)
            .ConfigureAwait(false);

        await conn.CloseAsync().ConfigureAwait(false);

        return new TenantPartitionAddResult(assigned, statuses.ToArray());
    }

    /// <summary>
    ///     Register a single tenant with an explicitly assigned ordinal. See
    ///     <see cref="AddPartitionsToAllTables(ILogger,IDatabase{SqlConnection},IReadOnlyDictionary{string,int},CancellationToken)" />
    ///     for the bucketing semantics.
    /// </summary>
    public async Task<int> AddPartitionToAllTables(
        ILogger logger,
        IDatabase<SqlConnection> database,
        string tenantId,
        int ordinal,
        CancellationToken token)
    {
        if (string.IsNullOrEmpty(tenantId))
        {
            throw new ArgumentException("tenantId must not be null or empty", nameof(tenantId));
        }

        var result = await AddPartitionsToAllTables(logger, database,
                new Dictionary<string, int> { [tenantId] = ordinal }, token)
            .ConfigureAwait(false);
        return result.Ordinals[tenantId];
    }

    /// <summary>
    ///     Back-fill: make sure every table currently wired to this strategy has
    ///     a partition boundary for every registered tenant ordinal. Use this
    ///     after adding a table to an existing managed set — the regular
    ///     <c>TableDelta</c> migration deliberately leaves managed partition
    ///     functions alone, so a table whose partition function pre-dates newer
    ///     tenants needs this explicit reconciliation. Tables (or partition
    ///     functions) that don't exist yet are created via <c>MigrateAsync</c>
    ///     with the full boundary set.
    /// </summary>
    public async Task<TablePartitionStatus[]> MigrateAllTablesAsync(
        ILogger logger,
        IDatabase<SqlConnection> database,
        CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        await InitializeAsync(conn, token).ConfigureAwait(false);

        var statuses = await splitTablesForNewOrdinalsAsync(logger, database, conn, token)
            .ConfigureAwait(false);

        await conn.CloseAsync().ConfigureAwait(false);

        return statuses.ToArray();
    }

    /// <summary>
    ///     Bulk-replace the entire tenant_id -&gt; ordinal map. Use with care —
    ///     this is destructive and does NOT issue SPLIT/MERGE statements;
    ///     existing partitioned tables will be out of sync with the registry
    ///     until a full migration runs.
    /// </summary>
    public async Task ResetValues(
        IDatabase<SqlConnection> database,
        IReadOnlyDictionary<string, int> values,
        CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        await _registry.MigrateAsync(conn, token).ConfigureAwait(false);

        await using (var tx = (SqlTransaction)await conn.BeginTransactionAsync(token)
                         .ConfigureAwait(false))
        {
            await using (var delete = conn.CreateCommand())
            {
                delete.Transaction = tx;
                delete.CommandText = $"DELETE FROM {_registry.Identifier.QualifiedName}";
                await delete.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }

            foreach (var pair in values)
            {
                await using var insert = conn.CreateCommand();
                insert.Transaction = tx;
                insert.CommandText =
                    $"INSERT INTO {_registry.Identifier.QualifiedName} (tenant_id, ordinal) VALUES (@tenant, @ordinal)";
                insert.Parameters.AddWithValue("@tenant", pair.Key);
                insert.Parameters.AddWithValue("@ordinal", pair.Value);
                await insert.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }

            _ordinals.Clear();
            foreach (var pair in values)
            {
                _ordinals[pair.Key] = pair.Value;
            }

            await tx.CommitAsync(token).ConfigureAwait(false);
        }

        await conn.CloseAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     Drop tenants from the registry and MERGE RANGE the corresponding
    ///     boundaries off every partitioned table, retaining the tenants' rows
    ///     (<see cref="TenantDropBehavior.RetainData" />). The opposite of
    ///     <see cref="AddPartitionToAllTables(IDatabase{SqlConnection},string,CancellationToken)" />.
    /// </summary>
    /// <remarks>
    ///     SQL Server's <c>MERGE RANGE</c> only removes the boundary point — the
    ///     data on either side is merged into the resulting partition. Pass
    ///     <see cref="TenantDropBehavior.DeleteData" /> to the behavior overload
    ///     to physically remove the tenants' rows first (PostgreSQL managed-drop
    ///     parity), or delete them yourself before calling this method.
    /// </remarks>
    public Task DropPartitionFromAllTables(
        ILogger logger,
        IDatabase<SqlConnection> database,
        IEnumerable<string> tenantIds,
        CancellationToken token)
    {
        return DropPartitionFromAllTables(logger, database, tenantIds, TenantDropBehavior.RetainData, token);
    }

    /// <summary>
    ///     Drop tenants from the registry and MERGE RANGE the corresponding
    ///     boundaries off every partitioned table. With
    ///     <see cref="TenantDropBehavior.DeleteData" /> the tenants' rows are
    ///     deleted from every managed table before the merge, so removing a
    ///     tenant physically removes its data (parity with the PostgreSQL
    ///     managed drop). An ordinal still referenced by other tenants (see
    ///     <see cref="AllowOrdinalSharing" />) is never merged or purged — the
    ///     remaining tenants keep the partition, and any finer-grained data
    ///     removal belongs to the caller.
    /// </summary>
    public async Task DropPartitionFromAllTables(
        ILogger logger,
        IDatabase<SqlConnection> database,
        IEnumerable<string> tenantIds,
        TenantDropBehavior behavior,
        CancellationToken token)
    {
        var tenants = tenantIds.Where(x => !string.IsNullOrEmpty(x)).Distinct().ToArray();
        if (tenants.Length == 0)
        {
            return;
        }

        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        await InitializeAsync(conn, token).ConfigureAwait(false);

        var droppedOrdinals = new HashSet<int>();
        foreach (var tenantId in tenants)
        {
            if (_ordinals.TryGetValue(tenantId, out var ordinal))
            {
                droppedOrdinals.Add(ordinal);
            }
            else
            {
                logger.LogWarning(
                    "Tenant {TenantId} is not registered with {Registry} — skipping",
                    tenantId, _registry.Identifier.QualifiedName);
            }
        }

        if (droppedOrdinals.Count == 0)
        {
            await conn.CloseAsync().ConfigureAwait(false);
            return;
        }

        // Delete from the registry first so concurrent readers don't see a stale
        // map after the boundaries are gone.
        await using (var delete = conn.CreateCommand())
        {
            var paramNames = new List<string>();
            for (var i = 0; i < tenants.Length; i++)
            {
                var pname = $"@tenant{i}";
                paramNames.Add(pname);
                delete.Parameters.AddWithValue(pname, tenants[i]);
            }

            delete.CommandText =
                $"DELETE FROM {_registry.Identifier.QualifiedName} WHERE tenant_id IN ({paramNames.Join(", ")})";
            await delete.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        foreach (var tenant in tenants)
        {
            _ordinals.Remove(tenant);
        }

        // With ordinal sharing, an ordinal is only released once its LAST tenant
        // is dropped; while other tenants still map to it the partition must
        // survive, and a purge by ordinal would take their rows with it.
        var ordinalsToMerge = new List<int>();
        foreach (var ordinal in droppedOrdinals.OrderBy(x => x))
        {
            if (_ordinals.ContainsValue(ordinal))
            {
                logger.LogWarning(
                    "Ordinal {Ordinal} is still shared by other tenants — the partition is retained and " +
                    "no rows are removed; per-tenant data removal within a shared partition belongs to the caller",
                    ordinal);
                continue;
            }

            ordinalsToMerge.Add(ordinal);
        }

        if (ordinalsToMerge.Count == 0)
        {
            await conn.CloseAsync().ConfigureAwait(false);
            return;
        }

        var tables = ResolveManagedTables(database);

        foreach (var table in tables)
        {
            var pfName = PartitionFunctionName(table);
            foreach (var ordinal in ordinalsToMerge)
            {
                if (behavior == TenantDropBehavior.DeleteData)
                {
                    await using var purge = conn.CreateCommand();
                    purge.CommandText =
                        $"DELETE FROM {table.Identifier.QualifiedName} WHERE [{Column}] = @ordinal;";
                    purge.Parameters.AddWithValue("@ordinal", ordinal);
                    try
                    {
                        var removed = await purge.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                        logger.LogInformation(
                            "Deleted {Count} rows with ordinal {Ordinal} from table {Table}",
                            removed, ordinal, table.Identifier);
                    }
                    catch (SqlException e)
                    {
                        // Leave the boundary in place rather than merging rows we
                        // failed to delete into the neighboring partition.
                        logger.LogError(e,
                            "Could not delete rows with ordinal {Ordinal} from table {Table} — " +
                            "skipping the boundary merge for this table",
                            ordinal, table.Identifier);
                        continue;
                    }
                }

                await using var merge = conn.CreateCommand();
                merge.CommandText =
                    $"ALTER PARTITION FUNCTION [{pfName}]() MERGE RANGE ({ordinal});";
                try
                {
                    await merge.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                    logger.LogInformation(
                        "Merged ordinal {Ordinal} out of partition function {Function} for table {Table}",
                        ordinal, pfName, table.Identifier);
                }
                catch (SqlException e)
                {
                    logger.LogError(e,
                        "Could not merge ordinal {Ordinal} out of partition function {Function} for table {Table}",
                        ordinal, pfName, table.Identifier);
                }
            }
        }

        await conn.CloseAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     Walk every table in <paramref name="database" /> that is configured
    ///     to use this <see cref="ManagedTenantPartitions" /> instance as its
    ///     <see cref="Table.SqlServerPartitioning" /> strategy.
    /// </summary>
    internal IReadOnlyList<Table> ResolveManagedTables(IDatabase database)
    {
        return database.AllObjects()
            .OfType<Table>()
            .Where(t => ReferenceEquals(t.SqlServerPartitioning, this))
            .ToArray();
    }

    // ---------------------------------------------------------------------
    // private — registry mutation + per-table SPLIT
    // ---------------------------------------------------------------------

    private async Task<int> upsertTenantAsync(
        SqlConnection conn,
        string tenantId,
        CancellationToken token,
        int? requestedOrdinal = null)
    {
        if (_ordinals.TryGetValue(tenantId, out var existing))
        {
            if (requestedOrdinal.HasValue && requestedOrdinal.Value != existing)
            {
                throw new InvalidOperationException(
                    $"Tenant '{tenantId}' is already registered with ordinal {existing}; remapping to " +
                    $"{requestedOrdinal.Value} is not supported because existing rows keep the old ordinal. " +
                    "Drop the tenant first if it really has to move.");
            }

            return existing;
        }

        if (requestedOrdinal is <= SentinelBoundary)
        {
            throw new ArgumentOutOfRangeException(nameof(requestedOrdinal),
                $"Tenant ordinals must be positive — {SentinelBoundary} is the sentinel boundary");
        }

        if (requestedOrdinal.HasValue && !AllowOrdinalSharing &&
            _ordinals.ContainsValue(requestedOrdinal.Value))
        {
            throw new InvalidOperationException(
                $"Ordinal {requestedOrdinal.Value} is already assigned to another tenant. Set " +
                $"{nameof(AllowOrdinalSharing)} = true to bucket multiple tenants into one partition.");
        }

        var nextOrdinal = requestedOrdinal ??
                          ((_ordinals.Count == 0) ? 1 : _ordinals.Values.Max() + 1);

        await using var cmd = conn.CreateCommand();
        // MERGE rather than INSERT to handle a race where another process
        // registered the same tenant first. The unique index on ordinal still
        // guards against two tenants getting the same number; if that happens
        // the InvalidOperationException propagates and the caller can retry.
        cmd.CommandText = $@"
MERGE {_registry.Identifier.QualifiedName} WITH (HOLDLOCK) AS target
USING (SELECT @tenant AS tenant_id, @ordinal AS ordinal) AS source
ON target.tenant_id = source.tenant_id
WHEN NOT MATCHED THEN INSERT (tenant_id, ordinal) VALUES (source.tenant_id, source.ordinal)
OUTPUT inserted.ordinal;";
        cmd.Parameters.AddWithValue("@tenant", tenantId);
        cmd.Parameters.AddWithValue("@ordinal", nextOrdinal);

        var insertedOrdinal = (int?)await cmd.ExecuteScalarAsync(token).ConfigureAwait(false);

        // OUTPUT is silent on the matched branch — re-read the tenant if the
        // upsert was a no-op (another process beat us to it).
        if (insertedOrdinal == null)
        {
            await using var select = conn.CreateCommand();
            select.CommandText =
                $"SELECT ordinal FROM {_registry.Identifier.QualifiedName} WHERE tenant_id = @tenant";
            select.Parameters.AddWithValue("@tenant", tenantId);
            insertedOrdinal = (int?)await select.ExecuteScalarAsync(token).ConfigureAwait(false);
        }

        if (insertedOrdinal == null)
        {
            throw new InvalidOperationException(
                $"Failed to register tenant '{tenantId}' in {_registry.Identifier.QualifiedName}");
        }

        _ordinals[tenantId] = insertedOrdinal.Value;
        return insertedOrdinal.Value;
    }

    private async Task<List<TablePartitionStatus>> splitTablesForNewOrdinalsAsync(
        ILogger logger,
        IDatabase<SqlConnection> database,
        SqlConnection conn,
        CancellationToken token)
    {
        var statuses = new List<TablePartitionStatus>();
        var tables = ResolveManagedTables(database);

        // Per-table failure isolation mirrors PG ManagedListPartitions: one
        // table failing to split must not keep the remaining tables from
        // getting the new tenant's partition, and callers get the per-table
        // outcome to surface partial failures.
        foreach (var table in tables)
        {
            try
            {
                var pfName = PartitionFunctionName(table);
                var psName = PartitionSchemeName(table);

                var existing = await fetchActualBoundariesAsync(conn, pfName, token).ConfigureAwait(false);
                if (existing == null)
                {
                    // Function doesn't exist yet — let MigrateAsync create it.
                    logger.LogInformation(
                        "Partition function {Function} for table {Table} not found — running MigrateAsync to create it",
                        pfName, table.Identifier);
                    await table.MigrateAsync(conn, token).ConfigureAwait(false);
                    statuses.Add(new TablePartitionStatus(table.Identifier, PartitionMigrationStatus.Complete));
                    continue;
                }

                foreach (var ordinal in OrderedBoundaries())
                {
                    if (existing.Contains(ordinal.ToString()))
                    {
                        continue;
                    }

                    await using (var nextUsed = conn.CreateCommand())
                    {
                        nextUsed.CommandText =
                            $"ALTER PARTITION SCHEME [{psName}] NEXT USED [{Filegroup}];";
                        await nextUsed.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                    }

                    await using (var split = conn.CreateCommand())
                    {
                        split.CommandText =
                            $"ALTER PARTITION FUNCTION [{pfName}]() SPLIT RANGE ({ordinal});";
                        await split.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                    }

                    logger.LogInformation(
                        "Split partition function {Function} at boundary {Boundary} for table {Table}",
                        pfName, ordinal, table.Identifier);
                }

                statuses.Add(new TablePartitionStatus(table.Identifier, PartitionMigrationStatus.Complete));
            }
            catch (Exception e)
            {
                logger.LogError(e, "Error trying to add table partitions to {Table}", table.Identifier);
                statuses.Add(new TablePartitionStatus(table.Identifier, PartitionMigrationStatus.Failed));
            }
        }

        return statuses;
    }

    private static async Task<HashSet<string>?> fetchActualBoundariesAsync(
        SqlConnection conn,
        string pfName,
        CancellationToken token)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
SELECT CAST(prv.value AS NVARCHAR(MAX))
FROM sys.partition_functions pf
JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id
WHERE pf.name = @pf
ORDER BY prv.boundary_id;";
        cmd.Parameters.AddWithValue("@pf", pfName);

        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

        // sys.partition_functions has no row for a function that doesn't exist;
        // distinguish "no function" from "function with zero boundaries" by
        // checking sys.partition_functions separately.
        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            await reader.CloseAsync().ConfigureAwait(false);
            return await partitionFunctionExistsAsync(conn, pfName, token).ConfigureAwait(false)
                ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                : null;
        }

        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            reader.GetString(0)
        };
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            set.Add(reader.GetString(0));
        }

        return set;
    }

    private static async Task<bool> partitionFunctionExistsAsync(
        SqlConnection conn,
        string pfName,
        CancellationToken token)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sys.partition_functions WHERE name = @pf";
        cmd.Parameters.AddWithValue("@pf", pfName);
        var count = (int?)await cmd.ExecuteScalarAsync(token).ConfigureAwait(false) ?? 0;
        return count > 0;
    }
}

/// <summary>
///     What happens to a tenant's rows when its partition is dropped via
///     <see cref="ManagedTenantPartitions.DropPartitionFromAllTables(ILogger,IDatabase{SqlConnection},IEnumerable{string},TenantDropBehavior,CancellationToken)" />.
/// </summary>
public enum TenantDropBehavior
{
    /// <summary>
    ///     Only the partition boundary is removed (<c>MERGE RANGE</c>) — SQL
    ///     Server merges the tenant's rows into the neighboring partition. The
    ///     caller owns any data purge. This is the historical behavior and the
    ///     default.
    /// </summary>
    RetainData,

    /// <summary>
    ///     The tenant's rows are deleted from every managed table before the
    ///     boundary is merged, mirroring the PostgreSQL managed drop
    ///     (<c>DETACH PARTITION</c> + <c>DROP TABLE</c>) where the tenant's
    ///     rows are physically removed. Needed for hard tenant deletion.
    /// </summary>
    DeleteData
}

/// <summary>
///     Per-table outcome of a partition add / back-fill operation. Mirrors the
///     PostgreSQL <c>ManagedListPartitions</c> status reporting so callers
///     (Wolverine, Polecat, CritterWatch) can surface partial failures.
/// </summary>
public enum PartitionMigrationStatus
{
    Complete,
    Failed,
    RequiresTableRebuild
}

/// <summary>Per-table status of a partition add / back-fill operation.</summary>
public record TablePartitionStatus(DbObjectName Identifier, PartitionMigrationStatus Status);

/// <summary>
///     Result of a batch tenant registration: the tenant_id -&gt; ordinal map
///     that was assigned plus the per-table migration statuses.
/// </summary>
public record TenantPartitionAddResult(
    IReadOnlyDictionary<string, int> Ordinals,
    TablePartitionStatus[] Tables);
