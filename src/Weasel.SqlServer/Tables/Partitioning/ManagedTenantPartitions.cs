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
        var uniqueOrdinal =
            new IndexDefinition($"uniq_{registryTableName.Name}_ordinal") { IsUnique = true };
        uniqueOrdinal.AgainstColumns("ordinal");
        _registry.Indexes.Add(uniqueOrdinal);
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
    ///     tenant.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, int>> AddPartitionToAllTables(
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

        await splitTablesForNewOrdinalsAsync(logger, database, conn, token).ConfigureAwait(false);

        await conn.CloseAsync().ConfigureAwait(false);

        return assigned;
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
    ///     boundaries off every partitioned table. The opposite of
    ///     <see cref="AddPartitionToAllTables(IDatabase{SqlConnection},string,CancellationToken)" />.
    /// </summary>
    /// <remarks>
    ///     SQL Server's <c>MERGE RANGE</c> only removes the boundary point — the
    ///     data on either side is merged into the resulting partition. If you
    ///     need to delete a tenant's rows first do that as a separate step
    ///     before calling this method.
    /// </remarks>
    public async Task DropPartitionFromAllTables(
        ILogger logger,
        IDatabase<SqlConnection> database,
        IEnumerable<string> tenantIds,
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

        var ordinalsToMerge = new List<int>();
        foreach (var tenantId in tenants)
        {
            if (_ordinals.TryGetValue(tenantId, out var ordinal))
            {
                ordinalsToMerge.Add(ordinal);
            }
            else
            {
                logger.LogWarning(
                    "Tenant {TenantId} is not registered with {Registry} — skipping",
                    tenantId, _registry.Identifier.QualifiedName);
            }
        }

        if (ordinalsToMerge.Count == 0)
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

        var tables = ResolveManagedTables(database);

        foreach (var table in tables)
        {
            var pfName = PartitionFunctionName(table);
            foreach (var ordinal in ordinalsToMerge)
            {
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
        CancellationToken token)
    {
        if (_ordinals.TryGetValue(tenantId, out var existing))
        {
            return existing;
        }

        var nextOrdinal = (_ordinals.Count == 0) ? 1 : _ordinals.Values.Max() + 1;

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

    private async Task splitTablesForNewOrdinalsAsync(
        ILogger logger,
        IDatabase<SqlConnection> database,
        SqlConnection conn,
        CancellationToken token)
    {
        var tables = ResolveManagedTables(database);

        foreach (var table in tables)
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
        }
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
