#nullable enable
using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using JasperFx;
using JasperFx.Descriptors;
using JasperFx.MultiTenancy;

namespace Weasel.Core.MultiTenancy;

/// <summary>
/// Dynamic database-per-tenant driven by a control table mapping <c>tenant_id</c> to a connection
/// string, with tenants added, disabled, enabled and removed at runtime rather than fixed at
/// registration. The runtime Marten's <c>MasterTableTenancy</c> (515 lines) and Polecat's (434)
/// each carried a copy of — weasel#567.
/// </summary>
/// <typeparam name="TDatabase">
/// The store's own database type. Left unconstrained on purpose: the three stores' database types
/// share no interface Weasel could name here, and the lift does not need one — everything this
/// class does with a <typeparamref name="TDatabase" /> is cache it, hand it back, and dispose it
/// through <see cref="DisposeDatabaseAsync" />.
/// </typeparam>
/// <typeparam name="TDataSource">
/// The provider's data source, which is what makes <see cref="MasterTableTenancyOptions{T}" />
/// usable from here. Marten already closes that type over <c>NpgsqlDataSource</c>; Polecat
/// re-implemented the same options inline and never touched the shared type at all, which is the
/// asymmetry this lift exists to close.
/// </typeparam>
/// <remarks>
/// <para>
/// <b>What is here and what is not.</b> The base owns the control-table contract, the resolved
/// database cache, provisioning the table exactly once, and the whole
/// <see cref="IDynamicTenantSource{T}" /> lifecycle — which in both stores today is ~80 lines of
/// adapter forwarding to the store's own cancellable methods. The dialect owns every SQL string
/// (<see cref="IMasterTenantTableDialect" />) and the store owns building a database from a
/// connection string.
/// </para>
/// <para>
/// <b><see cref="ITenancy" /> is deliberately not touched.</b> All three stores declare their own,
/// differing only in the concrete database type, and an <c>ITenancy&lt;TDatabase&gt;</c> would
/// unify them — but it is a breaking change to three public API surfaces and this class does not
/// need it, being generic over its database type already. A store adopting this base implements
/// its own <c>ITenancy</c> on top and forwards.
/// </para>
/// </remarks>
public abstract class MasterTableTenancyBase<TDatabase, TDataSource>:
    IMasterTableMultiTenancy, IDynamicTenantSource<string>, IDisposable, IAsyncDisposable
    where TDatabase : class
    where TDataSource : DbDataSource
{
    private readonly ConcurrentDictionary<string, TenantEntry> _databases;
    private readonly Func<TDataSource> _dataSourceFactory;
    private readonly Lock _dataSourceLock = new();
    private readonly bool _ownsDataSource;
    private readonly SemaphoreSlim _provisioning = new(1, 1);

    private TDataSource? _dataSource;

    private volatile bool _controlTableProvisioned;
    private volatile bool _seeded;

    // weasel#838: set once any entry point's ProvisionControlTableAsync -- the store's override
    // included -- has returned, so TryResolveTenant knows the table is there without provisioning
    // it, which it cannot do synchronously (Marten provisions through its own async migration).
    // weasel#850: or once TryProvisionControlTableSynchronously has returned true. It only tells the
    // synchronous path it may read; the asynchronous entry points provision as they always have.
    private volatile bool _controlTableReady;

    // weasel#850: serializes TryProvisionControlTableSynchronously against itself. A Lock rather than
    // _provisioning, because a thread waiting here waits on synchronous I/O another thread is doing,
    // which needs no pool thread to finish; waiting on _provisioning could be waiting on an await.
    private readonly Lock _synchronousProvisioningLock = new();

    // weasel#850: set when TryProvisionControlTableSynchronously threw NotSupportedException, so a
    // burst does not throw and catch it once per call. Reads stay eligible once an asynchronous entry
    // point has provisioned the table.
    private volatile bool _synchronousProvisioningRefused;

    // weasel#838: whether the control-plane data source takes synchronous I/O. Unknown until first
    // asked; Refused once it has refused (Npgsql with Multiplexing=true). An int written with
    // Interlocked rather than a bool?: a bool? is two fields, and a caller racing the first one to ask
    // could read it half-written, HasValue set with Value still false, and decline a tenant it
    // should have resolved (weasel#907's gate caught a_burst_of_cold_callers doing it).
    private const int SynchronousReadsUnknown = 0;
    private const int SynchronousReadsSupported = 1;
    private const int SynchronousReadsRefused = 2;
    private int _synchronousReads = SynchronousReadsUnknown;

    /// <param name="options">
    /// The shared options — master connection string or data source, schema name, application name,
    /// <see cref="MasterTableTenancyOptions{T}.AutoCreate" /> override and seed databases.
    /// </param>
    /// <param name="tableName">
    /// The control table's unqualified name. The stores each pick their own prefixed spelling
    /// (<c>mt_tenant_databases</c>, <c>pc_tenants</c>) and neither should change, because a rename
    /// is a migration of a table an operator may already be populating by hand.
    /// </param>
    /// <param name="dialect">Renders this store's control-table SQL.</param>
    /// <param name="tenantIdComparer">
    /// How tenant ids are compared in the cache. Defaults to
    /// <see cref="StringComparer.Ordinal" />, which is the conservative choice: a store folding
    /// case should do it in <see cref="CorrectTenantId" /> so the folded id is what reaches the
    /// database as well as the cache, rather than having the two disagree.
    /// </param>
    protected MasterTableTenancyBase(
        MasterTableTenancyOptions<TDataSource> options,
        string tableName,
        IMasterTenantTableDialect dialect,
        IEqualityComparer<string>? tenantIdComparer = null)
    {
        Options = options ?? throw new ArgumentNullException(nameof(options));
        Dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        TableName = tableName;
        QualifiedTableName = dialect.QualifiedTableName(options.SchemaName, tableName);

        _databases = new ConcurrentDictionary<string, TenantEntry>(tenantIdComparer ?? StringComparer.Ordinal);

        if (options.DataSource is not null)
        {
            // Somebody else built it, so somebody else disposes it. Disposing a caller-supplied
            // data source is the kind of thing that only shows up as an ObjectDisposedException in
            // an unrelated part of an application.
            var supplied = options.DataSource;
            _ownsDataSource = false;
            _dataSourceFactory = () => supplied;
        }
        else if (!string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            _ownsDataSource = true;
            _dataSourceFactory = () => BuildDataSource(options.CorrectedConnectionString());
        }
        else
        {
            throw new ArgumentOutOfRangeException(nameof(options),
                $"Master table tenancy needs either a {typeof(TDataSource).Name} or a connection string for the control-plane database.");
        }
    }

    /// <summary>The shared options this tenancy was configured with.</summary>
    public MasterTableTenancyOptions<TDataSource> Options { get; }

    /// <summary>The control table's unqualified name.</summary>
    public string TableName { get; }

    /// <summary>The control table's name as this dialect qualifies it.</summary>
    public string QualifiedTableName { get; }

    /// <summary>The SQL seam.</summary>
    protected IMasterTenantTableDialect Dialect { get; }

    /// <summary>The control-plane data source. Built lazily; never opened by the constructor.</summary>
    /// <remarks>
    /// Lazy by hand rather than through <see cref="Lazy{T}" />, which would need
    /// <typeparamref name="TDataSource" /> annotated for trimming and push that annotation onto
    /// every store closing the generic.
    /// </remarks>
    protected TDataSource MasterDataSource
    {
        get
        {
            if (_dataSource is not null)
            {
                return _dataSource;
            }

            lock (_dataSourceLock)
            {
                return _dataSource ??= _dataSourceFactory();
            }
        }
    }

    /// <summary>
    /// Always <see cref="DatabaseCardinality.DynamicMultiple" /> — the tenant set is read from the
    /// control table and changes while the store runs.
    /// </summary>
    public DatabaseCardinality Cardinality => DatabaseCardinality.DynamicMultiple;

    /// <summary>
    /// Whether the control table may be created at runtime. Defaults to the options' override, and
    /// to <see cref="AutoCreate.CreateOrUpdate" /> when it names none; override to fall back to the
    /// store's own schema policy instead.
    /// </summary>
    /// <remarks>
    /// <see cref="AutoCreate.None" /> has to be honoured here, not merely respected elsewhere: it
    /// says the schema is the operator's, and the control table is exactly the object a
    /// least-privilege deployment provisions by hand. A read against a table that is genuinely
    /// missing then surfaces as the database's own clean "no such table" error.
    /// </remarks>
    protected virtual AutoCreate EffectiveAutoCreate => Options.AutoCreate ?? AutoCreate.CreateOrUpdate;

    /// <summary>Build the control-plane data source from a connection string.</summary>
    protected abstract TDataSource BuildDataSource(string connectionString);

    /// <summary>Build a tenant database from its connection string.</summary>
    /// <remarks>
    /// The connection string reaching here has already been through
    /// <see cref="MasterTableTenancyOptions{T}.CorrectConnectionString" />, so the application name
    /// is on it — which is the whole reason a store wants the shared options rather than its own.
    /// </remarks>
    protected abstract TDatabase BuildDatabase(string tenantId, string connectionString);

    /// <summary>
    /// Normalize a tenant id before it is used as a cache key or bound as a parameter. Defaults to
    /// leaving it alone; a store with a <see cref="TenantIdStyle" /> applies it here.
    /// </summary>
    /// <remarks>
    /// One place, applied on every public entry point, because the failure mode of missing one is
    /// a tenant that resolves through one method and not another — which reads as an intermittent.
    /// </remarks>
    protected virtual string CorrectTenantId(string tenantId) => tenantId;

    /// <summary>
    /// Dispose a tenant database evicted from the cache, asynchronously. The default routes an
    /// <see cref="IAsyncDisposable" /> to its async disposal and everything else to
    /// <see cref="DisposeDatabase" />.
    /// </summary>
    /// <remarks>
    /// Preferring the async form matters where a store's database owns a data source: a
    /// <see cref="DbDataSource" /> closes pooled connections on async disposal and blocks on sync
    /// disposal, and this runs on the path a tenant is disabled or removed on.
    /// </remarks>
    protected virtual ValueTask DisposeDatabaseAsync(TDatabase database)
    {
        if (database is IAsyncDisposable asyncDisposable)
        {
            return asyncDisposable.DisposeAsync();
        }

        DisposeDatabase(database);
        return default;
    }

    /// <summary>
    /// Dispose a tenant database synchronously, for <see cref="Dispose" />. The default disposes an
    /// <see cref="IDisposable" /> and ignores anything else.
    /// </summary>
    protected virtual void DisposeDatabase(TDatabase database) => (database as IDisposable)?.Dispose();

    /// <summary>
    /// Run one control-plane operation. The default just runs it; override to wrap every database
    /// call in the store's resilience pipeline, as Polecat does.
    /// </summary>
    protected virtual Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken token)
        => operation(token);

    /// <summary>Run one control-plane operation with no result.</summary>
    protected async Task ExecuteAsync(Func<CancellationToken, Task> operation, CancellationToken token) =>
        await ExecuteAsync<object?>(async ct =>
        {
            await operation(ct).ConfigureAwait(false);
            return null;
        }, token).ConfigureAwait(false);

    // ------------------------------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------------------------------

    /// <summary>
    /// Read every enabled tenant from the control table, bring the cache into line with it, and
    /// return the databases this tenancy now knows about.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Idempotent: a tenant already cached under the same connection string keeps its existing
    /// database rather than getting a new one, so calling this on a schedule does not churn
    /// connection pools. A tenant whose connection string has <em>changed</em> is rebuilt, and the
    /// database it replaces is disposed.
    /// </para>
    /// <para>
    /// <b>A cached tenant the table no longer offers is evicted</b> (weasel#809), through
    /// <see cref="EvictStaleTenantAsync" />: one whose row was deleted, or disabled, by another
    /// process or node. This used to only add, so such a tenant stayed cached here — and went on
    /// resolving — until the process restarted; that is the bug Polecat fixed on its own in
    /// polecat#871 / polecat#849. Disabling or removing a tenant through <em>this</em> instance
    /// still evicts immediately; this is how every other instance finds out, the next time it reads
    /// the table.
    /// </para>
    /// <para>
    /// Only tenants cached <em>before</em> the read are candidates, and only while their cache
    /// entry is still the one that was there then — so a tenant added or resolved on this instance
    /// while the read was in flight is not evicted on the strength of a read that could not have
    /// seen it.
    /// </para>
    /// </remarks>
    public async Task<IReadOnlyList<TDatabase>> BuildDatabasesAsync(CancellationToken token = default)
    {
        await ensureControlTableAsync(token).ConfigureAwait(false);
        await SeedDatabasesAsync(token).ConfigureAwait(false);

        // Taken before the read: anything cached after it may be newer than what the read sees.
        var cachedBeforeRead = _databases.ToArray();

        var rows = await ExecuteAsync(async ct =>
        {
            var list = new List<(string TenantId, string ConnectionString)>();

            await using var conn = await MasterDataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = CreateCommand(conn, Dialect.SelectEnabledTenants(QualifiedTableName));
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                list.Add((reader.GetString(0), reader.GetString(1)));
            }

            await reader.CloseAsync().ConfigureAwait(false);
            return list;
        }, token).ConfigureAwait(false);

        var enabled = new HashSet<string>(_databases.Comparer);

        foreach (var (rawTenantId, rawConnectionString) in rows)
        {
            var tenantId = CorrectTenantId(rawTenantId);
            var connectionString = Options.CorrectConnectionString(rawConnectionString);
            enabled.Add(tenantId);

            var replaced = default(TDatabase);

            _databases.AddOrUpdate(tenantId,
                id => new TenantEntry(connectionString, BuildDatabase(id, connectionString)),
                (id, existing) =>
                {
                    if (string.Equals(existing.ConnectionString, connectionString, StringComparison.Ordinal))
                    {
                        return existing;
                    }

                    replaced = existing.Database;
                    return new TenantEntry(connectionString, BuildDatabase(id, connectionString));
                });

            if (replaced is not null)
            {
                await DisposeDatabaseAsync(replaced).ConfigureAwait(false);
            }
        }

        foreach (var (tenantId, entry) in cachedBeforeRead)
        {
            if (enabled.Contains(tenantId))
            {
                continue;
            }

            // Re-checked against the live cache: an entry replaced since the snapshot is newer than
            // the read, and is left alone.
            if (_databases.TryGetValue(tenantId, out var current) && ReferenceEquals(current, entry))
            {
                await EvictStaleTenantAsync(tenantId, entry.Database, token).ConfigureAwait(false);
            }
        }

        return AllDatabases();
    }

    /// <summary>
    /// Called by <see cref="BuildDatabasesAsync" /> for each cached tenant the control table no
    /// longer offers as enabled — its row deleted, or disabled, since this instance cached it. The
    /// default evicts it from the cache and disposes its database through
    /// <see cref="DisposeDatabaseAsync" />.
    /// </summary>
    /// <param name="tenantId">The tenant id, as cached (already through <see cref="CorrectTenantId" />).</param>
    /// <param name="database">The tenant's cached database.</param>
    /// <param name="token">The token <see cref="BuildDatabasesAsync" /> was called with.</param>
    /// <remarks>
    /// <para>
    /// Override to do something alongside the eviction — stop the tenant's projection agents, or tell
    /// a second cache the store keeps (Fisher's <c>OnTenantRevoked</c>) — and call the base to evict.
    /// Removed and disabled are not told apart here, because the read that found the tenant missing
    /// was of enabled rows only; <see cref="LookupTenantAsync" /> says which, at the cost of a read.
    /// </para>
    /// <para>
    /// An override that does not call the base keeps the tenant cached, which is the behaviour before
    /// weasel#809 — a tenant disabled or removed elsewhere goes on resolving here.
    /// </para>
    /// <para>
    /// The base's eviction is conditional: it removes the cache entry only if it still holds
    /// <paramref name="database" />, so a tenant re-added on this instance in the meantime survives.
    /// </para>
    /// </remarks>
    protected virtual async ValueTask EvictStaleTenantAsync(string tenantId, TDatabase database, CancellationToken token)
    {
        if (_databases.TryGetValue(tenantId, out var entry)
            && ReferenceEquals(entry.Database, database)
            && _databases.TryRemove(new KeyValuePair<string, TenantEntry>(tenantId, entry)))
        {
            await DisposeDatabaseAsync(database).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Evict one tenant from the cache and dispose its database through
    /// <see cref="DisposeDatabaseAsync" />, without touching the control table. Returns whether
    /// anything was cached for it.
    /// </summary>
    /// <remarks>
    /// The tenant resolves again on next access if its row is still there and enabled — this is a
    /// cache operation, not a lifecycle one. To stop a tenant resolving, disable or delete it.
    /// </remarks>
    protected ValueTask<bool> EvictTenantAsync(string tenantId) => EvictAsync(CorrectTenantId(tenantId));

    /// <summary>Every tenant database currently cached.</summary>
    public IReadOnlyList<TDatabase> AllDatabases() =>
        _databases.Values.Select(x => x.Database).ToList();

    /// <summary>Every tenant id currently cached.</summary>
    public IReadOnlyList<string> AllTenantIds() => _databases.Keys.ToList();

    /// <summary>
    /// Resolve a tenant's database, reading the control table when it is not cached. Returns null
    /// for a tenant that is unknown or disabled.
    /// </summary>
    /// <remarks>
    /// Unknown and disabled are deliberately the same answer <em>here</em> — a disabled tenant is
    /// not routable, which is what disabling means. A caller that needs to tell them apart, to
    /// refuse a disabled tenant with <c>DisabledTenantException</c> as Marten, Polecat and Fisher do,
    /// uses <see cref="ResolveTenantAsync" /> instead, and <see cref="CreateTenantRefusal" /> for
    /// the exception.
    /// </remarks>
    public async ValueTask<TDatabase?> TryFindDatabaseAsync(string tenantId, CancellationToken token = default)
    {
        tenantId = CorrectTenantId(tenantId);

        if (_databases.TryGetValue(tenantId, out var cached))
        {
            return cached.Database;
        }

        var connectionString = await LookupConnectionStringAsync(tenantId, token).ConfigureAwait(false);
        if (connectionString is null)
        {
            return null;
        }

        return _databases.GetOrAdd(tenantId,
            id => new TenantEntry(connectionString, BuildDatabase(id, connectionString))).Database;
    }

    /// <summary>The cached database for a tenant, without touching the control table.</summary>
    public TDatabase? FindCachedDatabase(string tenantId) =>
        _databases.TryGetValue(CorrectTenantId(tenantId), out var entry) ? entry.Database : null;

    /// <summary>
    /// The connection string recorded for one enabled tenant, with the application name applied,
    /// or null when the tenant is unknown or disabled.
    /// </summary>
    public async Task<string?> LookupConnectionStringAsync(string tenantId, CancellationToken token = default)
    {
        tenantId = CorrectTenantId(tenantId);
        await ensureControlTableAsync(token).ConfigureAwait(false);

        var connectionString = await ExecuteAsync(async ct =>
        {
            await using var conn = await MasterDataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = CreateCommand(conn, Dialect.SelectConnectionString(QualifiedTableName),
                ("@id", tenantId));

            return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
        }, token).ConfigureAwait(false);

        return string.IsNullOrEmpty(connectionString)
            ? null
            : Options.CorrectConnectionString(connectionString);
    }

    /// <summary>
    /// Read one tenant's row from the control table, bypassing the cache, and say whether it is
    /// unknown, disabled or enabled — with its connection string (application name applied) only
    /// when enabled.
    /// </summary>
    /// <remarks>
    /// One round trip through <see cref="IMasterTenantTableDialect.SelectTenantRow" />. A dialect
    /// that does not render it costs a second read on a miss: the enabled-only
    /// <see cref="IMasterTenantTableDialect.SelectConnectionString" /> first, then
    /// <see cref="IMasterTenantTableDialect.SelectDisabledTenantIds" /> to tell disabled from unknown.
    /// A row with an empty connection string is reported as unknown, as it always has been.
    /// </remarks>
    public async Task<MasterTenantLookup> LookupTenantAsync(string tenantId, CancellationToken token = default)
    {
        tenantId = CorrectTenantId(tenantId);
        await ensureControlTableAsync(token).ConfigureAwait(false);

        var sql = Dialect.SelectTenantRow(QualifiedTableName);
        if (sql is null)
        {
            return await LookupTenantWithoutRowReadAsync(tenantId, token).ConfigureAwait(false);
        }

        var (found, connectionString, disabled) = await ExecuteAsync(async ct =>
        {
            await using var conn = await MasterDataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = CreateCommand(conn, sql, ("@id", tenantId));
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return (false, (string?)null, false);
            }

            var value = await reader.IsDBNullAsync(0, ct).ConfigureAwait(false) ? null : reader.GetString(0);
            var flag = !await reader.IsDBNullAsync(1, ct).ConfigureAwait(false)
                       && Convert.ToBoolean(reader.GetValue(1), CultureInfo.InvariantCulture);

            await reader.CloseAsync().ConfigureAwait(false);
            return (true, value, flag);
        }, token).ConfigureAwait(false);

        return toLookup(found, connectionString, disabled);
    }

    private MasterTenantLookup toLookup(bool found, string? connectionString, bool disabled)
    {
        if (!found)
        {
            return new MasterTenantLookup(MasterTenantStatus.Unknown, null);
        }

        if (disabled)
        {
            return new MasterTenantLookup(MasterTenantStatus.Disabled, null);
        }

        return string.IsNullOrEmpty(connectionString)
            ? new MasterTenantLookup(MasterTenantStatus.Unknown, null)
            : new MasterTenantLookup(MasterTenantStatus.Enabled, Options.CorrectConnectionString(connectionString));
    }

    private async Task<MasterTenantLookup> LookupTenantWithoutRowReadAsync(string tenantId, CancellationToken token)
    {
        var connectionString = await LookupConnectionStringAsync(tenantId, token).ConfigureAwait(false);
        if (connectionString is not null)
        {
            return new MasterTenantLookup(MasterTenantStatus.Enabled, connectionString);
        }

        var disabled = await AllDisabledAsync(token).ConfigureAwait(false);
        return disabled.Any(id => _databases.Comparer.Equals(CorrectTenantId(id), tenantId))
            ? new MasterTenantLookup(MasterTenantStatus.Disabled, null)
            : new MasterTenantLookup(MasterTenantStatus.Unknown, null);
    }

    /// <summary>
    /// Resolve a tenant's database the way <see cref="TryFindDatabaseAsync" /> does — from the
    /// cache, reading the control table when it is not cached and caching what it finds — but say
    /// <em>why</em> when there is nothing to route to: unknown, or disabled.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the seam for a store that refuses the two differently (polecat#655, marten#5479):
    /// <code>
    /// var resolution = await ResolveTenantAsync(tenantId, token);
    /// return resolution.Database ?? throw CreateTenantRefusal(tenantId, resolution.Status);
    /// </code>
    /// </para>
    /// <para>
    /// A cached tenant is <see cref="MasterTenantStatus.Enabled" /> without a read. That is right
    /// because disabling or deleting evicts on this instance, and <see cref="BuildDatabasesAsync" />
    /// evicts what another instance disabled or deleted.
    /// </para>
    /// </remarks>
    public async ValueTask<MasterTenantResolution<TDatabase>> ResolveTenantAsync(
        string tenantId, CancellationToken token = default)
    {
        tenantId = CorrectTenantId(tenantId);

        if (_databases.TryGetValue(tenantId, out var cached))
        {
            return new MasterTenantResolution<TDatabase>(MasterTenantStatus.Enabled, cached.Database);
        }

        var lookup = await LookupTenantAsync(tenantId, token).ConfigureAwait(false);
        if (lookup.Status != MasterTenantStatus.Enabled)
        {
            return new MasterTenantResolution<TDatabase>(lookup.Status, null);
        }

        var connectionString = lookup.ConnectionString!;
        var database = _databases.GetOrAdd(tenantId,
            id => new TenantEntry(connectionString, BuildDatabase(id, connectionString))).Database;

        return new MasterTenantResolution<TDatabase>(MasterTenantStatus.Enabled, database);
    }

    /// <summary>
    /// Resolve a tenant's database <b>synchronously</b> — from the cache, or with one blocking read
    /// of the control table — when that can be done without waiting on asynchronous work. Returns
    /// false, touching nothing, when it cannot; the caller then resolves through
    /// <see cref="ResolveTenantAsync" /> (weasel#838).
    /// </summary>
    /// <param name="tenantId">The tenant id, before <see cref="CorrectTenantId" />.</param>
    /// <param name="resolution">
    /// When this returns true, the same answer <see cref="ResolveTenantAsync" /> would give, and a
    /// tenant it finds enabled is cached in the same cache, so the two paths see one set of tenants.
    /// </param>
    /// <remarks>
    /// <para>
    /// For a store's synchronous tenant lookup, which otherwise has to block a thread on
    /// <see cref="ResolveTenantAsync" /> for every uncached tenant. Under a burst of cold tenants
    /// from pool threads that is thread-pool starvation (marten#5985). The read here is
    /// <see cref="DbDataSource.OpenConnection" /> and one statement through
    /// <see cref="IMasterTenantTableDialect.SelectTenantRow" />, run inside
    /// <see cref="ExecuteSynchronously{T}" />; there is no <see cref="Task" /> anywhere in it.
    /// </para>
    /// <code>
    /// if (!TryResolveTenant(tenantId, out var resolution))
    /// {
    ///     resolution = ResolveTenantAsync(tenantId).AsTask().GetAwaiter().GetResult();
    /// }
    /// return resolution.Database ?? throw CreateTenantRefusal(tenantId, resolution.Status);
    /// </code>
    /// <para>
    /// A cached tenant always resolves here. An uncached one returns false — the fallback — when:
    /// </para>
    /// <list type="bullet">
    /// <item>The dialect does not render <see cref="IMasterTenantTableDialect.SelectTenantRow" />, so
    /// telling a disabled tenant from an unknown one would take a second read.</item>
    /// <item>The control-plane data source does not take synchronous I/O
    /// (<see cref="SupportsSynchronousControlTableReads" />): Npgsql refuses it when
    /// <c>Multiplexing=true</c>. A <see cref="NotSupportedException" /> from the read itself is taken
    /// as the same refusal, and remembered.</item>
    /// <item>No entry point has provisioned the control table yet, and the store cannot provision it
    /// synchronously. <see cref="ProvisionControlTableAsync" /> is asynchronous, so this never calls
    /// it; it asks <see cref="TryProvisionControlTableSynchronously" /> instead, which declines by
    /// default (weasel#850). Any asynchronous call, <see cref="BuildDatabasesAsync" /> at start-up
    /// included, or one <c>true</c> from that seam makes every later call eligible.</item>
    /// </list>
    /// <para>
    /// Any other failure of the read — the database is down, the table is missing under
    /// <see cref="AutoCreate.None" /> — is thrown, as <see cref="ResolveTenantAsync" /> would throw it.
    /// So is any failure of <see cref="TryProvisionControlTableSynchronously" /> other than a
    /// <see cref="NotSupportedException" />, as the asynchronous path throws a failed provisioning.
    /// </para>
    /// </remarks>
    public bool TryResolveTenant(string tenantId, out MasterTenantResolution<TDatabase> resolution)
    {
        tenantId = CorrectTenantId(tenantId);

        if (_databases.TryGetValue(tenantId, out var cached))
        {
            resolution = new MasterTenantResolution<TDatabase>(MasterTenantStatus.Enabled, cached.Database);
            return true;
        }

        resolution = default;

        // The read is checked for first: provisioning synchronously is only worth doing when the
        // read that follows it can be done synchronously too.
        var sql = Dialect.SelectTenantRow(QualifiedTableName);
        if (sql is null || !canReadSynchronously() || !ensureControlTableSynchronously())
        {
            return false;
        }

        (bool Found, string? ConnectionString, bool Disabled) row;
        try
        {
            row = ExecuteSynchronously(() => readTenantRow(sql, tenantId));
        }
        catch (NotSupportedException)
        {
            // Npgsql's "Synchronous command execution is not supported when multiplexing is on",
            // for a data source whose connection string did not say so where it could be read.
            Volatile.Write(ref _synchronousReads, SynchronousReadsRefused);
            return false;
        }

        var lookup = toLookup(row.Found, row.ConnectionString, row.Disabled);
        if (lookup.Status != MasterTenantStatus.Enabled)
        {
            resolution = new MasterTenantResolution<TDatabase>(lookup.Status, null);
            return true;
        }

        var connectionString = lookup.ConnectionString!;
        var database = _databases.GetOrAdd(tenantId,
            id => new TenantEntry(connectionString, BuildDatabase(id, connectionString))).Database;

        resolution = new MasterTenantResolution<TDatabase>(MasterTenantStatus.Enabled, database);
        return true;
    }

    /// <summary>
    /// Whether the control-plane data source takes synchronous I/O, for
    /// <see cref="TryResolveTenant" />. Asked once per instance.
    /// </summary>
    /// <remarks>
    /// The default reads the data source's connection string and answers false when it sets
    /// <c>Multiplexing=true</c>, which Npgsql refuses synchronous commands under; every other
    /// provider ignores the keyword. Override where a store knows better, for example from its own
    /// data source's settings.
    /// </remarks>
    protected virtual bool SupportsSynchronousControlTableReads
    {
        get
        {
            var builder = new DbConnectionStringBuilder();
            try
            {
                builder.ConnectionString = MasterDataSource.ConnectionString;
            }
            catch (ArgumentException)
            {
                // Not in the generic key=value form, so nothing here can be read from it.
                return true;
            }

            return !(builder.TryGetValue("Multiplexing", out var value)
                     && bool.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), out var multiplexing)
                     && multiplexing);
        }
    }

    /// <summary>
    /// Run one synchronous control-plane operation, for <see cref="TryResolveTenant" />. The default
    /// just runs it; override to wrap it in the store's resilience pipeline, as
    /// <see cref="ExecuteAsync{T}" /> is for the asynchronous calls.
    /// </summary>
    /// <remarks>
    /// An override must not block on asynchronous work here — that is the whole point of the
    /// synchronous path. A pipeline with no synchronous form should leave this alone rather than
    /// wrap it in sync-over-async.
    /// </remarks>
    protected virtual T ExecuteSynchronously<T>(Func<T> operation) => operation();

    /// <summary>
    /// Provision the control table <b>without awaiting anything</b>, for
    /// <see cref="TryResolveTenant" /> in a process where no asynchronous entry point has run yet.
    /// Return true once the table is known to be there; return false to keep declining, which sends
    /// the caller to <see cref="ResolveTenantAsync" /> as before. The default returns false
    /// (weasel#850).
    /// </summary>
    /// <returns>
    /// True when the control table is there and may be read: <see cref="TryResolveTenant" /> then
    /// stops asking and reads, as it does once an asynchronous entry point has provisioned. The
    /// asynchronous entry points are unaffected and call <see cref="ProvisionControlTableAsync" />
    /// as they always have. False to
    /// decline; this is asked again on the next uncached <see cref="TryResolveTenant" />, so an
    /// override that can never provision synchronously should answer cheaply, and one that has a
    /// lasting reason to decline should remember it itself.
    /// </returns>
    /// <remarks>
    /// <para>
    /// Without this, <see cref="TryResolveTenant" /> declines every uncached tenant until something
    /// asynchronous has provisioned the table, so a cold process whose first touch is synchronous —
    /// the first requests after a deploy — blocks a thread on <see cref="ResolveTenantAsync" /> for
    /// each of them, which is the thread-pool starvation the synchronous path exists to avoid.
    /// </para>
    /// <para>
    /// An override must do with synchronous I/O what its <see cref="ProvisionControlTableAsync" />
    /// does, or less: nothing at all when the schema is the operator's
    /// (<see cref="EffectiveAutoCreate" /> is <see cref="AutoCreate.None" />), otherwise the same
    /// idempotent DDL or migration, one blocking statement at a time. It must not block on a
    /// <see cref="Task" /> — not its own, not <see cref="ProvisionControlTableAsync" />'s, not a
    /// semaphore an asynchronous provisioning holds across an <c>await</c>. When it cannot avoid
    /// that, it returns false. It is not run inside <see cref="ExecuteSynchronously{T}" />; an
    /// override that wants the store's resilience pipeline applies it itself.
    /// </para>
    /// <para>
    /// <b>Concurrency.</b> The base never runs this concurrently with itself: calls are serialized
    /// on one lock, and a caller that waited re-checks whether the table became ready before asking
    /// again, so a burst of cold callers provisions once. Waiting on that lock is waiting on
    /// synchronous I/O another thread is doing, which needs no pool thread to complete. It <i>can</i>
    /// run while <see cref="ProvisionControlTableAsync" /> runs on another thread — the asynchronous
    /// path cannot take a synchronous lock — so the two must tolerate overlapping: idempotent DDL,
    /// as <see cref="IMasterTenantTableDialect.CreateControlTable" /> already has to be, makes that
    /// harmless. An override sharing a "provisioned" flag with its asynchronous override should set
    /// it only after it succeeds, as the base sets its own.
    /// </para>
    /// <para>
    /// <b>Failure.</b> A <see cref="NotSupportedException" /> — a driver refusing synchronous I/O —
    /// declines, and is remembered: the seam is not asked again on this instance, though reads become
    /// eligible once an asynchronous entry point provisions the table. Any other exception
    /// propagates out of <see cref="TryResolveTenant" />, as a failed provisioning propagates out of
    /// the asynchronous entry points, and nothing is remembered, so the next call asks again.
    /// </para>
    /// </remarks>
    protected virtual bool TryProvisionControlTableSynchronously() => false;

    private bool canReadSynchronously()
    {
        var state = Volatile.Read(ref _synchronousReads);
        if (state == SynchronousReadsUnknown)
        {
            var asked = SupportsSynchronousControlTableReads ? SynchronousReadsSupported : SynchronousReadsRefused;

            // The first answer wins; a refusal recorded meanwhile by a failed read stands
            state = Interlocked.CompareExchange(ref _synchronousReads, asked, SynchronousReadsUnknown);
            if (state == SynchronousReadsUnknown) state = asked;
        }

        return state == SynchronousReadsSupported;
    }

    // weasel#850: the synchronous counterpart of ensureControlTableAsync, publishing the same flag.
    private bool ensureControlTableSynchronously()
    {
        if (_controlTableReady)
        {
            return true;
        }

        if (_synchronousProvisioningRefused)
        {
            return false;
        }

        lock (_synchronousProvisioningLock)
        {
            // Another caller may have provisioned while this one waited, on either path.
            if (_controlTableReady)
            {
                return true;
            }

            if (_synchronousProvisioningRefused)
            {
                return false;
            }

            bool provisioned;
            try
            {
                provisioned = TryProvisionControlTableSynchronously();
            }
            catch (NotSupportedException)
            {
                // The same refusal TryResolveTenant takes from the read: the driver will not do
                // synchronous I/O here, so do not throw and catch it once per call of a burst.
                _synchronousProvisioningRefused = true;
                return false;
            }

            if (provisioned)
            {
                _controlTableReady = true;
            }

            return provisioned;
        }
    }

    private (bool Found, string? ConnectionString, bool Disabled) readTenantRow(string sql, string tenantId)
    {
        using var conn = MasterDataSource.OpenConnection();
        using var cmd = CreateCommand(conn, sql, ("@id", tenantId));
        using var reader = cmd.ExecuteReader();

        if (!reader.Read())
        {
            return (false, null, false);
        }

        var value = reader.IsDBNull(0) ? null : reader.GetString(0);
        var flag = !reader.IsDBNull(1) && Convert.ToBoolean(reader.GetValue(1), CultureInfo.InvariantCulture);
        return (true, value, flag);
    }

    /// <summary>
    /// The exception refusing a tenant this tenancy will not route to. Used by
    /// <see cref="ITenantedSource{T}.FindAsync" />, and by a store turning a
    /// <see cref="ResolveTenantAsync" /> miss into an exception.
    /// </summary>
    /// <param name="tenantId">The tenant id that was refused.</param>
    /// <param name="status">
    /// Why: <see cref="MasterTenantStatus.Unknown" /> or <see cref="MasterTenantStatus.Disabled" />.
    /// </param>
    /// <remarks>
    /// The default is <see cref="UnknownTenantIdException" /> for both, which is what the base has
    /// always thrown. A store that tells them apart overrides this to return
    /// <c>DisabledTenantException</c> for <see cref="MasterTenantStatus.Disabled" />; it derives from
    /// <see cref="UnknownTenantIdException" />, so a caller catching the old type keeps catching.
    /// </remarks>
    protected virtual UnknownTenantIdException CreateTenantRefusal(string tenantId, MasterTenantStatus status) =>
        new(tenantId);

    /// <summary>The tenant ids currently disabled.</summary>
    public async Task<IReadOnlyList<string>> AllDisabledAsync(CancellationToken token = default)
    {
        await ensureControlTableAsync(token).ConfigureAwait(false);

        return await ExecuteAsync(async ct =>
        {
            var list = new List<string>();

            await using var conn = await MasterDataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = CreateCommand(conn, Dialect.SelectDisabledTenantIds(QualifiedTableName));
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                list.Add(reader.GetString(0));
            }

            await reader.CloseAsync().ConfigureAwait(false);
            return (IReadOnlyList<string>)list;
        }, token).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------------------------------
    // The runtime lifecycle
    // ------------------------------------------------------------------------------------------

    /// <summary>
    /// Record a tenant's connection string, creating or updating it. The tenant is cached eagerly,
    /// so it is usable by the next session without waiting for a refresh.
    /// </summary>
    public async Task AddDatabaseRecordAsync(
        string tenantId, string connectionString, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        tenantId = CorrectTenantId(tenantId);
        await ensureControlTableAsync(token).ConfigureAwait(false);

        await ExecuteAsync(async ct =>
        {
            await using var conn = await MasterDataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = CreateCommand(conn, Dialect.UpsertTenant(QualifiedTableName),
                ("@id", tenantId), ("@connection", connectionString));

            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, token).ConfigureAwait(false);

        // The stored string is the caller's; the built database gets the corrected one.
        var corrected = Options.CorrectConnectionString(connectionString);
        var replaced = default(TDatabase);

        _databases.AddOrUpdate(tenantId,
            id => new TenantEntry(corrected, BuildDatabase(id, corrected)),
            (id, existing) =>
            {
                replaced = existing.Database;
                return new TenantEntry(corrected, BuildDatabase(id, corrected));
            });

        if (replaced is not null)
        {
            await DisposeDatabaseAsync(replaced).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Remove a tenant's record. The tenant's own database is left completely alone.
    /// </summary>
    public async Task DeleteDatabaseRecordAsync(string tenantId, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        tenantId = CorrectTenantId(tenantId);
        await ensureControlTableAsync(token).ConfigureAwait(false);

        // Evicted before the delete, not after: a caller racing this must not be handed a database
        // for a record that is on its way out.
        await EvictAsync(tenantId).ConfigureAwait(false);

        await ExecuteAsync(async ct =>
        {
            await using var conn = await MasterDataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = CreateCommand(conn, Dialect.DeleteTenant(QualifiedTableName),
                ("@id", tenantId));

            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, token).ConfigureAwait(false);
    }

    /// <summary>Remove every tenant record and empty the cache.</summary>
    public async Task ClearAllDatabaseRecordsAsync(CancellationToken token = default)
    {
        await ensureControlTableAsync(token).ConfigureAwait(false);

        foreach (var tenantId in _databases.Keys.ToList())
        {
            await EvictAsync(tenantId).ConfigureAwait(false);
        }

        await ExecuteAsync(async ct =>
        {
            await using var conn = await MasterDataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = CreateCommand(conn, Dialect.DeleteAllTenants(QualifiedTableName));

            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, token).ConfigureAwait(false);
    }

    /// <summary>
    /// Disable a tenant without deleting its record. It is evicted from the cache and stops
    /// resolving until re-enabled.
    /// </summary>
    public async Task DisableTenantAsync(string tenantId, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        tenantId = CorrectTenantId(tenantId);
        await ensureControlTableAsync(token).ConfigureAwait(false);

        await SetDisabledAsync(tenantId, true, token).ConfigureAwait(false);

        // The eviction is what makes disabling take effect at all — the cache does not consult the
        // control table, so a cached database would go on serving a tenant that is switched off.
        await EvictAsync(tenantId).ConfigureAwait(false);
    }

    /// <summary>Re-enable a disabled tenant. It resolves again on next access.</summary>
    public async Task EnableTenantAsync(string tenantId, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        tenantId = CorrectTenantId(tenantId);
        await ensureControlTableAsync(token).ConfigureAwait(false);

        await SetDisabledAsync(tenantId, false, token).ConfigureAwait(false);

        // No eager cache fill: the tenant is loaded lazily on next access, which is also what makes
        // "enable a tenant that was never added" a no-op rather than a phantom database.
    }

    // ------------------------------------------------------------------------------------------
    // Control table provisioning
    // ------------------------------------------------------------------------------------------

    /// <summary>
    /// Create the control table on first use, once per instance.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Guarded by a semaphore with the flag re-checked inside it, so a burst of concurrent first
    /// calls issues one statement rather than N. That is the cheap half; the DDL itself has to be
    /// idempotent regardless (see <see cref="IMasterTenantTableDialect.CreateControlTable" />),
    /// because a semaphore guards one process and a deployment has several.
    /// </para>
    /// <para>
    /// <b>The flag is only set after the statement succeeds.</b> Remembering a failed attempt as
    /// done would leave the tenancy permanently broken with nothing to say why — the same
    /// discipline a first-use migration cache needs.
    /// </para>
    /// <para>
    /// Override to run the store's own Weasel migration for the table instead, as Marten does; an
    /// override that does so never calls the dialect's DDL. A store that can do the same without
    /// awaiting anything should also override <see cref="TryProvisionControlTableSynchronously" />,
    /// or <see cref="TryResolveTenant" /> declines until this has run once.
    /// </para>
    /// </remarks>
    protected virtual async Task ProvisionControlTableAsync(CancellationToken token)
    {
        if (_controlTableProvisioned)
        {
            return;
        }

        if (EffectiveAutoCreate == AutoCreate.None)
        {
            // The schema is the operator's. Do not emit DDL on what may be a least-privilege
            // connection; a genuinely missing table surfaces as the database's own error.
            _controlTableProvisioned = true;
            return;
        }

        await _provisioning.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_controlTableProvisioned)
            {
                return;
            }

            await ExecuteAsync(async ct =>
            {
                await using var conn = await MasterDataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
                await using var cmd = CreateCommand(conn,
                    Dialect.CreateControlTable(Options.SchemaName, TableName, QualifiedTableName));

                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }, token).ConfigureAwait(false);

            _controlTableProvisioned = true;
        }
        finally
        {
            _provisioning.Release();
        }
    }

    // Every entry point provisions through here rather than calling the virtual directly, so the
    // synchronous path can tell the table is there once any of them -- whatever the store's override
    // does -- has returned.
    private async Task ensureControlTableAsync(CancellationToken token)
    {
        await ProvisionControlTableAsync(token).ConfigureAwait(false);
        _controlTableReady = true;
    }

    /// <summary>
    /// Write the configured <see cref="MasterTableTenancyOptions{T}.SeedDatabases" /> into the
    /// control table, once.
    /// </summary>
    /// <remarks>
    /// Upserts rather than inserts, so a seeded tenant whose connection string changed in
    /// configuration is corrected on the next start rather than silently keeping the old one.
    /// </remarks>
    protected virtual async Task SeedDatabasesAsync(CancellationToken token)
    {
        if (_seeded || !Options.SeedDatabases.HasAny())
        {
            return;
        }

        foreach (var assignment in Options.SeedDatabases.AllActiveByTenant())
        {
            await ExecuteAsync(async ct =>
            {
                await using var conn = await MasterDataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
                await using var cmd = CreateCommand(conn, Dialect.UpsertTenant(QualifiedTableName),
                    ("@id", CorrectTenantId(assignment.TenantId)), ("@connection", assignment.Value));

                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }, token).ConfigureAwait(false);
        }

        _seeded = true;
    }

    // ------------------------------------------------------------------------------------------
    // IMasterTableMultiTenancy — the host-level convenience surface
    // ------------------------------------------------------------------------------------------

    async Task<bool> IMasterTableMultiTenancy.TryAddTenantDatabaseRecordsAsync(
        string tenantId, string connectionString)
    {
        await AddDatabaseRecordAsync(tenantId, connectionString, CancellationToken.None).ConfigureAwait(false);
        return true;
    }

    async Task<bool> IMasterTableMultiTenancy.ClearAllDatabaseRecordsAsync()
    {
        await ClearAllDatabaseRecordsAsync(CancellationToken.None).ConfigureAwait(false);
        return true;
    }

    // ------------------------------------------------------------------------------------------
    // IDynamicTenantSource<string> — the store-agnostic admin surface (CritterWatch)
    // ------------------------------------------------------------------------------------------

    // Implemented explicitly throughout, because every member here has a cancellable twin above and
    // the concrete API should stay the cancellable one. Both stores wrote this adapter by hand; it
    // is the single largest block the lift removes from each.

    async ValueTask<string> ITenantedSource<string>.FindAsync(string tenantId)
    {
        var lookup = await LookupTenantAsync(tenantId, CancellationToken.None).ConfigureAwait(false);

        // The refusal is the store's to choose (weasel#809), and defaults to UnknownTenantIdException
        // for both unknown and disabled, which is what this has always thrown.
        return lookup.ConnectionString ?? throw CreateTenantRefusal(CorrectTenantId(tenantId), lookup.Status);
    }

    // The point of a refresh is to drop tenants that have since been removed or disabled, and
    // BuildDatabasesAsync now does exactly that (weasel#809). This used to evict every tenant first,
    // which dropped the stale ones too but also disposed every live tenant's database -- and its
    // connection pool -- out from under sessions still using it, and bypassed EvictStaleTenantAsync.
    async Task ITenantedSource<string>.RefreshAsync() =>
        await BuildDatabasesAsync(CancellationToken.None).ConfigureAwait(false);

    /// <summary>
    /// The tenant identifiers currently active. Deliberately not the connection strings, which
    /// would put credentials on an admin dashboard.
    /// </summary>
    IReadOnlyList<string> ITenantedSource<string>.AllActive() => AllTenantIds();

    IReadOnlyList<Assignment<string>> ITenantedSource<string>.AllActiveByTenant() =>
        _databases.Keys.Select(id => new Assignment<string>(id, id)).ToList();

    Task IDynamicTenantSource<string>.AddTenantAsync(string tenantId, string connectionValue) =>
        AddDatabaseRecordAsync(tenantId, connectionValue, CancellationToken.None);

    // The auto-assign overload -- Task<string> AddTenantAsync(string, CancellationToken) -- is left
    // on its throwing default deliberately. There is no pool for a database-per-tenant model to
    // assign from, so a caller has to supply a connection string.

    Task IDynamicTenantSource<string>.DisableTenantAsync(string tenantId) =>
        DisableTenantAsync(tenantId, CancellationToken.None);

    Task IDynamicTenantSource<string>.EnableTenantAsync(string tenantId) =>
        EnableTenantAsync(tenantId, CancellationToken.None);

    Task IDynamicTenantSource<string>.RemoveTenantAsync(string tenantId) =>
        DeleteDatabaseRecordAsync(tenantId, CancellationToken.None);

    Task<IReadOnlyList<string>> IDynamicTenantSource<string>.AllDisabledAsync() =>
        AllDisabledAsync(CancellationToken.None);

    // ------------------------------------------------------------------------------------------

    private async Task SetDisabledAsync(string tenantId, bool disabled, CancellationToken token) =>
        await ExecuteAsync(async ct =>
        {
            await using var conn = await MasterDataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = CreateCommand(conn,
                Dialect.SetTenantDisabled(QualifiedTableName, disabled), ("@id", tenantId));

            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, token).ConfigureAwait(false);

    private async ValueTask<bool> EvictAsync(string tenantId)
    {
        if (!_databases.TryRemove(tenantId, out var entry))
        {
            return false;
        }

        await DisposeDatabaseAsync(entry.Database).ConfigureAwait(false);
        return true;
    }

    [SuppressMessage("Reliability", "CA2100:Review SQL queries for security vulnerabilities",
        Justification = "The SQL comes from the store's own IMasterTenantTableDialect; tenant ids and connection strings are bound as parameters.")]
    private static DbCommand CreateCommand(
        DbConnection connection, string sql, params (string Name, object? Value)[] parameters)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;

        foreach (var (name, value) in parameters)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }

        return command;
    }

    /// <summary>
    /// Dispose every cached tenant database and, when this tenancy built it, the control-plane data
    /// source.
    /// </summary>
    /// <remarks>
    /// Written out synchronously rather than blocking on <see cref="DisposeAsync" />. A container
    /// disposed synchronously reaches this one, and sync-over-async here would be a deadlock risk
    /// on exactly the shutdown path that has nowhere to report it.
    /// </remarks>
    public void Dispose()
    {
        foreach (var entry in _databases.Values)
        {
            DisposeDatabase(entry.Database);
        }

        _databases.Clear();

        if (_ownsDataSource)
        {
            _dataSource?.Dispose();
        }

        _provisioning.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc cref="Dispose" />
    public async ValueTask DisposeAsync()
    {
        foreach (var entry in _databases.Values)
        {
            await DisposeDatabaseAsync(entry.Database).ConfigureAwait(false);
        }

        _databases.Clear();

        if (_ownsDataSource && _dataSource is not null)
        {
            await _dataSource.DisposeAsync().ConfigureAwait(false);
        }

        _provisioning.Dispose();
        GC.SuppressFinalize(this);
    }

    // A class, not a record: eviction removes an entry only if it is still the *same* entry, and a
    // record's value equality would let a re-added tenant with the same connection string -- and a
    // TDatabase that is itself a record, as Fisher's is -- be mistaken for the stale one.
    private sealed class TenantEntry(string connectionString, TDatabase database)
    {
        public string ConnectionString { get; } = connectionString;
        public TDatabase Database { get; } = database;
    }
}
