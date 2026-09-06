#nullable enable
using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
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
    /// Read every enabled tenant from the control table, refreshing the cache, and return the
    /// databases this tenancy now knows about.
    /// </summary>
    /// <remarks>
    /// Idempotent: a tenant already cached under the same connection string keeps its existing
    /// database rather than getting a new one, so calling this on a schedule does not churn
    /// connection pools. A tenant whose connection string has <em>changed</em> is rebuilt, and the
    /// database it replaces is disposed.
    /// </remarks>
    public async Task<IReadOnlyList<TDatabase>> BuildDatabasesAsync(CancellationToken token = default)
    {
        await ProvisionControlTableAsync(token).ConfigureAwait(false);
        await SeedDatabasesAsync(token).ConfigureAwait(false);

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

        foreach (var (rawTenantId, rawConnectionString) in rows)
        {
            var tenantId = CorrectTenantId(rawTenantId);
            var connectionString = Options.CorrectConnectionString(rawConnectionString);

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

        return AllDatabases();
    }

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
    /// not routable, which is what disabling means — but a caller turning this into an exception
    /// should throw <see cref="UnknownTenantIdException" /> so it is the same failure an
    /// unregistered tenant produces, rather than something a caller has to handle twice.
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
        await ProvisionControlTableAsync(token).ConfigureAwait(false);

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

    /// <summary>The tenant ids currently disabled.</summary>
    public async Task<IReadOnlyList<string>> AllDisabledAsync(CancellationToken token = default)
    {
        await ProvisionControlTableAsync(token).ConfigureAwait(false);

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
        await ProvisionControlTableAsync(token).ConfigureAwait(false);

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
        await ProvisionControlTableAsync(token).ConfigureAwait(false);

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
        await ProvisionControlTableAsync(token).ConfigureAwait(false);

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
        await ProvisionControlTableAsync(token).ConfigureAwait(false);

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
        await ProvisionControlTableAsync(token).ConfigureAwait(false);

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
    /// override that does so never calls the dialect's DDL.
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

    async ValueTask<string> ITenantedSource<string>.FindAsync(string tenantId) =>
        await LookupConnectionStringAsync(tenantId, CancellationToken.None).ConfigureAwait(false)
        ?? throw new UnknownTenantIdException(tenantId);

    async Task ITenantedSource<string>.RefreshAsync()
    {
        // Clear rather than merge: the point of a refresh is to drop tenants that have since been
        // removed or disabled, and a merge would keep serving them from cache forever.
        foreach (var tenantId in _databases.Keys.ToList())
        {
            await EvictAsync(tenantId).ConfigureAwait(false);
        }

        await BuildDatabasesAsync(CancellationToken.None).ConfigureAwait(false);
    }

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

    private async ValueTask EvictAsync(string tenantId)
    {
        if (_databases.TryRemove(tenantId, out var entry))
        {
            await DisposeDatabaseAsync(entry.Database).ConfigureAwait(false);
        }
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

    private sealed record TenantEntry(string ConnectionString, TDatabase Database);
}
