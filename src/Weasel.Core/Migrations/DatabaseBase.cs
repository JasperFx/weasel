using System.Collections.Concurrent;
using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using JasperFx.Descriptors;

namespace Weasel.Core.Migrations;

public abstract class DatabaseBase<TConnection>: IDatabase<TConnection> where TConnection : DbConnection, new()
{
    private readonly ConcurrentDictionary<Type, bool> _checks = new();
    private readonly Func<TConnection> _connectionSource;
    private readonly IMigrationLogger _logger;
    private readonly TimedLock _migrateLocker = new();
    private readonly List<IDatabaseInitializer<TConnection>> _initializers = new();

    public DatabaseBase(
        IMigrationLogger logger,
        AutoCreate autoCreate,
        Migrator migrator,
        string identifier,
        string connectionString
    ): this(logger, autoCreate, migrator, identifier, () => CreateConnection(connectionString))
    {
    }

    public abstract DatabaseDescriptor Describe();

    public DatabaseBase(
        IMigrationLogger logger,
        AutoCreate autoCreate,
        Migrator migrator,
        string identifier,
        Func<TConnection>? connectionSource
    )
    {
        _logger = logger;
        _connectionSource = connectionSource ?? CreateConnection;
        AutoCreate = autoCreate;
        Migrator = migrator;
        Identifier = identifier;
    }

    public void AddInitializer(IDatabaseInitializer<TConnection> initializer)
    {
        _initializers.Add(initializer);
    }

    private static TConnection CreateConnection(string connectionString)
    {
        var conn = new TConnection();
        conn.ConnectionString = connectionString;

        return conn;
    }

    public abstract IFeatureSchema[] BuildFeatureSchemas();

    public AutoCreate AutoCreate { get; set; }
    public Migrator Migrator { get; }

    public string Identifier { get; }


    /// <summary>
    ///     All referenced schema names by the known objects in this database
    /// </summary>
    /// <returns></returns>
    public string[] AllSchemaNames()
    {
        return AllObjects()
            .Select(x => x.Identifier.Schema)
            .Distinct()
            .ToArray();
    }

    public IEnumerable<ISchemaObject> AllObjects()
    {
        return BuildFeatureSchemas().SelectMany(group => group.Objects);
    }

    public async Task AssertConnectivityAsync(CancellationToken ct = default)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await conn.CloseAsync().ConfigureAwait(false);
    }

    private async Task initializeSchema(TConnection connection, CancellationToken token)
    {
        foreach (var initializer in _initializers)
        {
            await initializer.InitializeAsync(connection, token).ConfigureAwait(false);
        }
    }

    public async Task<SchemaMigration> CreateMigrationAsync(IFeatureSchema group, CancellationToken ct = default)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(ct).ConfigureAwait(false);

        await initializeSchema(conn, ct).ConfigureAwait(false);

        var migration = await SchemaMigration.DetermineAsync(conn, ct, group.Objects).ConfigureAwait(false);

        await conn.CloseAsync().ConfigureAwait(false);

        return migration;
    }

    public string ToDatabaseScript()
    {
        var schemaNames = AllObjects()
            .SelectMany(x => x.AllNames())
            .Select(x => x.Schema)
            .Distinct()
            .ToArray();
        var writer = new StringWriter();

        applyPostProcessingIfAny();

        Migrator.WriteScript(writer, (m, w) =>
        {
            m.WriteSchemaCreationSql(schemaNames, writer);

            foreach (var group in BuildFeatureSchemas()) group.WriteFeatureCreation(Migrator, writer);
        });

        return writer.ToString();
    }

    private void applyPostProcessingIfAny()
    {
        var objects = AllObjects().ToArray();
        foreach (var postProcessing in objects.OfType<ISchemaObjectWithPostProcessing>().ToArray())
        {
            postProcessing.PostProcess(objects);
        }
    }

    public async Task WriteCreationScriptToFileAsync(string filename, CancellationToken ct = default)
    {
        var directory = Path.GetDirectoryName(filename);
        FileSystem.CreateDirectoryIfNotExists(directory);

        await initializeSchemaWithNewConnection(ct).ConfigureAwait(false);

        var sql = ToDatabaseScript();
        await File.WriteAllTextAsync(filename, sql, ct).ConfigureAwait(false);
    }

    private async Task initializeSchemaWithNewConnection(CancellationToken ct)
    {
        // Don't do this if it's unnecessary
        if (!_initializers.Any()) return;

        await using var conn = CreateConnection();
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await initializeSchema(conn, ct).ConfigureAwait(false);
        await conn.CloseAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     Write scripts for all the features in this database to a file for
    ///     each feature
    /// </summary>
    /// <param name="directory"></param>
    public async Task WriteScriptsByTypeAsync(string directory, CancellationToken ct = default)
    {
        applyPostProcessingIfAny();

        FileSystem.CleanDirectory(directory);

        await initializeSchemaWithNewConnection(ct).ConfigureAwait(false);

        var scriptNames = new List<string>();

        var schemaNames = AllObjects()
            .SelectMany(x => x.AllNames())
            .Select(x => x.Schema)
            .Distinct()
            .Where(x => x != Migrator.DefaultSchemaName)
            .ToArray();

        if (schemaNames.Any())
        {
            scriptNames.Add("schemas.sql");
            await Migrator.WriteTemplatedFile(directory.AppendPath("schemas.sql"), (m, w) =>
            {
                m.WriteSchemaCreationSql(schemaNames, w);
            }, ct).ConfigureAwait(false);
        }

        foreach (var feature in BuildFeatureSchemas())
        {
            var scriptName = $"{feature.Identifier}.sql";
            scriptNames.Add(scriptName);

            await Migrator.WriteTemplatedFile(directory.AppendPath(scriptName), (m, w) =>
            {
                feature.WriteFeatureCreation(m, w);
            }, ct).ConfigureAwait(false);
        }

        var writer = new StringWriter();
        foreach (var scriptName in scriptNames)
            await writer.WriteLineAsync(Migrator.ToExecuteScriptLine(scriptName)).ConfigureAwait(false);

        var filename = directory.AppendPath("all.sql");
        await File.WriteAllTextAsync(filename, writer.ToString(), ct).ConfigureAwait(false);
    }

    public async Task<SchemaMigration> CreateMigrationAsync(CancellationToken ct = default)
    {
        applyPostProcessingIfAny();
        var objects = AllObjects().ToArray();

        objects = possiblyCheckForSchemas(objects);

        await using var conn = CreateConnection();
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await initializeSchema(conn, ct).ConfigureAwait(false);

        var result = await SchemaMigration.DetermineAsync(conn, ct, objects).ConfigureAwait(false);
        await conn.CloseAsync().ConfigureAwait(false);
        return result;
    }

    protected virtual ISchemaObject[] possiblyCheckForSchemas(ISchemaObject[] objects)
    {
        return objects;
    }

    public Task<SchemaPatchDifference> ApplyAllConfiguredChangesToDatabaseAsync(
        AutoCreate? @override = null,
        ReconnectionOptions? reconnectionOptions = null,
        CancellationToken ct = default
    ) =>
        ApplyAllConfiguredChangesToDatabaseAsync(
            new NulloGlobalList<TConnection>(),
            @override,
            reconnectionOptions,
            ct
        );

    public async Task AssertDatabaseMatchesConfigurationAsync(CancellationToken ct = default)
    {
        var patch = await CreateMigrationAsync(ct).ConfigureAwait(false);
        if (patch.Difference != SchemaPatchDifference.None)
        {
            var writer = new StringWriter();
            patch.WriteAllUpdates(writer, Migrator, AutoCreate.CreateOrUpdate);

            throw new DatabaseValidationException(Identifier, writer.ToString());
        }
    }

    public virtual TConnection CreateConnection()
    {
        return _connectionSource();
    }

    public Task<SchemaMigration> CreateMigrationAsync(Type featureType, CancellationToken ct = default)
    {
        var feature = FindFeature(featureType);
        return CreateMigrationAsync(feature, ct);
    }

    public async Task<SchemaPatchDifference> ApplyAllConfiguredChangesToDatabaseAsync(
        IGlobalLock<TConnection> globalLock,
        AutoCreate? @override = null,
        ReconnectionOptions? reconnectionOptions = null,
        CancellationToken ct = default
    )
    {
        var autoCreate = @override ?? AutoCreate;
        if (autoCreate == AutoCreate.None)
        {
            autoCreate = AutoCreate.CreateOrUpdate;
        }

        var objects = AllObjects().ToArray();

        foreach (var objectName in objects.SelectMany(x => x.AllNames()))
        {
            Migrator.AssertValidIdentifier(objectName.Name);
        }

        foreach (var postProcessing in objects.OfType<ISchemaObjectWithPostProcessing>().ToArray())
        {
            postProcessing.PostProcess(objects);
        }

        TConnection? conn = null;
        try
        {
            conn = CreateConnection();
            await conn.OpenAsync(ct).ConfigureAwait(false);

            AttainLockResult attainLockResult;

            reconnectionOptions ??= ReconnectionOptions.Default;
            var (maxReconnectionCount, delayInMs) = reconnectionOptions;

            var reconnectionCount = 0;
            do
            {
                attainLockResult = await globalLock.TryAttainLock(conn, ct).ConfigureAwait(false);

                if (!attainLockResult.ShouldReconnect || ++reconnectionCount == maxReconnectionCount)
                    continue;

                conn = CreateConnection();
                await conn.OpenAsync(ct).ConfigureAwait(false);

                await Task.Delay(reconnectionCount * delayInMs, ct).ConfigureAwait(false);
            } while (attainLockResult.ShouldReconnect && reconnectionCount < maxReconnectionCount);

            if (attainLockResult == AttainLockResult.Success)
            {
                await initializeSchema(conn, ct).ConfigureAwait(false);
                var patch = await SchemaMigration.DetermineAsync(conn, ct, objects).ConfigureAwait(false);

                if (patch.Difference != SchemaPatchDifference.None)
                {
                    await Migrator.ApplyAllAsync(conn, patch, autoCreate, _logger, ct).ConfigureAwait(false);
                }

                MarkAllFeaturesAsChecked();

                await globalLock.ReleaseLock(conn, ct).ConfigureAwait(false);

                return patch.Difference;
            }

            await conn.CloseAsync().ConfigureAwait(false);

            throw new InvalidOperationException(
                "Unable to attain a global lock in time order to apply database changes");
        }
        finally
        {
            if (conn != null)
                await conn.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async Task WriteMigrationFileAsync(string filename, CancellationToken ct = default)
    {
        var patch = await CreateMigrationAsync(ct).ConfigureAwait(false);
        await Migrator.WriteMigrationFileAsync(filename, patch, ct).ConfigureAwait(false);
    }

    public virtual void ResetSchemaExistenceChecks()
    {
        _checks.Clear();
    }

    [Obsolete("Use async version")]
    public void EnsureStorageExists(Type featureType)
    {
        if (AutoCreate == AutoCreate.None)
        {
            return;
        }

#pragma warning disable VSTHRD002
        ensureStorageExistsAsync(new List<Type>(), featureType, default).AsTask().GetAwaiter().GetResult();
#pragma warning restore VSTHRD002
    }

    public ValueTask EnsureStorageExistsAsync(Type featureType, CancellationToken token = default)
    {
        if (AutoCreate == AutoCreate.None)
        {
            return new ValueTask();
        }

        return ensureStorageExistsAsync(new List<Type>(), featureType, token);
    }


    public virtual IFeatureSchema FindFeature(Type featureType)
    {
        return null; // TODO - could get smarter and try to create by type
    }

    public void MarkAllFeaturesAsChecked()
    {
        foreach (var feature in BuildFeatureSchemas()) _checks[feature.StorageType] = true;
    }

    private async ValueTask ensureStorageExistsAsync(
        IList<Type> types,
        Type featureType,
        CancellationToken token = default
    )
    {
        if (_checks.ContainsKey(featureType))
        {
            return;
        }

        var feature = FindFeature(featureType);

        if (feature == null)
        {
            throw new ArgumentOutOfRangeException(nameof(featureType),
                $"Unknown feature type {featureType.FullName}");
        }

        if (_checks.ContainsKey(feature.StorageType))
        {
            _checks[featureType] = true;
            return;
        }

        // Preventing cyclic dependency problems
        if (types.Contains(featureType))
        {
            return;
        }

        types.Fill(featureType);

        await initializeSchemaWithNewConnection(token).ConfigureAwait(false);

        foreach (var dependentType in feature.DependentTypes())
        {
            await ensureStorageExistsAsync(types, dependentType, token).ConfigureAwait(false);
        }

        await generateOrUpdateFeature(featureType, feature, token, false).ConfigureAwait(false);
    }

    protected async ValueTask generateOrUpdateFeature(Type featureType, IFeatureSchema feature, CancellationToken token,
        bool skipPostProcessing)
    {
        if (_checks.ContainsKey(featureType))
        {
            RegisterCheck(featureType, feature);
            return;
        }

        var schemaObjects = feature.Objects;

        foreach (var objectName in schemaObjects.SelectMany(x => x.AllNames()))
        {
            Migrator.AssertValidIdentifier(objectName.Name);
        }

        if (!skipPostProcessing)
        {
            var allObjects = AllObjects().ToArray();
            foreach (var processing in schemaObjects.OfType<ISchemaObjectWithPostProcessing>().ToArray())
            {
                processing.PostProcess(allObjects);
            }
        }

        using (await _migrateLocker.Lock(5.Seconds(), token).ConfigureAwait(false))
        {
            if (_checks.ContainsKey(featureType))
            {
                RegisterCheck(featureType, feature);
                return;
            }

            await executeMigration(schemaObjects, token).ConfigureAwait(false);
            RegisterCheck(featureType, feature);
        }
    }

    private async Task executeMigration(ISchemaObject[] schemaObjects, CancellationToken ct = default)
    {
        await using var conn = _connectionSource();

        await conn.OpenAsync(ct).ConfigureAwait(false);
        await initializeSchema(conn, ct).ConfigureAwait(false);

        var migration = await SchemaMigration.DetermineAsync(conn, ct, schemaObjects).ConfigureAwait(false);

        if (migration.Difference == SchemaPatchDifference.None)
        {
            return;
        }

        migration.AssertPatchingIsValid(AutoCreate);

        await Migrator.ApplyAllAsync(conn, migration, AutoCreate, _logger, ct)
            .ConfigureAwait(false);
    }

    private void RegisterCheck(Type featureType, IFeatureSchema feature)
    {
        _checks[featureType] = true;
        if (feature.StorageType != featureType)
        {
            _checks[feature.StorageType] = true;
        }
    }
}
