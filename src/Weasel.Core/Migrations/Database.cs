using System.Collections.Concurrent;
using System.Data.Common;
using JasperFx.Core;

namespace Weasel.Core.Migrations;

public record AttainLockResult(bool Succeeded, AttainLockResult.FailureReason Reason)
{
    public bool ShouldReconnect => Reason == FailureReason.DatabaseNotAvailable;

    public static readonly AttainLockResult Success = new(true, FailureReason.None);

    public static AttainLockResult Failure(FailureReason reason = FailureReason.Failure) => new(false, reason);

    public enum FailureReason
    {
        None,
        Failure,
        DatabaseNotAvailable
    }
}

public interface IGlobalLock<in TConnection> where TConnection : DbConnection
{
    Task<AttainLockResult> TryAttainLock(TConnection conn, CancellationToken ct = default);
    Task ReleaseLock(TConnection conn, CancellationToken ct = default);
}

internal class NulloGlobalList<TConnection>: IGlobalLock<TConnection> where TConnection : DbConnection
{
    public Task<AttainLockResult> TryAttainLock(TConnection conn, CancellationToken ct = default)
    {
        return Task.FromResult(AttainLockResult.Success);
    }

    public Task ReleaseLock(TConnection conn, CancellationToken ct = default)
    {
        return Task.CompletedTask;
    }
}

public abstract class DatabaseBase<TConnection>: IDatabase<TConnection> where TConnection : DbConnection, new()
{
    private readonly ConcurrentDictionary<Type, bool> _checks = new();
    private readonly Func<TConnection> _connectionSource;
    private readonly IMigrationLogger _logger;
    private readonly TimedLock _migrateLocker = new();

    public DatabaseBase(
        IMigrationLogger logger,
        AutoCreate autoCreate,
        Migrator migrator,
        string identifier,
        string connectionString
    )
    {
        _logger = logger;
        _connectionSource = () =>
        {
            var conn = new TConnection();
            conn.ConnectionString = connectionString;

            return conn;
        };

        AutoCreate = autoCreate;
        Migrator = migrator;
        Identifier = identifier;
    }

    public DatabaseBase(
        IMigrationLogger logger,
        AutoCreate autoCreate,
        Migrator migrator,
        string identifier,
        Func<TConnection> connectionSource
    )
    {
        _logger = logger;
        _connectionSource = connectionSource;
        AutoCreate = autoCreate;
        Migrator = migrator;
        Identifier = identifier;
    }

    public abstract IFeatureSchema[] BuildFeatureSchemas();

    public AutoCreate AutoCreate { get; set; }
    public Migrator Migrator { get; }

    public string Identifier { get; }


    TConnection IConnectionSource<TConnection>.CreateConnection()
    {
        return _connectionSource();
    }

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

    public async Task<SchemaMigration> CreateMigrationAsync(IFeatureSchema group, CancellationToken ct = default)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(ct).ConfigureAwait(false);

        var migration = await SchemaMigration.DetermineAsync(conn, ct, group.Objects).ConfigureAwait(false);

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

        Migrator.WriteScript(writer, (m, w) =>
        {
            m.WriteSchemaCreationSql(schemaNames, writer);

            foreach (var group in BuildFeatureSchemas()) group.WriteFeatureCreation(Migrator, writer);
        });

        return writer.ToString();
    }

    public Task WriteCreationScriptToFileAsync(string filename, CancellationToken ct = default)
    {
        var directory = Path.GetDirectoryName(filename);
        FileSystem.CreateDirectoryIfNotExists(directory);

        var sql = ToDatabaseScript();
        return File.WriteAllTextAsync(filename, sql, ct);
    }

    /// <summary>
    ///     Write scripts for all the features in this database to a file for
    ///     each feature
    /// </summary>
    /// <param name="directory"></param>
    public async Task WriteScriptsByTypeAsync(string directory, CancellationToken ct = default)
    {
        FileSystem.CleanDirectory(directory);

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
        var objects = AllObjects().ToArray();

        await using var conn = CreateConnection();
        await conn.OpenAsync(ct).ConfigureAwait(false);

        return await SchemaMigration.DetermineAsync(conn, ct, objects).ConfigureAwait(false);
    }

    public virtual Task<SchemaPatchDifference> ApplyAllConfiguredChangesToDatabaseAsync(
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

    public TConnection CreateConnection()
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

        foreach (var dependentType in feature.DependentTypes())
            await ensureStorageExistsAsync(types, dependentType, token).ConfigureAwait(false);

        await generateOrUpdateFeature(featureType, feature, token).ConfigureAwait(false);
    }

    protected async ValueTask generateOrUpdateFeature(Type featureType, IFeatureSchema feature, CancellationToken token)
    {
        if (_checks.ContainsKey(featureType))
        {
            RegisterCheck(featureType, feature);
            return;
        }

        var schemaObjects = feature.Objects;


        foreach (var objectName in schemaObjects.SelectMany(x => x.AllNames()))
            Migrator.AssertValidIdentifier(objectName.Name);

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

/// <summary>
/// Reconnection policy options when the database is unavailable while applying database changes.
/// </summary>
/// <param name="MaxReconnectionCount">The maximum number of reconnections if the database is unavailable while applying database changes. Default is 3.</param>
/// <param name="DelayInMs">The base delay between reconnections to perform if the database is unavailable while applying database changes. Note it'll be performed with exponential backoff. Default is 50ms.</param>
public record ReconnectionOptions(
    int MaxReconnectionCount = 3,
    int DelayInMs = 50
)
{
    public static ReconnectionOptions Default { get; } = new();
}
