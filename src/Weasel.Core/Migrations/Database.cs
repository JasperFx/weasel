using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Baseline;
using Baseline.Dates;

namespace Weasel.Core.Migrations
{
    public interface IGlobalLock<TConnection> where TConnection : DbConnection
    {
        Task<bool> TryAttainLock(TConnection conn);
        Task ReleaseLock(TConnection conn);
    }

    internal class NulloGlobalList<TConnection> : IGlobalLock<TConnection> where TConnection : DbConnection
    {
        public Task<bool> TryAttainLock(TConnection conn)
        {
            return Task.FromResult(true);
        }

        public Task ReleaseLock(TConnection conn)
        {
            return Task.CompletedTask;
        }
    }

    public abstract class DatabaseBase<TConnection>: IDatabase<TConnection> where TConnection : DbConnection, new()
    {
        private readonly IMigrationLogger _logger;
        private readonly ConcurrentDictionary<Type, bool> _checks = new();
        private readonly Func<TConnection> _connectionSource;
        private readonly TimedLock _migrateLocker = new();
        public abstract IFeatureSchema[] BuildFeatureSchemas();

        public AutoCreate AutoCreate { get; set; }
        public Migrator Migrator { get; }

        public string Identifier { get; }

        public DatabaseBase(IMigrationLogger logger, AutoCreate autoCreate, Migrator migrator, string identifier, string connectionString)
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

        public DatabaseBase(IMigrationLogger logger, AutoCreate autoCreate, Migrator migrator, string identifier, Func<TConnection> connectionSource)
        {
            _logger = logger;
            _connectionSource = connectionSource;
            AutoCreate = autoCreate;
            Migrator = migrator;
            Identifier = identifier;
        }

        public TConnection CreateConnection()
        {
            return _connectionSource();
        }


        TConnection IConnectionSource<TConnection>.CreateConnection()
        {
            return _connectionSource();
        }

        /// <summary>
        /// All referenced schema names by the known objects in this database
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
            return BuildFeatureSchemas().SelectMany(@group => @group.Objects);
        }

        public Task<SchemaMigration> CreateMigrationAsync(Type featureType)
        {
            var feature = FindFeature(featureType);
            return CreateMigrationAsync(feature);
        }

        public async Task<SchemaMigration> CreateMigrationAsync(IFeatureSchema group)
        {
#if NETSTANDARD2_0
            using var conn = CreateConnection();
            #else
            await using var conn = CreateConnection();
#endif
            await conn.OpenAsync().ConfigureAwait(false);

            var migration = await SchemaMigration.Determine(conn, group.Objects).ConfigureAwait(false);

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

                foreach (var @group in BuildFeatureSchemas())
                {
                    @group.WriteFeatureCreation(Migrator, writer);
                }
            });

            return writer.ToString();
        }

        public Task WriteCreationScriptToFile(string filename)
        {
            var directory = Path.GetDirectoryName(filename);
            Directory.CreateDirectory(directory);

            var sql = ToDatabaseScript();
#if NETSTANDARD2_0
            File.WriteAllText(filename, sql);
            return Task.CompletedTask;
#else
            return File.WriteAllTextAsync(filename, sql);
#endif
        }

        [Obsolete("Prefer WriteCreationScriptToFile()")]
        public void WriteDatabaseCreationScriptFile(string filename)
        {
            var sql = ToDatabaseScript();
            File.WriteAllText(filename, sql);
        }

        /// <summary>
        /// Write scripts for all the features in this database to a file for
        /// each feature
        /// </summary>
        /// <param name="directory"></param>
        public async Task WriteScriptsByType(string directory)
        {
            // TODO -- really time for async helpers in Baseline
            var system = new FileSystem();

            system.CleanDirectory(directory);

            var scriptNames = new List<string> {  };

            var schemaNames = AllObjects()
                .SelectMany(x => x.AllNames())
                .Select(x => x.Schema)
                .Distinct()
                .Where( x=> x != Migrator.DefaultSchemaName)
                .ToArray();

            if (schemaNames.Any())
            {
                scriptNames.Add("schemas.sql");
                await Migrator.WriteTemplatedFile(directory.AppendPath("schemas.sql"), (m, w) =>
                {
                    m.WriteSchemaCreationSql(schemaNames, w);
                }).ConfigureAwait(false);
            }


            foreach (var feature in BuildFeatureSchemas())
            {
                var scriptName = $"{feature.Identifier}.sql";
                scriptNames.Add(scriptName);

                await Migrator.WriteTemplatedFile(directory.AppendPath(scriptName), (m, w) =>
                {
                    feature.WriteFeatureCreation(m, w);
                }).ConfigureAwait(false);
            }

            var writer = new StringWriter();
            foreach (var scriptName in scriptNames)
            {
                await writer.WriteLineAsync(Migrator.ToExecuteScriptLine(scriptName)).ConfigureAwait(false);
            }

            var filename = directory.AppendPath("all.sql");
            File.WriteAllText(filename, writer.ToString());

        }

        public async Task<SchemaMigration> CreateMigrationAsync()
        {
            var @objects = AllObjects().ToArray();

#if NETSTANDARD2_0
            using var conn = CreateConnection();
#else
            await using var conn = CreateConnection();
#endif
            await conn.OpenAsync().ConfigureAwait(false);

            return await SchemaMigration.Determine(conn, @objects).ConfigureAwait(false);
        }

        public Task<SchemaPatchDifference> ApplyAllConfiguredChangesToDatabaseAsync(AutoCreate? @override = null)
        {
            return ApplyAllConfiguredChangesToDatabaseAsync(new NulloGlobalList<TConnection>(), @override);
        }

        public async Task<SchemaPatchDifference> ApplyAllConfiguredChangesToDatabaseAsync(IGlobalLock<TConnection> globalLock, AutoCreate? @override = null)
        {
            var autoCreate = @override ?? AutoCreate;
            if (autoCreate == AutoCreate.None)
            {
                autoCreate = AutoCreate.CreateOrUpdate;
            }

            var @objects = AllObjects().ToArray();

#if NETSTANDARD2_0
            using var conn = CreateConnection();
#else
            await using var conn = CreateConnection();
#endif
            await conn.OpenAsync().ConfigureAwait(false);

            if (await globalLock.TryAttainLock(conn).ConfigureAwait(false))
            {
                var patch = await SchemaMigration.Determine(conn, @objects).ConfigureAwait(false);

                if (patch.Difference == SchemaPatchDifference.None) return SchemaPatchDifference.None;

                await Migrator.ApplyAll(conn, patch, autoCreate, _logger).ConfigureAwait(false);

                MarkAllFeaturesAsChecked();

                await globalLock.ReleaseLock(conn).ConfigureAwait(false);

                return patch.Difference;
            }

            await conn.CloseAsync().ConfigureAwait(false);
            throw new InvalidOperationException(
                "Unable to attain a global lock in time order to apply database changes");
        }

        public async Task AssertDatabaseMatchesConfigurationAsync()
        {
            var patch = await CreateMigrationAsync().ConfigureAwait(false);
            if (patch.Difference != SchemaPatchDifference.None)
            {
                var writer = new StringWriter();
                patch.WriteAllUpdates(writer, Migrator, AutoCreate.CreateOrUpdate);

                throw new DatabaseValidationException(Identifier,writer.ToString());
            }
        }

        public async Task WriteMigrationFileAsync(string filename)
        {
            var patch = await CreateMigrationAsync().ConfigureAwait(false);
            await Migrator.WriteMigrationFile(filename, patch).ConfigureAwait(false);
        }

        public virtual void ResetSchemaExistenceChecks()
        {
            _checks.Clear();
        }

        public void EnsureStorageExists(Type featureType)
        {
            if (AutoCreate == AutoCreate.None)
            {
                return;
            }


#pragma warning disable VSTHRD002
#if NETSTANDARD2_0
            ensureStorageExists(new List<Type>(), featureType).GetAwaiter().GetResult();
#else
            ensureStorageExists(new List<Type>(), featureType).AsTask().GetAwaiter().GetResult();
#endif

#pragma warning restore VSTHRD002
        }

#if NETSTANDARD2_0
        public Task EnsureStorageExistsAsync(Type featureType, CancellationToken token = default)
        #else

        public ValueTask EnsureStorageExistsAsync(Type featureType, CancellationToken token = default)
        #endif
        {
#if NETSTANDARD2_0
            if (AutoCreate == AutoCreate.None) return Task.CompletedTask;
            #else
            if (AutoCreate == AutoCreate.None) return new ValueTask();
#endif

            return ensureStorageExists(new List<Type>(), featureType, token);
        }


        public virtual IFeatureSchema FindFeature(Type featureType)
        {
            return null; // TODO - could get smarter and try to create by type
        }

        public void MarkAllFeaturesAsChecked()
        {
            foreach (var feature in BuildFeatureSchemas())
            {
                _checks[feature.StorageType] = true;
            }
        }

#if NETSTANDARD2_0
        private async Task ensureStorageExists(IList<Type> types, Type featureType, CancellationToken token = default)
        #else
        private async ValueTask ensureStorageExists(IList<Type> types, Type featureType, CancellationToken token = default)
#endif
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
            {
                await ensureStorageExists(types, dependentType, token).ConfigureAwait(false);
            }

            await generateOrUpdateFeature(featureType, feature, token).ConfigureAwait(false);
        }


#if NETSTANDARD2_0
        protected async Task generateOrUpdateFeature(Type featureType, IFeatureSchema feature, CancellationToken token)
#else
        protected async ValueTask generateOrUpdateFeature(Type featureType, IFeatureSchema feature, CancellationToken token)
#endif
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

            using (await _migrateLocker.Lock(5.Seconds()).ConfigureAwait(false))
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

        private async Task executeMigration(ISchemaObject[] schemaObjects, CancellationToken token = default)
        {

#if NETSTANDARD2_0
            using var conn = _connectionSource();
            #else
            await using var conn = _connectionSource();
#endif
            await conn.OpenAsync(token).ConfigureAwait(false);

            var migration = await SchemaMigration.Determine(conn, schemaObjects).ConfigureAwait(false);

            if (migration.Difference == SchemaPatchDifference.None)
            {
                return;
            }

            migration.AssertPatchingIsValid(AutoCreate);

            await Migrator.ApplyAll(conn, migration, AutoCreate, _logger)
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

}
