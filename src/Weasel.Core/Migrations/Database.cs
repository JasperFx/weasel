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
            using var conn = CreateConnection();
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

        // TODO -- shorten names
        public void WriteDatabaseCreationScriptByType(string directory)
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
                Migrator.WriteTemplatedFile(directory.AppendPath("schemas.sql"), (m, w) =>
                {
                    m.WriteSchemaCreationSql(schemaNames, w);
                });
            }


            foreach (var feature in BuildFeatureSchemas())
            {
                var scriptName = $"{feature.Identifier}.sql";
                scriptNames.Add(scriptName);

                Migrator.WriteTemplatedFile(directory.AppendPath(scriptName), (m, w) =>
                {
                    feature.WriteFeatureCreation(m, w);
                });
            }

            var writer = new StringWriter();
            foreach (var scriptName in scriptNames)
            {
                writer.WriteLine(Migrator.ToExecuteScriptLine(scriptName));
            }

            var filename = directory.AppendPath("all.sql");
            File.WriteAllText(filename, writer.ToString());

        }

        public async Task<SchemaMigration> CreateMigrationAsync()
        {
            var @objects = AllObjects().ToArray();

            using var conn = CreateConnection();
            await conn.OpenAsync().ConfigureAwait(false);

            return await SchemaMigration.Determine(conn, @objects).ConfigureAwait(false);
        }

        public async Task<SchemaPatchDifference> ApplyAllConfiguredChangesToDatabaseAsync(AutoCreate? @override = null)
        {
            var autoCreate = @override ?? AutoCreate;
            if (@override == AutoCreate.None)
            {
                autoCreate = AutoCreate.CreateOrUpdate;
            }

            var patch = await CreateMigrationAsync().ConfigureAwait(false);

            if (patch.Difference == SchemaPatchDifference.None) return SchemaPatchDifference.None;

            using var conn = CreateConnection();
            await conn.OpenAsync().ConfigureAwait(false);

            await Migrator.ApplyAll(conn, patch, autoCreate, _logger).ConfigureAwait(false);

            MarkAllFeaturesAsChecked();

            return patch.Difference;
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
            Migrator.WriteMigrationFile(filename, patch);
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
            ensureStorageExists(new List<Type>(), featureType).GetAwaiter().GetResult();
#pragma warning restore VSTHRD002
        }

        public Task EnsureStorageExistsAsync(Type featureType, CancellationToken token = default)
        {
            return AutoCreate == AutoCreate.None
                ? Task.CompletedTask
                : ensureStorageExists(new List<Type>(), featureType, token);
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

        private async Task ensureStorageExists(IList<Type> types, Type featureType, CancellationToken token = default)
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
                await ensureStorageExists(types, dependentType, token).ConfigureAwait(false);

            await generateOrUpdateFeature(featureType, feature, token).ConfigureAwait(false);
        }


        protected async Task generateOrUpdateFeature(Type featureType, IFeatureSchema feature, CancellationToken token)
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
            using var conn = _connectionSource();
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
