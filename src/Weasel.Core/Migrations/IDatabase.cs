using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Weasel.Core.Migrations
{

    /// <summary>
    /// Marker interface for a known database with expected schema features
    /// used to drive the migration process
    /// </summary>
    public interface IDatabase
    {
        /// <summary>
        /// Create all known features in order. Dependency relationships are assumed
        /// to be reflected in the order of the feature array
        /// </summary>
        /// <returns></returns>
        IFeatureSchema[] BuildFeatureSchemas();


        AutoCreate AutoCreate { get; }

        /// <summary>
        /// The migrator rules for formatting SQL for this database
        /// </summary>
        Migrator Migrator { get; }

        /// <summary>
        /// Identifying name for Weasel infrastructure and logging
        /// </summary>
        string Identifier { get; }

        /// <summary>
        /// All referenced schema names by the known objects in this database
        /// </summary>
        /// <returns></returns>
        string[] AllSchemaNames();

        /// <summary>
        /// Return an enumerable of all schema objects in dependency order
        /// </summary>
        /// <returns></returns>
        IEnumerable<ISchemaObject> AllObjects();

        /// <summary>
        /// Determine a migration for a single IFeatureSchema
        /// </summary>
        /// <param name="group"></param>
        /// <returns></returns>
        Task<SchemaMigration> CreateMigrationAsync(IFeatureSchema group);

        /// <summary>
        /// Return the SQL script for the entire database configuration as a single string
        /// </summary>
        /// <returns></returns>
        string ToDatabaseScript();

        /// <summary>
        /// Write the SQL creation script to the supplied filename
        /// </summary>
        /// <param name="filename"></param>
        /// <returns></returns>
        Task WriteCreationScriptToFile(string filename);

        /// <summary>
        /// Write the SQL creation script by feature type to the supplied directory
        /// </summary>
        /// <param name="directory"></param>
        Task WriteScriptsByType(string directory);

        /// <summary>
        /// Determine a migration for the configured database against the actual database
        /// </summary>
        /// <returns></returns>
        Task<SchemaMigration> CreateMigrationAsync();

        /// <summary>
        /// Apply all detected changes between configuration and the actual database to the database
        /// </summary>
        /// <param name="override">If supplied, this overrides the AutoCreate threshold of this database</param>
        /// <returns></returns>
        Task<SchemaPatchDifference> ApplyAllConfiguredChangesToDatabaseAsync(AutoCreate? @override = null);

        /// <summary>
        /// Assert that the existing database matches the configured database
        /// </summary>
        /// <returns></returns>
        Task AssertDatabaseMatchesConfigurationAsync();

    }

    public static class DatabaseExtensions
    {
        /// <summary>
        /// Generate a migration, and write the results to the specified file name
        /// </summary>
        /// <param name="database"></param>
        /// <param name="filename"></param>
        public static async Task WriteMigrationFileAsync(this IDatabase database, string filename)
        {
            var migration = await database.CreateMigrationAsync().ConfigureAwait(false);
            await database.Migrator.WriteMigrationFile(filename, migration).ConfigureAwait(false);
        }

        public static Task<SchemaMigration> CreateMigrationAsync(this IDatabase database, Type featureType)
        {
            var feature = database.BuildFeatureSchemas().FirstOrDefault(x => x.StorageType == featureType);
            if (feature == null)
            {
                throw new ArgumentOutOfRangeException(nameof(featureType),
                    $"Type '{featureType.FullName}' is an unknown storage type");
            }

            return database.CreateMigrationAsync(feature);
        }
    }

    public interface IDatabase<T>: IDatabase, IConnectionSource<T> where T : DbConnection
    {

    }

    public interface IMigrationLogger
    {
        void SchemaChange(string sql);

        void OnFailure(DbCommand command, Exception ex);
    }

    public class DefaultMigrationLogger: IMigrationLogger
    {
        public void SchemaChange(string sql)
        {
            Console.WriteLine(sql);
        }

        public void OnFailure(DbCommand command, Exception ex)
        {
            throw ex;
        }
    }
}
