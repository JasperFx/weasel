using JasperFx;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql;

namespace DocSamples;

public class PostgresqlExtensionSamples
{
    public void register_extensions()
    {
        #region sample_pg_register_extensions
        var uuidExt = new Extension("uuid-ossp");
        var plv8Ext = new Extension("plv8");
        var postgisExt = new Extension("postgis");
        #endregion
    }

    #region sample_pg_database_with_extensions
    public class AppDatabaseWithExtensions : PostgresqlDatabase
    {
        public AppDatabaseWithExtensions(NpgsqlDataSource dataSource)
            : base(new DefaultMigrationLogger(), AutoCreate.CreateOrUpdate,
                   new PostgresqlMigrator(), "app", dataSource)
        {
        }

        public override IFeatureSchema[] BuildFeatureSchemas()
        {
            return [new AppFeatureSchema(this)];
        }

        private class AppFeatureSchema : FeatureSchemaBase
        {
            public AppFeatureSchema(AppDatabaseWithExtensions database)
                : base("App", database.Migrator)
            {
            }

            public override Type StorageType => typeof(AppDatabaseWithExtensions);

            protected override IEnumerable<ISchemaObject> schemaObjects()
            {
                // Extensions should be listed first
                yield return new Extension("uuid-ossp");
                yield return new Extension("postgis");

                // Then tables, functions, etc.
                var table = new Weasel.Postgresql.Tables.Table("users");
                table.AddColumn<int>("id").AsPrimaryKey();
                yield return table;
            }
        }
    }
    #endregion

    public void extension_delta_detection()
    {
        #region sample_pg_extension_delta_detection
        var ext = new Extension("hstore");

        // Used internally during migration, but you can invoke manually:
        var migrator = new PostgresqlMigrator();
        var writer = new StringWriter();
        ext.WriteCreateStatement(migrator, writer);
        // CREATE EXTENSION IF NOT EXISTS hstore;
        #endregion
    }

    public void extension_generate_ddl()
    {
        #region sample_pg_extension_generate_ddl
        var ext = new Extension("uuid-ossp");
        var migrator = new PostgresqlMigrator();
        var writer = new StringWriter();

        ext.WriteCreateStatement(migrator, writer);
        // CREATE EXTENSION IF NOT EXISTS uuid-ossp;

        ext.WriteDropStatement(migrator, writer);
        // DROP EXTENSION IF EXISTS uuid-ossp CASCADE;
        #endregion
    }
}
