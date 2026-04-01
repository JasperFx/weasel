using JasperFx;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql;

namespace DocSamples;

public class PostgresqlIndexSamples
{
    public void access_provider_singleton()
    {
        #region sample_pg_access_provider_singleton
        // Access the singleton
        var provider = PostgresqlProvider.Instance;

        // Map a .NET type to a PostgreSQL type
        string dbType = provider.GetDatabaseType(typeof(string), EnumStorage.AsInteger);
        // Returns "varchar"
        #endregion
    }

    public void configure_migrator()
    {
        #region sample_pg_configure_migrator
        var migrator = new PostgresqlMigrator
        {
            Formatting = SqlFormatting.Pretty,
            IsTransactional = true
        };
        #endregion
    }

    #region sample_pg_app_database
    public class AppDatabase : PostgresqlDatabase
    {
        public AppDatabase(NpgsqlDataSource dataSource)
            : base(new DefaultMigrationLogger(), AutoCreate.CreateOrUpdate,
                   new PostgresqlMigrator(), "app", dataSource)
        {
        }

        public override IFeatureSchema[] BuildFeatureSchemas()
        {
            // Return your feature schemas containing tables, functions, sequences, etc.
            return Array.Empty<IFeatureSchema>();
        }
    }
    #endregion

    public async Task use_npgsql_datasource()
    {
        #region sample_pg_use_npgsql_datasource
        var dataSourceBuilder = new NpgsqlDataSourceBuilder("Host=localhost;Database=myapp;");
        await using var dataSource = dataSourceBuilder.Build();

        var database = new AppDatabase(dataSource);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();
        #endregion
    }
}
