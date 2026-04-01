using System.Data.Common;
using JasperFx;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace DocSamples;

#region sample_IDatabase_interface
public interface IDatabase_Sample
{
    AutoCreate AutoCreate { get; }
    Migrator Migrator { get; }
    string Identifier { get; }
    List<string> TenantIds { get; }

    IFeatureSchema[] BuildFeatureSchemas();
    string[] AllSchemaNames();
    IEnumerable<ISchemaObject> AllObjects();

    Task<SchemaMigration> CreateMigrationAsync(CancellationToken ct = default);
    Task<SchemaMigration> CreateMigrationAsync(IFeatureSchema group, CancellationToken ct = default);

    Task<SchemaPatchDifference> ApplyAllConfiguredChangesToDatabaseAsync(
        AutoCreate? @override = null,
        ReconnectionOptions? reconnectionOptions = null,
        CancellationToken ct = default);

    Task AssertDatabaseMatchesConfigurationAsync(CancellationToken ct = default);
    string ToDatabaseScript();
}
#endregion

#region sample_IFeatureSchema_interface
public interface IFeatureSchema_Sample
{
    ISchemaObject[] Objects { get; }
    string Identifier { get; }
    Migrator Migrator { get; }
    Type StorageType { get; }
}
#endregion

#region sample_IMigrationLogger_interface
public interface IMigrationLogger_Sample
{
    void SchemaChange(string sql);
    void OnFailure(DbCommand command, Exception ex);
}
#endregion

public class SchemaMigrationSamples
{
    public async Task check_migration_result(IDatabase database)
    {
        #region sample_check_migration_result
        var migration = await database.CreateMigrationAsync();

        // Check the overall result
        if (migration.Difference == SchemaPatchDifference.None)
        {
            // Database is up to date
        }
        #endregion
    }

    public void set_autocreate_policy(DatabaseBase<Npgsql.NpgsqlConnection> database)
    {
        #region sample_set_autocreate_policy
        // In development -- let Weasel manage everything
        database.AutoCreate = AutoCreate.All;

        // In production -- fail fast if the schema is wrong
        database.AutoCreate = AutoCreate.None;
        #endregion
    }

    public async Task override_autocreate_policy(IDatabase database)
    {
        #region sample_override_autocreate_policy
        await database.ApplyAllConfiguredChangesToDatabaseAsync(
            @override: AutoCreate.CreateOrUpdate
        );
        #endregion
    }

    public async Task typical_migration_workflow(Npgsql.NpgsqlDataSource dataSource)
    {
        #region sample_typical_migration_workflow
        // 1. Configure your database with schema objects
        var database = new MyPostgresqlDatabase(dataSource);

        // 2. Apply all changes (respects AutoCreate policy)
        var result = await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // result is SchemaPatchDifference.None if no changes were needed
        #endregion
    }

    public async Task generate_migration_script(DatabaseBase<Npgsql.NpgsqlConnection> database)
    {
        #region sample_generate_migration_script
        // Generate a migration script file
        await database.WriteMigrationFileAsync("migrations/next.sql");

        // Or get the full creation script
        var script = database.ToDatabaseScript();
        #endregion
    }
}

// Stub class for the sample to compile
public class MyPostgresqlDatabase: Weasel.Postgresql.PostgresqlDatabase
{
    public MyPostgresqlDatabase(Npgsql.NpgsqlDataSource dataSource)
        : base(new DefaultMigrationLogger(), AutoCreate.All, new Weasel.Postgresql.PostgresqlMigrator(), "mydb", dataSource)
    {
    }

    public override IFeatureSchema[] BuildFeatureSchemas()
    {
        return Array.Empty<IFeatureSchema>();
    }
}
