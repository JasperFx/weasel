using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.EntityFrameworkCore;
using Weasel.EntityFrameworkCore.CommandLine;
using Weasel.Postgresql;
using PgTable = Weasel.Postgresql.Tables.Table;

namespace DocSamples;

public class EfCoreMigrationSamples
{
    public async Task generate_first_migration(IDatabase database)
    {
        #region sample_efgen_add_migration
        // IDatabase is the source of all schema objects — Marten system
        // tables, Wolverine envelope storage, EF projection tables, ...
        var result = await EfMigrationGenerator.AddAsync(
            database,
            "Initial",
            new EfMigrationGenerationOptions
            {
                Directory = "WeaselMigrations",
                Namespace = "MyApp.WeaselMigrations"
            });

        // first run writes three artifacts:
        // result.MigrationFile  -> 20260718120000_Initial.cs (attribute-only migration)
        // result.ContextFile    -> <Identifier>SchemaDbContext.cs (stub context)
        // result.SnapshotFile   -> weasel-schema-snapshot.json (design-time baseline)
        #endregion
    }

    public void translate_a_table()
    {
        #region sample_efgen_translate_table
        var table = new PgTable("app.orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();

        // translate Weasel schema objects into EF Core MigrationOperation
        // instances — raw store types everywhere, so the DDL EF generates
        // matches Weasel's own
        var options = new MigrationOperationTranslationOptions(EfMigrationProvider.PostgreSql)
        {
            Migrator = new PostgresqlMigrator()
        };

        var operations = new ISchemaObject[] { table }.ToMigrationOperations(options);
        var downOperations = new ISchemaObject[] { table }.ToDropMigrationOperations(options);

        // render as a compilable, attribute-only migration file
        var migration = EfMigrationFileEmitter.EmitMigration(
            "AddOrders", operations, downOperations,
            new EfMigrationEmissionOptions("AppSchemaDbContext"));
        #endregion
    }

    public void diff_against_the_snapshot()
    {
        #region sample_efgen_snapshot_diff
        var options = new MigrationOperationTranslationOptions(EfMigrationProvider.PostgreSql)
        {
            Migrator = new PostgresqlMigrator()
        };

        var table = new PgTable("app.orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();

        // the serialized snapshot is Weasel's analog of EF's ModelSnapshot:
        // design-time JSON written beside the migrations, never compiled
        var baseline = EfSchemaSnapshot.FromSchemaObjects(new ISchemaObject[] { table }, options);
        var json = baseline.ToJson();

        // ... later: the model changed
        table.AddColumn<string>("tenant_id").NotNull();
        table.ColumnFor("tenant_id")!.DefaultExpression = "'*DEFAULT*'";
        var target = EfSchemaSnapshot.FromSchemaObjects(new ISchemaObject[] { table }, options);

        // diff entirely in memory — no live database, no shadow container
        var incremental = EfSnapshotDiffer.Diff(EfSchemaSnapshot.FromJson(json), target, options);
        // incremental.UpOperations   -> AddColumn tenant_id
        // incremental.DownOperations -> DropColumn tenant_id
        #endregion
    }

    public async Task live_database_baseline(IDatabase database)
    {
        #region sample_efgen_live_database_diff
        // the secondary mode: let Weasel's own delta detection diff against
        // the actual database, and wrap the migration SQL in Sql() operations.
        // Handles everything Weasel can migrate — including partition deltas
        // and function changes the snapshot diff refuses.
        var operations = await EfSnapshotDiffer.DiffAgainstDatabaseAsync(database);
        #endregion
    }

    public async Task baseline_an_existing_database(IDatabase database)
    {
        #region sample_efgen_baseline
        // adopt a pre-existing database: record every generated migration file
        // as already applied (history rows only, nothing is executed)
        var recorded = await EfMigrationGenerator.BaselineAsync(
            database,
            new EfMigrationGenerationOptions { Directory = "WeaselMigrations" });
        #endregion
    }
}
