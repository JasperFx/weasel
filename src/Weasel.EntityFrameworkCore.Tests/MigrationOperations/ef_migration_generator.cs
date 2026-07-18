using JasperFx;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.EntityFrameworkCore.CommandLine;
using Weasel.EntityFrameworkCore.Tests.MigrationOperations.SampleGenerated;
using Weasel.EntityFrameworkCore.Tests.Postgresql;
using Weasel.Postgresql;
using Xunit;
using PgTable = Weasel.Postgresql.Tables.Table;

namespace Weasel.EntityFrameworkCore.Tests.MigrationOperations;

/// <summary>
///     The db-ef-migration engine (#368): first-run scaffolding, incremental
///     add against the snapshot, and baselining an existing database.
/// </summary>
[Collection("pg-schema-comparison")]
public class ef_migration_generator : IDisposable
{
    private readonly string _directory =
        Path.Combine(Path.GetTempPath(), $"weasel-efgen-{Guid.NewGuid():N}");

    /// <summary>Minimal IDatabase over the sample schema objects</summary>
    private class SampleDatabase : PostgresqlDatabase
    {
        private readonly List<ISchemaObject> _objects;

        public SampleDatabase(List<ISchemaObject> objects)
            : base(new DefaultMigrationLogger(), AutoCreate.CreateOrUpdate, new PostgresqlMigrator(),
                "SampleStore", NpgsqlDataSource.Create(PostgresqlDbContext.ConnectionString))
        {
            _objects = objects;
        }

        public override IFeatureSchema[] BuildFeatureSchemas() =>
            new IFeatureSchema[] { new SchemaObjects(_objects) };

        private class SchemaObjects : FeatureSchemaBase
        {
            private readonly List<ISchemaObject> _objects;

            public SchemaObjects(List<ISchemaObject> objects) : base("sample", new PostgresqlMigrator())
            {
                _objects = objects;
            }

            protected override IEnumerable<ISchemaObject> schemaObjects() => _objects;
        }
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }

    private EfMigrationGenerationOptions options() => new()
    {
        Directory = _directory, Namespace = "SampleStore.Migrations"
    };

    [Fact]
    public void detects_the_provider_from_the_migrator()
    {
        var database = new SampleDatabase(SampleWeaselSchema.Objects().ToList());
        EfMigrationGenerator.DetectProvider(database).ShouldBe(EfMigrationProvider.PostgreSql);
    }

    [Fact]
    public void partition_detection_works_structurally()
    {
        var partitioned = new PgTable("p.t");
        partitioned.AddColumn<int>("id").AsPrimaryKey();
        partitioned.AddColumn<string>("tenant_id").AsPrimaryKey();
        partitioned.PartitionByList("tenant_id");

        var plain = new PgTable("p.plain");
        plain.AddColumn<int>("id").AsPrimaryKey();

        EfMigrationGenerator.IsPartitioned(partitioned).ShouldBeTrue();
        EfMigrationGenerator.IsPartitioned(plain).ShouldBeFalse();
    }

    [Fact]
    public async Task first_add_scaffolds_context_migration_and_snapshot_then_incremental_add()
    {
        var objects = SampleWeaselSchema.Objects().ToList();
        var database = new SampleDatabase(objects);

        // first run: create everything
        var first = await EfMigrationGenerator.AddAsync(database, "Initial", options());

        first.HasChanges.ShouldBeTrue();
        first.MigrationId!.ShouldEndWith("_Initial");
        File.Exists(first.MigrationFile!).ShouldBeTrue();
        File.Exists(first.ContextFile!).ShouldBeTrue();
        File.Exists(first.SnapshotFile).ShouldBeTrue();

        var contextCode = await File.ReadAllTextAsync(first.ContextFile!);
        contextCode.ShouldContain("class SampleStoreSchemaDbContext : DbContext");
        contextCode.ShouldContain("UseNpgsql(");
        // history relocated into the sample schema, not public
        contextCode.ShouldContain($"MigrationsHistoryTable(\"__EFMigrationsHistory\", \"{SampleWeaselSchema.SchemaName}\")");

        var migrationCode = await File.ReadAllTextAsync(first.MigrationFile!);
        migrationCode.ShouldContain("[DbContext(typeof(SampleStoreSchemaDbContext))]");
        migrationCode.ShouldContain("migrationBuilder.CreateTable(");

        // no model change → no migration
        var unchanged = await EfMigrationGenerator.AddAsync(database, "Nothing", options());
        unchanged.HasChanges.ShouldBeFalse();

        // change the model → incremental migration with a later id
        var orders = objects.OfType<PgTable>().Single(x => x.Identifier.Name == "orders");
        orders.AddColumn<string>("tenant_id").NotNull();
        orders.ColumnFor("tenant_id")!.DefaultExpression = "'*DEFAULT*'";

        var second = await EfMigrationGenerator.AddAsync(database, "AddTenantId", options());
        second.HasChanges.ShouldBeTrue();
        second.ContextFile.ShouldBeNull();
        string.Compare(second.MigrationId, first.MigrationId, StringComparison.Ordinal).ShouldBeGreaterThan(0);

        var incrementalCode = await File.ReadAllTextAsync(second.MigrationFile!);
        incrementalCode.ShouldContain("migrationBuilder.AddColumn<string>(");
        incrementalCode.ShouldNotContain("migrationBuilder.CreateTable(");

        EfMigrationGenerator.MigrationIdsIn(_directory)
            .ShouldBe(new[] { first.MigrationId, second.MigrationId! });
    }

    [Fact]
    public async Task baseline_records_history_rows_without_executing()
    {
        var database = new SampleDatabase(SampleWeaselSchema.Objects().ToList());

        // reset the history table state
        await using (var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"drop table if exists \"{SampleWeaselSchema.SchemaName}\".\"__EFMigrationsHistory\";";
            await cmd.ExecuteNonQueryAsync();
        }

        var added = await EfMigrationGenerator.AddAsync(database, "Initial", options());

        var recorded = await EfMigrationGenerator.BaselineAsync(database, options());
        recorded.ShouldBe(new[] { added.MigrationId! });

        // idempotent: nothing new to record on a second pass
        (await EfMigrationGenerator.BaselineAsync(database, options())).ShouldBeEmpty();

        // and the row really is in the relocated history table
        await using (var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"select count(*) from \"{SampleWeaselSchema.SchemaName}\".\"__EFMigrationsHistory\" where \"MigrationId\" = @id";
            var p = cmd.CreateParameter();
            p.ParameterName = "@id";
            p.Value = added.MigrationId!;
            cmd.Parameters.Add(p);
            ((long)(await cmd.ExecuteScalarAsync())!).ShouldBe(1);
        }
    }
}
