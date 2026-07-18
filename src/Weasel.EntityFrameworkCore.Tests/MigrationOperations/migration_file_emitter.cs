using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Npgsql;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.MigrationOperations.SampleGenerated;
using Weasel.EntityFrameworkCore.Tests.Postgresql;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.MigrationOperations;

/// <summary>
///     The C# migration file emitter (#366). The generated sample files are
///     checked in next to SampleWeaselSchema and compiled as part of this test
///     project — which IS the "generated files compile" acceptance — and the
///     drift-guard test proves they are byte-for-byte what the emitter produces.
/// </summary>
public class migration_file_emitter
{
    // resolved from the test output directory rather than [CallerFilePath]:
    // deterministic CI builds rewrite caller paths to a virtual /_/ root that
    // does not exist on disk
    private static string sampleDirectory()
    {
        var dir = AppContext.BaseDirectory;
        while (!File.Exists(Path.Combine(dir, "Weasel.slnx")))
        {
            dir = Directory.GetParent(dir)!.FullName;
        }

        return Path.Combine(dir, "src", "Weasel.EntityFrameworkCore.Tests", "MigrationOperations",
            "SampleGenerated");
    }

    [Fact]
    public void checked_in_migration_file_matches_regenerated_output()
    {
        var migration = SampleWeaselSchema.GenerateMigration();

        migration.MigrationId.ShouldBe("20260718120000_WeaselSampleSchema");
        migration.FileName.ShouldBe("20260718120000_WeaselSampleSchema.cs");

        var checkedIn = File.ReadAllText(Path.Combine(sampleDirectory(), migration.FileName));
        migration.Code.ShouldBe(checkedIn);
    }

    [Fact]
    public void checked_in_stub_context_matches_regenerated_output()
    {
        var checkedIn = File.ReadAllText(
            Path.Combine(sampleDirectory(), $"{SampleWeaselSchema.ContextTypeName}.cs"));

        SampleWeaselSchema.GenerateStubContext().ShouldBe(checkedIn);
    }

    [Fact]
    public void migration_ids_are_bumped_past_the_previous_id()
    {
        var options = SampleWeaselSchema.EmissionOptions();
        options.LastMigrationId = "20260718120000_WeaselSampleSchema";

        var migration = EfMigrationFileEmitter.EmitMigration(
            "SecondMigration",
            Array.Empty<MigrationOperation>(),
            Array.Empty<MigrationOperation>(),
            options);

        // same configured second as the previous id — bumped forward so the
        // plain string sort EF uses puts it after
        migration.MigrationId.ShouldBe("20260718120001_SecondMigration");
        string.Compare(migration.MigrationId, options.LastMigrationId, StringComparison.Ordinal)
            .ShouldBeGreaterThan(0);
    }

    [Fact]
    public void migration_names_are_sanitized_into_class_names()
    {
        var migration = EfMigrationFileEmitter.EmitMigration(
            "add tenant-id!",
            Array.Empty<MigrationOperation>(),
            Array.Empty<MigrationOperation>(),
            SampleWeaselSchema.EmissionOptions());

        migration.ClassName.ShouldBe("add_tenant_id_");
        migration.Code.ShouldContain("public partial class add_tenant_id_ : Migration");
    }

    [Fact]
    public void sql_operations_render_as_verbatim_strings()
    {
        var sql = new SqlOperation { Sql = "comment on table t is 'has \"quotes\"';" };

        var migration = EfMigrationFileEmitter.EmitMigration(
            "RawSql", new[] { sql }, Array.Empty<MigrationOperation>(),
            SampleWeaselSchema.EmissionOptions());

        migration.Code.ShouldContain("migrationBuilder.Sql(@\"comment on table t is 'has \"\"quotes\"\"';\");");
    }

    [Fact]
    public void reserved_word_column_names_are_escaped()
    {
        var createTable = new CreateTableOperation { Name = "t" };
        createTable.Columns.Add(new AddColumnOperation
        {
            Name = "default", Table = "t", ClrType = typeof(int), ColumnType = "integer", IsNullable = false
        });
        createTable.PrimaryKey = new AddPrimaryKeyOperation
        {
            Name = "pk_t", Table = "t", Columns = new[] { "default" }
        };

        var migration = EfMigrationFileEmitter.EmitMigration(
            "Reserved", new MigrationOperation[] { createTable }, Array.Empty<MigrationOperation>(),
            SampleWeaselSchema.EmissionOptions());

        migration.Code.ShouldContain("@default = table.Column<int>(");
        migration.Code.ShouldContain("table.PrimaryKey(\"pk_t\", x => x.@default);");
    }

    [Fact]
    public void sql_server_stub_context_uses_the_sql_server_provider()
    {
        var code = EfMigrationFileEmitter.EmitStubContext(
            EfMigrationProvider.SqlServer,
            new EfMigrationEmissionOptions("WolverineSchemaDbContext"),
            "wolverine");

        code.ShouldContain("UseSqlServer(");
        code.ShouldContain("MigrationsHistoryTable(\"__EFMigrationsHistory\", \"wolverine\")");
        code.ShouldContain("class WolverineSchemaDbContext : DbContext");
        code.ShouldContain("class WolverineSchemaDbContextFactory : IDesignTimeDbContextFactory<WolverineSchemaDbContext>");
        code.ShouldContain("PendingModelChangesWarning");
    }
}

/// <summary>
///     End-to-end: the checked-in generated migration + stub context are applied
///     through the real EF runtime against PostgreSQL, round-tripped against
///     Weasel's own delta detection, and migrated back down.
/// </summary>
[Collection("pg-schema-comparison")]
public class generated_migration_end_to_end : IAsyncLifetime
{
    public async Task InitializeAsync()
    {
        WeaselSampleDbContext.ConnectionString = PostgresqlDbContext.ConnectionString;

        await using var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"drop schema if exists {SampleWeaselSchema.SchemaName} cascade;";
        await cmd.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task applies_via_ef_round_trips_against_weasel_and_migrates_down()
    {
        await using var context = new WeaselSampleDbContext();

        await context.Database.MigrateAsync();

        var applied = (await context.Database.GetAppliedMigrationsAsync()).ToArray();
        applied.ShouldBe(new[] { "20260718120000_WeaselSampleSchema" });

        // the EF-created schema must satisfy Weasel's own delta detection
        await using (var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString))
        {
            await conn.OpenAsync();
            foreach (var table in SampleWeaselSchema.Objects().OfType<Weasel.Postgresql.Tables.Table>())
            {
                var delta = await table.FindDeltaAsync(conn);
                delta.HasChanges().ShouldBeFalse(
                    $"table {table.Identifier} should round-trip, but had {delta.Difference}");
            }
        }

        // and Down() takes it all back out
        await context.GetService<IMigrator>().MigrateAsync("0");

        await using (var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"select count(*) from information_schema.tables where table_schema = '{SampleWeaselSchema.SchemaName}' and table_name in ('customers', 'orders')";
            var count = (long)(await cmd.ExecuteScalarAsync())!;
            count.ShouldBe(0);
        }
    }
}
