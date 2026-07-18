using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.EntityFrameworkCore.Tests.MigrationOperations.SampleGenerated;
using Weasel.EntityFrameworkCore.Tests.Postgresql;
using Xunit;
using PgTable = Weasel.Postgresql.Tables.Table;
using PgIndex = Weasel.Postgresql.Tables.IndexDefinition;

namespace Weasel.EntityFrameworkCore.Tests.MigrationOperations;

/// <summary>
///     #367 acceptance: apply the initial generated migration, change the
///     Weasel model, diff against the snapshot baseline, run the incremental
///     operations through the real Npgsql migrations SQL generator against the
///     database — and Weasel's own delta detection then reports None.
/// </summary>
[Collection("pg-schema-comparison")]
public class incremental_migration_end_to_end : IAsyncLifetime
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

    private static ISchemaObject[] modifiedObjects()
    {
        var objects = SampleWeaselSchema.Objects();
        var orders = objects.OfType<PgTable>().Single(x => x.Identifier.Name == "orders");
        orders.AddColumn<string>("tenant_id").NotNull();
        orders.ColumnFor("tenant_id")!.DefaultExpression = "'*DEFAULT*'";
        orders.Indexes.Add(new PgIndex("idx_orders_tenant") { Columns = new[] { "tenant_id" } });
        return objects;
    }

    [Fact]
    public async Task incremental_operations_apply_and_round_trip()
    {
        var options = SampleWeaselSchema.TranslationOptions();

        // 1. initial migration through the EF runtime (checked-in generated file)
        await using var context = new WeaselSampleDbContext();
        await context.Database.MigrateAsync();

        // 2. snapshot baseline of the initial model, then change the model
        var baseline = EfSchemaSnapshot.FromSchemaObjects(SampleWeaselSchema.Objects(), options);
        var target = EfSchemaSnapshot.FromSchemaObjects(modifiedObjects(), options);

        var diff = EfSnapshotDiffer.Diff(baseline, target, options);
        diff.HasChanges.ShouldBeTrue();

        // 3. run the incremental operations through the REAL provider SQL
        //    generator and execute against the database
        await executeOperationsAsync(context, diff.UpOperations);

        // 4. Weasel's own delta detection agrees the migrated database matches
        //    the changed model
        foreach (var table in modifiedObjects().OfType<PgTable>())
        {
            await using var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString);
            await conn.OpenAsync();
            var delta = await table.FindDeltaAsync(conn);
            delta.HasChanges().ShouldBeFalse(
                $"table {table.Identifier} should round-trip after the incremental migration");
        }

        // 5. and the Down operations take the change back out
        await executeOperationsAsync(context, diff.DownOperations);

        foreach (var table in SampleWeaselSchema.Objects().OfType<PgTable>())
        {
            await using var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString);
            await conn.OpenAsync();
            var delta = await table.FindDeltaAsync(conn);
            delta.HasChanges().ShouldBeFalse(
                $"table {table.Identifier} should match the original model after rollback");
        }
    }

    private static async Task executeOperationsAsync(
        DbContext context,
        IReadOnlyList<Microsoft.EntityFrameworkCore.Migrations.Operations.MigrationOperation> operations)
    {
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var commands = generator.Generate(operations.ToList());

        await using var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString);
        await conn.OpenAsync();
        foreach (var command in commands)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = command.CommandText;
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
