using System.Data.Common;
using JasperFx;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql;
using Xunit;
using ITable = Weasel.Core.ITable;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

/// <summary>
///     weasel#629. An EF-derived table is <see cref="ITable.AddOnlyMigrations" />, so a column the
///     mapper could not translate is left in place instead of being read as a column the developer
///     removed. The list of shapes the mapper does not translate is not short -- TPC, entity
///     splitting, temporal period columns, Npgsql enums -- and before this each one of them was a
///     data-loss branch waiting for someone's model to use it.
/// </summary>
/// <remarks>
///     The unmappable column is added by hand here rather than by finding a shape the mapper still
///     cannot express. That is the point: the test has to keep holding as the translation gaps are
///     closed one by one, so it stands for "a column EF knows about and Weasel does not" in general
///     rather than for whichever gap happened to be open the day it was written.
/// </remarks>
[Collection("add_only_ef")]
public class add_only_ef_migrations : IAsyncLifetime
{
    private const string SchemaName = "ef_add_only";
    private const string UnmappableColumn = "period_start";

    private readonly IServiceProvider _services = new ServiceCollection()
        .AddSingleton<Migrator>(new PostgresqlMigrator())
        .BuildServiceProvider();

    public async ValueTask InitializeAsync()
    {
        await using var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString);
        await conn.OpenAsync();
        await conn.CreateCommand($"drop schema if exists {SchemaName} cascade").ExecuteNonQueryAsync();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>
    ///     Create the table from the EF model, then add a column Weasel's model knows nothing
    ///     about -- standing in for any shape the mapper cannot translate.
    /// </summary>
    private async Task ArrangeTableWithAnUnmappableColumnAsync()
    {
        await using var db = new AddOnlyDbContext();
        await using (var migration = await _services.CreateMigrationAsync(db, CancellationToken.None))
        {
            await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, CancellationToken.None);
        }

        await using var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString);
        await conn.OpenAsync();
        await conn.CreateCommand(
                $"""alter table {SchemaName}."AddOnlyOrders" add column "{UnmappableColumn}" timestamptz""")
            .ExecuteNonQueryAsync();
        await conn.CreateCommand(
                $"""insert into {SchemaName}."AddOnlyOrders" ("Id", "Customer", "{UnmappableColumn}") values (1, 'ACME', now())""")
            .ExecuteNonQueryAsync();
    }

    private static async Task<bool> HasColumnAsync(string columnName)
    {
        await using var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString);
        await conn.OpenAsync();
        var count = await conn.CreateCommand(
                "select count(*) from information_schema.columns where table_schema = :schema "
                + "and table_name = 'AddOnlyOrders' and column_name = :column")
            .With("schema", SchemaName)
            .With("column", columnName)
            .ExecuteScalarAsync();

        return Convert.ToInt32(count) == 1;
    }

    [Fact]
    public async Task create_or_update_does_not_drop_a_column_the_mapper_cannot_express()
    {
        await ArrangeTableWithAnUnmappableColumnAsync();

        await using var db = new AddOnlyDbContext();
        await using var migration = await _services.CreateMigrationAsync(db, CancellationToken.None);

        // and not an Update that writes nothing: the unmapped column is not a difference
        migration.Migration.Difference.ShouldBe(SchemaPatchDifference.None);

        await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, CancellationToken.None);

        (await HasColumnAsync(UnmappableColumn)).ShouldBeTrue(
            "the column EF Core knows about and the mapper cannot express must survive the migration");
    }

    [Fact]
    public async Task the_mapped_tables_say_they_are_add_only()
    {
        await using var db = new AddOnlyDbContext();

        var tables = DbContextExtensions
            .GetSchemaObjectsForMigration(db, new PostgresqlMigrator())
            .OfType<ITable>()
            .ToArray();

        tables.ShouldNotBeEmpty();
        tables.ShouldAllBe(x => x.AddOnlyMigrations);
    }

    [Fact]
    public async Task allow_drops_restores_the_drop()
    {
        await ArrangeTableWithAnUnmappableColumnAsync();

        await using var db = new AddOnlyDbContext();
        var customization = new EfSchemaMappingCustomization { AllowDrops = true };

        var tables = DbContextExtensions
            .GetSchemaObjectsForMigration(db, new PostgresqlMigrator(), customization)
            .OfType<ITable>()
            .ToArray();
        tables.ShouldAllBe(x => !x.AddOnlyMigrations);

        await using var migration = await _services.CreateMigrationAsync(db, customization, CancellationToken.None);
        migration.Migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, CancellationToken.None);

        (await HasColumnAsync(UnmappableColumn)).ShouldBeFalse();
    }

    [Fact]
    public async Task an_additive_change_still_applies_to_an_add_only_table()
    {
        await ArrangeTableWithAnUnmappableColumnAsync();

        // the same model plus one property: add-only means "never remove", not "never change"
        await using var db = new AddOnlyWithDiscountDbContext();
        await using var migration = await _services.CreateMigrationAsync(db, CancellationToken.None);

        migration.Migration.Difference.ShouldBe(SchemaPatchDifference.Update);
        await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, CancellationToken.None);

        (await HasColumnAsync("Discount")).ShouldBeTrue();
        (await HasColumnAsync(UnmappableColumn)).ShouldBeTrue();
    }

    [Fact]
    public async Task the_withheld_drop_is_logged_when_a_migration_runs()
    {
        await ArrangeTableWithAnUnmappableColumnAsync();

        await using var db = new AddOnlyWithDiscountDbContext();
        await using var migration = await _services.CreateMigrationAsync(db, CancellationToken.None);
        var logger = new RecordingMigrationLogger();

        await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, CancellationToken.None, logger);

        logger.WithheldDrops.ShouldHaveSingleItem()
            .ShouldContain($"column {UnmappableColumn}");
    }

    private sealed class RecordingMigrationLogger : IMigrationLogger
    {
        public List<string> WithheldDrops { get; } = [];

        public void SchemaChange(string sql)
        {
        }

        public void OnFailure(DbCommand command, Exception ex) => throw ex;
        public void WithheldDrop(string description) => WithheldDrops.Add(description);
    }
}

public class AddOnlyOrder
{
    public int Id { get; set; }
    public string Customer { get; set; } = string.Empty;
    /// <summary>
    ///     Nullable so the added column can be applied with an ALTER: a NOT NULL column with no
    ///     default cannot be added to a table that already has rows, which makes the delta Invalid
    ///     rather than Update and is a different story than this one.
    /// </summary>
    public decimal? Discount { get; set; }
}

/// <summary>
///     Two context TYPES rather than one with a flag: EF Core keys its model cache on the context
///     type, so a second instance of the same type with different constructor arguments silently
///     gets the first instance's model back. Both map the same table, which is the point -- one
///     declares the extra property and one does not.
/// </summary>
public abstract class AddOnlyDbContextBase : DbContext
{
    public DbSet<AddOnlyOrder> Orders => Set<AddOnlyOrder>();

    protected abstract bool WithDiscount { get; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("ef_add_only");

        modelBuilder.Entity<AddOnlyOrder>(entity =>
        {
            entity.ToTable("AddOnlyOrders");
            entity.HasKey(e => e.Id);
            if (!WithDiscount)
            {
                entity.Ignore(e => e.Discount);
            }
        });
    }
}

public class AddOnlyDbContext : AddOnlyDbContextBase
{
    protected override bool WithDiscount => false;
}

public class AddOnlyWithDiscountDbContext : AddOnlyDbContextBase
{
    protected override bool WithDiscount => true;
}
