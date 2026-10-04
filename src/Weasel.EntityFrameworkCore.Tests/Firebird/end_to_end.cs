using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird;
using Xunit;
using CascadeAction = Weasel.Core.CascadeAction;

namespace Weasel.EntityFrameworkCore.Tests.Firebird;

public class end_to_end : IAsyncLifetime
{
    private IHost _host = null!;

    public async ValueTask InitializeAsync()
    {
        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbContext<FirebirdDbContext>(options =>
                    options.UseFirebird(FirebirdDbContext.ConnectionString));

                services.AddSingleton<Migrator, FirebirdMigrator>();
                services.AddDatabaseCleaner<FirebirdDbContext>();
            })
            .Build();

        await _host.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    [Fact]
    public async Task can_map_entity_to_table()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<FirebirdDbContext>();
        var migrator = scope.ServiceProvider.GetRequiredService<Migrator>();

        var entityType = context.Model.FindEntityType(typeof(MyEntity));
        entityType.ShouldNotBeNull();

        var table = migrator.MapToTable(entityType);

        table.ShouldNotBeNull();
        table.Identifier.Name.ShouldBe("my_entities");

        // MapToTable sets PreserveIdentifierCase, so the names carry EF Core's casing verbatim, the way
        // EF Core's own quoted DDL creates them. The whole set is asserted rather than HasColumn() one
        // at a time, because Firebird's Table compares names ignoring case and HasColumn("intvalue")
        // would pass against "IntValue" without proving anything about casing (weasel#394).
        table.Columns.Select(x => x.Name).OrderBy(x => x, StringComparer.Ordinal)
            .ShouldBe([
                "BoolValue",
                "CascadeActionValue",
                "DateOnlyValue",
                "DateTimeOffsetValue",
                "DateTimeValue",
                "GuidValue",
                "Id",
                "IntValue",
                "NullableBoolValue",
                "NullableCascadeActionValue",
                "NullableDateOnlyValue",
                "NullableDateTimeOffsetValue",
                "NullableDateTimeValue",
                "NullableGuidValue",
                "NullableIntValue",
                "NullableTimeOnlyValue",
                "StringValue",
                "TimeOnlyValue"
            ]);

        table.PrimaryKeyColumns.ShouldBe(["Id"]);
        table.PrimaryKeyName.ShouldBe("PK_my_entities");
    }

    [Fact]
    public async Task can_create_table_and_verify_schema()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<FirebirdDbContext>();

        // The database file is this class's alone, so it can be dropped and recreated with the table.
        await context.Database.EnsureDeletedAsync(TestContext.Current.CancellationToken);
        await context.Database.EnsureCreatedAsync(TestContext.Current.CancellationToken);

        var entity = newEntity();
        context.MyEntities.Add(entity);
        await context.SaveChangesAsync(TestContext.Current.CancellationToken);

        var retrieved = await context.MyEntities.FindAsync([entity.Id], TestContext.Current.CancellationToken);
        retrieved.ShouldNotBeNull();
        retrieved.IntValue.ShouldBe(42);
        retrieved.BoolValue.ShouldBeTrue();
        retrieved.StringValue.ShouldBe("test");
        retrieved.CascadeActionValue.ShouldBe(CascadeAction.Cascade);
        retrieved.NullableCascadeActionValue.ShouldBe(CascadeAction.SetNull);
    }

    [Fact]
    public async Task can_create_migration_and_apply()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<FirebirdDbContext>();

        await recreateEmptyDatabaseAsync(context, TestContext.Current.CancellationToken);

        await using var migration = await _host.Services.CreateMigrationAsync(context, TestContext.Current.CancellationToken);

        migration.ShouldNotBeNull();
        migration.Migration.ShouldNotBeNull();
        migration.Migrator.ShouldBeOfType<FirebirdMigrator>();

        // The table is missing, so the migration has something to create
        migration.Migration.Difference.ShouldNotBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_table_weasel_creates_reads_back_unchanged_and_serves_ef_core()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<FirebirdDbContext>();

        await recreateEmptyDatabaseAsync(context, TestContext.Current.CancellationToken);

        await using (var migration = await _host.Services.CreateMigrationAsync(context, TestContext.Current.CancellationToken))
        {
            migration.Migration.Difference.ShouldBe(SchemaPatchDifference.Create);
            await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, TestContext.Current.CancellationToken);
        }

        // EF Core's names are case-preserved, so they are delimited in the DDL and bound exactly in the
        // catalog queries; a folded spelling on either side would read the table as missing or drifted.
        await using (var again = await _host.Services.CreateMigrationAsync(context, TestContext.Current.CancellationToken))
        {
            again.Migration.Difference.ShouldBe(SchemaPatchDifference.None,
                "a table Weasel just created from the EF Core model must read back as matching it");
        }

        // And EF Core can use the table Weasel created, through its own quoted SQL
        var entity = newEntity();
        context.MyEntities.Add(entity);
        await context.SaveChangesAsync(TestContext.Current.CancellationToken);

        var retrieved = await context.MyEntities.AsNoTracking()
            .SingleAsync(x => x.Id == entity.Id, TestContext.Current.CancellationToken);
        retrieved.GuidValue.ShouldBe(entity.GuidValue);
        retrieved.DateOnlyValue.ShouldBe(entity.DateOnlyValue);
        retrieved.TimeOnlyValue.ShouldBe(entity.TimeOnlyValue);
        retrieved.NullableIntValue.ShouldBe(100);
    }

    [Fact]
    public async Task the_database_cleaner_empties_the_tables()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<FirebirdDbContext>();

        await context.Database.EnsureDeletedAsync(TestContext.Current.CancellationToken);
        await context.Database.EnsureCreatedAsync(TestContext.Current.CancellationToken);
        context.MyEntities.Add(newEntity());
        await context.SaveChangesAsync(TestContext.Current.CancellationToken);

        // One EXECUTE BLOCK, run as a single command, which has to find the table by the
        // case-preserved name EF Core gave it
        var cleaner = _host.Services.GetRequiredService<IDatabaseCleaner<FirebirdDbContext>>();
        await cleaner.DeleteAllDataAsync(TestContext.Current.CancellationToken);

        (await context.MyEntities.CountAsync(TestContext.Current.CancellationToken)).ShouldBe(0);
    }

    [Fact]
    public void can_build_a_database_for_db_context()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<FirebirdDbContext>();

        var database = scope.ServiceProvider.CreateDatabase(context, "Ralph");
        database.ShouldNotBeNull();
        database.Tables.Single().Identifier.Name.ShouldBe("my_entities");
    }

    /// <summary>
    ///     An empty database, with no table in it: dropped and created again rather than emptied, because
    ///     Firebird has no <c>DROP TABLE IF EXISTS</c> before version 6, and <c>DROP TABLE</c> refuses a
    ///     table that an idle pooled attachment has read.
    /// </summary>
    private static async Task recreateEmptyDatabaseAsync(FirebirdDbContext context, CancellationToken ct)
    {
        await context.Database.EnsureDeletedAsync(ct);
        FbConnection.ClearAllPools();
        await FbConnection.CreateDatabaseAsync(FirebirdDbContext.ConnectionString, 16384, true, false, ct);
    }

    private static MyEntity newEntity() => new()
    {
        Id = Guid.NewGuid(),
        IntValue = 42,
        BoolValue = true,
        StringValue = "test",
        GuidValue = Guid.NewGuid(),
        DateOnlyValue = new DateOnly(2024, 1, 15),
        TimeOnlyValue = new TimeOnly(10, 30, 0),
        DateTimeValue = new DateTime(2024, 1, 15, 10, 30, 0),
        DateTimeOffsetValue = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(-5)),
        CascadeActionValue = CascadeAction.Cascade,
        NullableIntValue = 100,
        NullableBoolValue = false,
        NullableGuidValue = Guid.NewGuid(),
        NullableDateOnlyValue = new DateOnly(2024, 6, 1),
        NullableTimeOnlyValue = new TimeOnly(14, 0, 0),
        NullableDateTimeValue = new DateTime(2024, 6, 1, 14, 0, 0),
        NullableDateTimeOffsetValue = new DateTimeOffset(2024, 6, 1, 14, 0, 0, TimeSpan.Zero),
        NullableCascadeActionValue = CascadeAction.SetNull
    };
}
