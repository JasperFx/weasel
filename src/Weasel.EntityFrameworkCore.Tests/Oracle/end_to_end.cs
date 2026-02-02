using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Weasel.Core;
using Weasel.Oracle;
using Xunit;
using CascadeAction = Weasel.Core.CascadeAction;

namespace Weasel.EntityFrameworkCore.Tests.Oracle;

public class end_to_end : IAsyncLifetime
{
    private IHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbContext<OracleDbContext>(options =>
                    options.UseOracle(OracleDbContext.ConnectionString));

                services.AddSingleton<Migrator, OracleMigrator>();
            })
            .Build();

        await _host.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    [Fact]
    public async Task can_map_entity_to_table()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<OracleDbContext>();
        var migrator = scope.ServiceProvider.GetRequiredService<Migrator>();

        var entityType = context.Model.FindEntityType(typeof(MyEntity));
        entityType.ShouldNotBeNull();

        var table = migrator.MapToTable(entityType);

        table.ShouldNotBeNull();
        table.Identifier.Name.ShouldBe("MY_ENTITIES");

        // Verify columns are mapped
        table.HasColumn("Id").ShouldBeTrue();
        table.HasColumn("IntValue").ShouldBeTrue();
        table.HasColumn("BoolValue").ShouldBeTrue();
        table.HasColumn("StringValue").ShouldBeTrue();
        table.HasColumn("GuidValue").ShouldBeTrue();
        table.HasColumn("DateOnlyValue").ShouldBeTrue();
        table.HasColumn("TimeOnlyValue").ShouldBeTrue();
        table.HasColumn("DateTimeValue").ShouldBeTrue();
        table.HasColumn("DT_OFFSET_VAL").ShouldBeTrue();
        table.HasColumn("CASCADE_VAL").ShouldBeTrue();

        // Verify nullable columns (using Oracle-specific short names)
        table.HasColumn("NULL_INT_VAL").ShouldBeTrue();
        table.HasColumn("NULL_BOOL_VAL").ShouldBeTrue();
        table.HasColumn("NULL_GUID_VAL").ShouldBeTrue();
        table.HasColumn("NULL_DATE_VAL").ShouldBeTrue();
        table.HasColumn("NULL_TIME_VAL").ShouldBeTrue();
        table.HasColumn("NULL_DT_VAL").ShouldBeTrue();
        table.HasColumn("NULL_DT_OFFSET_VAL").ShouldBeTrue();
        table.HasColumn("NULL_CASCADE_VAL").ShouldBeTrue();

        // Verify primary key
        table.PrimaryKeyColumns.ShouldContain("Id");
    }

    [Fact]
    public async Task can_create_table_and_verify_schema()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<OracleDbContext>();

        // Ensure database is created and schema is applied
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Verify table exists by inserting and reading data
        var entity = new MyEntity
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

        context.MyEntities.Add(entity);
        await context.SaveChangesAsync();

        // Read back and verify
        var retrieved = await context.MyEntities.FindAsync(entity.Id);
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
        var context = scope.ServiceProvider.GetRequiredService<OracleDbContext>();

        // Ensure clean state
        await context.Database.EnsureDeletedAsync();

        // Use Weasel to create migration
        var migration = await _host.Services.CreateMigrationAsync(context, CancellationToken.None);

        migration.ShouldNotBeNull();
        migration.Migration.ShouldNotBeNull();
        migration.Migrator.ShouldBeOfType<OracleMigrator>();

        // The migration should indicate tables need to be created
        migration.Migration.Difference.ShouldNotBe(SchemaPatchDifference.None);
    }
}
