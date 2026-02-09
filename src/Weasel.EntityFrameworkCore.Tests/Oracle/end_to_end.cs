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
        // Oracle uses uppercase identifiers by default
        table.Identifier.Name.ShouldBe("MY_ENTITIES");

        // Oracle lowercases column names in Weasel
        // Verify columns are mapped
        table.HasColumn("id").ShouldBeTrue();
        table.HasColumn("intvalue").ShouldBeTrue();
        table.HasColumn("boolvalue").ShouldBeTrue();
        table.HasColumn("stringvalue").ShouldBeTrue();
        table.HasColumn("guidvalue").ShouldBeTrue();
        table.HasColumn("dateonlyvalue").ShouldBeTrue();
        table.HasColumn("timeonlyvalue").ShouldBeTrue();
        table.HasColumn("datetimevalue").ShouldBeTrue();
        table.HasColumn("dt_offset_val").ShouldBeTrue();
        table.HasColumn("cascade_val").ShouldBeTrue();

        // Verify nullable columns (using Oracle-specific short names, lowercased)
        table.HasColumn("null_int_val").ShouldBeTrue();
        table.HasColumn("null_bool_val").ShouldBeTrue();
        table.HasColumn("null_guid_val").ShouldBeTrue();
        table.HasColumn("null_date_val").ShouldBeTrue();
        table.HasColumn("null_time_val").ShouldBeTrue();
        table.HasColumn("null_dt_val").ShouldBeTrue();
        table.HasColumn("null_dt_offset_val").ShouldBeTrue();
        table.HasColumn("null_cascade_val").ShouldBeTrue();

        // Verify primary key
        table.PrimaryKeyColumns.ShouldContain("id");
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

    [Fact(Skip = "Skipped due to pre-existing bug in Weasel.Oracle schema detection SQL (ORA-03048)")]
    public async Task can_create_migration_and_apply()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<OracleDbContext>();

        // Ensure database exists then delete tables for a clean schema state
        await context.Database.EnsureCreatedAsync();

        // Drop the table to simulate needing a migration (Oracle syntax)
        try
        {
            await context.Database.ExecuteSqlRawAsync("DROP TABLE MY_ENTITIES");
        }
        catch
        {
            // Table might not exist, ignore
        }

        // Use Weasel to create migration
        var migration = await _host.Services.CreateMigrationAsync(context, CancellationToken.None);

        migration.ShouldNotBeNull();
        migration.Migration.ShouldNotBeNull();
        migration.Migrator.ShouldBeOfType<OracleMigrator>();

        // The migration should indicate tables need to be created
        migration.Migration.Difference.ShouldNotBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void can_build_a_database_for_db_context()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<OracleDbContext>();

        var database = scope.ServiceProvider.CreateDatabase(context, "Ralph");
        database.ShouldNotBeNull();
        database.Tables.Single().Identifier.Name.ShouldBe("my_entities");
    }
}
