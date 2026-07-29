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

    public async ValueTask InitializeAsync()
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

    public async ValueTask DisposeAsync()
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

        // MapToTable sets PreserveIdentifierCase, so column names carry EF Core's
        // casing verbatim: the names configured via HasColumnName() where the model
        // gives one, and the CLR property name otherwise. Nothing is lowercased.
        //
        // Asserting the whole set, rather than a series of HasColumn() calls, is
        // deliberate. Oracle's Table compares names with OrdinalIgnoreCase, so
        // HasColumn("intvalue") succeeds against a column named "IntValue" and
        // proves nothing about casing - which is how this test came to assert a
        // lowercase "id" that the mapping has never produced (weasel#394).
        table.Columns.Select(x => x.Name).OrderBy(x => x, StringComparer.Ordinal)
            .ShouldBe([
                "BoolValue",
                "CASCADE_VAL",
                "DT_OFFSET_VAL",
                "DateOnlyValue",
                "DateTimeValue",
                "GuidValue",
                "Id",
                "IntValue",
                "NULL_BOOL_VAL",
                "NULL_CASCADE_VAL",
                "NULL_DATE_VAL",
                "NULL_DT_OFFSET_VAL",
                "NULL_DT_VAL",
                "NULL_GUID_VAL",
                "NULL_INT_VAL",
                "NULL_TIME_VAL",
                "StringValue",
                "TimeOnlyValue"
            ]);

        // MyEntity.Id has no HasColumnName(), so the key column is EF's "Id"
        table.PrimaryKeyColumns.ShouldBe(["Id"]);
        table.PrimaryKeyName.ShouldBe("PK_MY_ENTITIES");
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
        database.Tables.Single().Identifier.Name.ShouldBe("MY_ENTITIES");
    }
}
