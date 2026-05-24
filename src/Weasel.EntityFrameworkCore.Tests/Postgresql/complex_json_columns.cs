#if NET10_0_OR_GREATER
using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.Postgresql;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

/// <summary>
///     Regression coverage for weasel#291 — EF Core 10
///     <c>ComplexProperty(...).ToJson()</c> / <c>ComplexCollection(...).ToJson()</c>
///     columns were missing from the Weasel table mapping (only
///     <c>OwnsOne(...).ToJson()</c> was handled). These are model-mapping tests —
///     they build the EF model offline and exercise
///     <see cref="DbContextExtensions.MapToTable" /> without a live database.
/// </summary>
public class complex_json_columns
{
    private static (ComplexJsonDbContext context, PostgresqlMigrator migrator) build()
    {
        var options = new DbContextOptionsBuilder<ComplexJsonDbContext>()
            .UseNpgsql(ComplexJsonDbContext.ConnectionString)
            .Options;
        return (new ComplexJsonDbContext(options), new PostgresqlMigrator());
    }

    [Fact]
    public void maps_complex_property_tojson_to_a_jsonb_container_column()
    {
        var (context, migrator) = build();
        using var _ctx = context;

        var entityType = context.Model.FindEntityType(typeof(Order));
        entityType.ShouldNotBeNull();

        var table = migrator.MapToTable(entityType);

        table.HasColumn("id").ShouldBeTrue();
        table.HasColumn("shipping")
            .ShouldBeTrue("ComplexProperty(...).ToJson() should contribute a container column");

        var pgTable = table.ShouldBeOfType<Weasel.Postgresql.Tables.Table>();
        pgTable.ColumnFor("shipping")!.Type.ShouldBe("jsonb");
    }

    [Fact]
    public void maps_complex_collection_tojson_to_a_jsonb_container_column()
    {
        var (context, migrator) = build();
        using var _ctx = context;

        var entityType = context.Model.FindEntityType(typeof(Invoice));
        entityType.ShouldNotBeNull();

        var table = migrator.MapToTable(entityType);

        table.HasColumn("id").ShouldBeTrue();
        table.HasColumn("lines")
            .ShouldBeTrue("ComplexCollection(...).ToJson() should contribute a container column");

        var pgTable = table.ShouldBeOfType<Weasel.Postgresql.Tables.Table>();
        pgTable.ColumnFor("lines")!.Type.ShouldBe("jsonb");
    }
}
#endif
