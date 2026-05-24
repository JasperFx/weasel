using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Shouldly;
using Weasel.Postgresql;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

/// <summary>
///     Regression coverage for weasel#290 — a property mapped via Npgsql
///     <c>IsRowVersion()</c> resolves to PostgreSQL's implicit <c>xmin</c> system
///     column and must NOT be emitted as a real column (PG rejects
///     <c>CREATE TABLE</c> with "42701 column name 'xmin' conflicts with a system
///     column name"). These are model-mapping tests — they build the EF model
///     offline and exercise <see cref="DbContextExtensions.MapToTable" /> without a
///     live database connection.
/// </summary>
public class row_version_system_column
{
    private static (RowVersionDbContext context, PostgresqlMigrator migrator) build()
    {
        var options = new DbContextOptionsBuilder<RowVersionDbContext>()
            .UseNpgsql(RowVersionDbContext.ConnectionString)
            .Options;
        return (new RowVersionDbContext(options), new PostgresqlMigrator());
    }

    [Fact]
    public void npgsql_maps_row_version_to_the_xmin_system_column()
    {
        // Sanity: confirm the provider still maps IsRowVersion() → xmin, so the
        // skip below is exercising the real scenario.
        var (context, _) = build();
        using var _ctx = context;

        var entityType = context.Model.FindEntityType(typeof(VersionedRecord));
        entityType.ShouldNotBeNull();

        var soi = StoreObjectIdentifier.Table("versioned_records", "ef_rowversion_test");
        var versionColumn = entityType.GetProperties()
            .Single(p => p.Name == nameof(VersionedRecord.Version))
            .GetColumnName(soi);

        versionColumn.ShouldBe("xmin");
    }

    [Fact]
    public void xmin_is_not_emitted_as_a_table_column()
    {
        var (context, migrator) = build();
        using var _ctx = context;

        var entityType = context.Model.FindEntityType(typeof(VersionedRecord));
        entityType.ShouldNotBeNull();

        var table = migrator.MapToTable(entityType);

        table.HasColumn("id").ShouldBeTrue();
        table.HasColumn("name").ShouldBeTrue();
        table.HasColumn("xmin")
            .ShouldBeFalse("xmin is a PostgreSQL system column and must not be created as a real column");
    }
}
