using Shouldly;
using Weasel.Postgresql;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     Unit coverage for <see cref="PostgresqlMigrator.IsSystemColumn" />, the hook
///     the EF Core integration uses to skip properties EF maps to a PostgreSQL system
///     column (e.g. Npgsql <c>IsRowVersion()</c> → <c>xmin</c>) so they aren't emitted
///     as real columns. weasel#290.
/// </summary>
public class PostgresqlMigratorSystemColumnTests
{
    private readonly PostgresqlMigrator theMigrator = new();

    [Theory]
    [InlineData("xmin")]
    [InlineData("XMIN")] // case-insensitive — PG folds, EF metadata may be either case
    [InlineData("xmax")]
    [InlineData("cmin")]
    [InlineData("cmax")]
    [InlineData("ctid")]
    [InlineData("tableoid")]
    public void recognizes_postgres_system_columns(string columnName)
    {
        theMigrator.IsSystemColumn(columnName).ShouldBeTrue();
    }

    [Theory]
    [InlineData("id")]
    [InlineData("name")]
    [InlineData("version")]
    [InlineData("oid")] // oid is a valid user column on modern PG (12+), not reserved
    public void allows_ordinary_user_columns(string columnName)
    {
        theMigrator.IsSystemColumn(columnName).ShouldBeFalse();
    }
}
