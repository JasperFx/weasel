using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests;

public class SqlServerMigratorTests
{
    [Fact]
    public void matches_sql_connection()
    {
        var migrator = new SqlServerMigrator();

        using var connection = new SqlConnection();
        migrator.MatchesConnection(connection).ShouldBeTrue();
    }

    [Fact]
    public void create_table_returns_sql_server_table()
    {
        var migrator = new SqlServerMigrator();
        var identifier = new SqlServerObjectName("dbo", "test_table");

        var table = migrator.CreateTable(identifier);

        table.ShouldBeOfType<Table>();
        table.Identifier.ShouldBe(identifier);
    }
}
