using MySqlConnector;
using Shouldly;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests;

public class MySqlMigratorTests
{
    [Fact]
    public void matches_mysql_connection()
    {
        var migrator = new MySqlMigrator();

        using var connection = new MySqlConnection();
        migrator.MatchesConnection(connection).ShouldBeTrue();
    }

    [Fact]
    public void create_table_returns_mysql_table()
    {
        var migrator = new MySqlMigrator();
        var identifier = new MySqlObjectName("test_db", "test_table");

        var table = migrator.CreateTable(identifier);

        table.ShouldBeOfType<Table>();
        table.Identifier.ShouldBe(identifier);
    }
}
