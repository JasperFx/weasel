using Oracle.ManagedDataAccess.Client;
using Shouldly;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests;

public class OracleMigratorTests
{
    [Fact]
    public void matches_oracle_connection()
    {
        var migrator = new OracleMigrator();

        using var connection = new OracleConnection();
        migrator.MatchesConnection(connection).ShouldBeTrue();
    }

    [Fact]
    public void create_table_returns_oracle_table()
    {
        var migrator = new OracleMigrator();
        var identifier = new OracleObjectName("TEST_SCHEMA", "TEST_TABLE");

        var table = migrator.CreateTable(identifier);

        table.ShouldBeOfType<Table>();
        table.Identifier.ShouldBe(identifier);
    }
}
