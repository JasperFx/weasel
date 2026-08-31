using JasperFx;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

public class numeric_upgrade_rebuild
{
    private readonly string _connectionString = $"Data Source={Path.GetTempFileName()};";

    private static Table AmountsTable(string quantityType)
    {
        var table = new Table("nu_amounts");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("quantity", quantityType);
        return table;
    }

    private static async Task ApplyAsync(SqliteConnection conn, Table table)
    {
        var migration = await SchemaMigration.DetermineAsync(conn, CancellationToken.None, table);
        await new SqliteMigrator().ApplyAllAsync(conn, migration, AutoCreate.All);
    }

    private async Task<SqliteConnection> ATableCreatedBeforeTheChangeAsync()
    {
        var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync();
        await conn.ResetSchemaAsync("main");

        await ApplyAsync(conn, AmountsTable("REAL"));

        await conn.CreateCommand("INSERT INTO nu_amounts (id, quantity) VALUES (1, 1)").ExecuteNonQueryAsync();
        await conn.CreateCommand("INSERT INTO nu_amounts (id, quantity) VALUES (2, 42)").ExecuteNonQueryAsync();
        await conn.CreateCommand("INSERT INTO nu_amounts (id, quantity) VALUES (3, 2.5)").ExecuteNonQueryAsync();

        var stored = await conn.CreateCommand("SELECT typeof(quantity) FROM nu_amounts WHERE id = 1")
            .ExecuteScalarAsync();
        stored.ShouldBe("real");

        return conn;
    }

    [Fact]
    public async Task the_first_migration_after_upgrading_needs_a_rebuild()
    {
        await using var conn = await ATableCreatedBeforeTheChangeAsync();

        var delta = await AmountsTable("numeric").FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.CanRebuildInPlace.ShouldBeTrue();
    }

    [Fact]
    public async Task the_rebuild_repairs_the_stored_values()
    {
        await using var conn = await ATableCreatedBeforeTheChangeAsync();

        await ApplyAsync(conn, AmountsTable("numeric"));

        var rows = new List<(string Type, object? Value)>();
        await using (var reader = await conn
                         .CreateCommand("SELECT typeof(quantity), quantity FROM nu_amounts ORDER BY id")
                         .ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                rows.Add((reader.GetString(0), reader.GetValue(1)));
            }
        }

        rows.Count.ShouldBe(3);

        rows[0].Type.ShouldBe("integer");
        Convert.ToDouble(rows[0].Value).ShouldBe(1d);

        rows[1].Type.ShouldBe("integer");
        Convert.ToDouble(rows[1].Value).ShouldBe(42d);

        rows[2].Type.ShouldBe("real");
        Convert.ToDouble(rows[2].Value).ShouldBe(2.5d);
    }

    [Fact]
    public async Task a_second_migration_has_nothing_left_to_do()
    {
        await using var conn = await ATableCreatedBeforeTheChangeAsync();

        await ApplyAsync(conn, AmountsTable("numeric"));

        var delta = await AmountsTable("numeric").FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
