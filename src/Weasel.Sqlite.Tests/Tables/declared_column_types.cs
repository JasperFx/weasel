using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     SQLite stores a column's declared type verbatim and derives its affinity from that text by
///     substring rules, so <c>TIMESTAMP</c> (NUMERIC affinity) and <c>TEXT</c> are different
///     columns. Normalizing the type in the constructor rewrote both the model's intent and what
///     was read back out of an existing database (weasel#532).
/// </summary>
public class declared_column_types
{
    private static async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection($"Data Source={Path.GetTempFileName()};");
        await conn.OpenAsync();
        await conn.ResetSchemaAsync("main");
        return conn;
    }

    private static async Task executeAsync(SqliteConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<string> tableSqlAsync(SqliteConnection conn, string name)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = $"select sql from sqlite_master where type = 'table' and name = '{name}'";
        return (string)(await cmd.ExecuteScalarAsync())!;
    }

    [Theory]
    [InlineData("TIMESTAMP")]
    [InlineData("DATETIME")]
    [InlineData("DATE")]
    [InlineData("BIGINT")]
    [InlineData("BOOLEAN")]
    [InlineData("NVARCHAR(50)")]
    public void the_declared_type_survives_construction(string type)
    {
        new TableColumn("whatever", type).Type.ShouldBe(type);
    }

    [Fact]
    public void the_declared_type_reaches_the_ddl()
    {
        var table = new Table("declared_ddl");
        table.AddColumn("id", "BIGINT").AsPrimaryKey();
        table.AddColumn("created_at", "TIMESTAMP");

        var writer = new StringWriter();
        table.WriteCreateStatement(new SqliteMigrator(), writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("TIMESTAMP");
        ddl.ShouldContain("BIGINT");
    }

    [Fact]
    public async Task the_declared_type_is_what_the_database_gets()
    {
        await using var conn = await openAsync();

        var table = new Table("declared_created");
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("last_update", "TIMESTAMP");
        await table.CreateAsync(conn);

        (await tableSqlAsync(conn, "declared_created")).ShouldContain("TIMESTAMP");
    }

    [Fact]
    public async Task reading_an_existing_table_keeps_its_declared_types()
    {
        await using var conn = await openAsync();
        await executeAsync(conn,
            "create table declared_read (film_id SMALLINT not null, title VARCHAR(255) not null, last_update TIMESTAMP not null)");

        var existing = await new Table("declared_read").FetchExistingAsync(conn);

        existing!.ColumnFor("film_id")!.Type.ShouldBe("SMALLINT");
        existing.ColumnFor("title")!.Type.ShouldBe("VARCHAR(255)");
        existing.ColumnFor("last_update")!.Type.ShouldBe("TIMESTAMP");
    }

    [Fact]
    public async Task a_declared_type_round_trips_without_drift()
    {
        await using var conn = await openAsync();

        var table = new Table("declared_round_trip");
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("last_update", "TIMESTAMP");
        await table.CreateAsync(conn);

        (await table.FetchExistingAsync(conn))!.ColumnFor("last_update")!.Type.ShouldBe("TIMESTAMP");

        var delta = await table.FindDeltaAsync(conn);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    /// <summary>
    ///     Upgrade safety: a database created before the fix holds the collapsed storage class, and
    ///     the model that created it still says <c>DATETIME</c>. Comparison normalizes both sides,
    ///     so that pairing must not report drift — on SQLite a column-type delta rebuilds the table
    ///     and copies every row.
    /// </summary>
    [Theory]
    [InlineData("DATETIME", "TEXT")]
    [InlineData("TIMESTAMP", "DATETIME")]
    [InlineData("BOOLEAN", "INTEGER")]
    [InlineData("BIGINT", "INTEGER")]
    [InlineData("VARCHAR", "TEXT")]
    public async Task a_pre_existing_column_does_not_start_reporting_drift(string modelType, string storedType)
    {
        await using var conn = await openAsync();
        await executeAsync(conn, $"create table declared_upgrade (id INTEGER primary key, moment {storedType})");

        var table = new Table("declared_upgrade");
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("moment", modelType);

        var delta = await table.FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    /// <summary>
    ///     A STRICT table accepts only INT, INTEGER, REAL, TEXT, BLOB and ANY, so there the declared
    ///     type has to be normalized or SQLite rejects the CREATE outright.
    /// </summary>
    [Fact]
    public async Task a_strict_table_still_normalizes_the_declared_type()
    {
        await using var conn = await openAsync();

        var table = new Table("declared_strict") { StrictTypes = true };
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("moment", "DATETIME");
        await table.CreateAsync(conn);

        (await tableSqlAsync(conn, "declared_strict")).ShouldContain("TEXT");

        var delta = await table.FindDeltaAsync(conn);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
