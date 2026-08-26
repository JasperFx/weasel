using JasperFx;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     <c>STRICT</c>, <c>WITHOUT ROWID</c> and <c>AUTOINCREMENT</c> live only in the stored
///     <c>CREATE TABLE</c> text — no pragma reports them — and the introspection fetched that text
///     and then discarded it. On SQLite that is not merely a lossy read: changing a column rebuilds
///     the table from the model, and the rebuild dropped the two table options even when the model
///     it was building from still declared them.
/// </summary>
public class table_shape_introspection
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

    [Fact]
    public async Task autoincrement_is_read_back()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table shape_ai (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)");

        var existing = await new Table("shape_ai").FetchExistingAsync(conn);

        existing!.ColumnFor("id")!.IsAutoNumber.ShouldBeTrue();
    }

    [Fact]
    public async Task a_plain_integer_key_is_not_reported_as_autoincrement()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table shape_plain (id INTEGER PRIMARY KEY, name TEXT)");

        var existing = await new Table("shape_plain").FetchExistingAsync(conn);

        existing!.ColumnFor("id")!.IsAutoNumber.ShouldBeFalse();
    }

    [Fact]
    public async Task strict_and_without_rowid_are_read_back()
    {
        await using var conn = await openAsync();
        await executeAsync(conn,
            "create table shape_sr (id INTEGER NOT NULL, v TEXT, PRIMARY KEY (id)) WITHOUT ROWID, STRICT");

        var existing = await new Table("shape_sr").FetchExistingAsync(conn);

        existing!.WithoutRowId.ShouldBeTrue();
        existing.StrictTypes.ShouldBeTrue();
    }

    [Fact]
    public async Task an_ordinary_table_reports_neither()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table shape_ordinary (id INTEGER PRIMARY KEY, v TEXT)");

        var existing = await new Table("shape_ordinary").FetchExistingAsync(conn);

        existing!.WithoutRowId.ShouldBeFalse();
        existing.StrictTypes.ShouldBeFalse();
    }

    /// <summary>
    ///     A CHECK constraint puts parentheses inside the column list, so the table options have to
    ///     be looked for past the column list's own closing paren, not past the last one.
    /// </summary>
    [Fact]
    public async Task a_check_constraint_does_not_hide_the_table_options()
    {
        await using var conn = await openAsync();
        await executeAsync(conn,
            "create table shape_check (id INTEGER NOT NULL, qty INT CHECK (qty > (0)), PRIMARY KEY (id)) WITHOUT ROWID");

        var existing = await new Table("shape_check").FetchExistingAsync(conn);

        existing!.WithoutRowId.ShouldBeTrue();
        existing.StrictTypes.ShouldBeFalse();
    }

    [Fact]
    public async Task rebuilding_a_strict_table_keeps_it_strict()
    {
        await using var conn = await openAsync();

        var table = new Table("shape_rebuild") { StrictTypes = true };
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("qty", "INTEGER");
        await table.CreateAsync(conn);

        var changed = new Table("shape_rebuild") { StrictTypes = true };
        changed.AddColumn("id", "INTEGER").AsPrimaryKey();
        changed.AddColumn("qty", "TEXT");

        var migration = await SchemaMigration.DetermineAsync(conn, CancellationToken.None, changed);
        await new SqliteMigrator().ApplyAllAsync(conn, migration, AutoCreate.All);

        (await tableSqlAsync(conn, "shape_rebuild")).ShouldContain("STRICT");
        (await changed.FetchExistingAsync(conn))!.StrictTypes.ShouldBeTrue();
    }

    [Fact]
    public async Task rebuilding_a_without_rowid_table_keeps_it_without_rowid()
    {
        await using var conn = await openAsync();

        var table = new Table("shape_rebuild2") { WithoutRowId = true };
        table.AddColumn("id", "TEXT").AsPrimaryKey();
        table.AddColumn("qty", "INTEGER");
        await table.CreateAsync(conn);

        var changed = new Table("shape_rebuild2") { WithoutRowId = true };
        changed.AddColumn("id", "TEXT").AsPrimaryKey();
        changed.AddColumn("qty", "TEXT");

        var migration = await SchemaMigration.DetermineAsync(conn, CancellationToken.None, changed);
        await new SqliteMigrator().ApplyAllAsync(conn, migration, AutoCreate.All);

        (await tableSqlAsync(conn, "shape_rebuild2")).ShouldContain("WITHOUT ROWID");
        (await changed.FetchExistingAsync(conn))!.WithoutRowId.ShouldBeTrue();
    }

    /// <summary>
    ///     Upgrade safety: neither option is part of the comparison, so reading them cannot start
    ///     reporting drift on a table that was already converged.
    /// </summary>
    [Fact]
    public async Task reading_the_shape_does_not_introduce_drift()
    {
        await using var conn = await openAsync();

        var table = new Table("shape_converge") { StrictTypes = true };
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("qty", "INTEGER");
        await table.CreateAsync(conn);

        var delta = await table.FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    [Fact]
    public async Task an_autoincrement_table_still_converges()
    {
        await using var conn = await openAsync();

        var table = new Table("shape_ai2");
        table.AddColumn("id", "INTEGER").AsPrimaryKey().AutoIncrement();
        table.AddColumn<string>("name");
        await table.CreateAsync(conn);

        var delta = await table.FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }
}
