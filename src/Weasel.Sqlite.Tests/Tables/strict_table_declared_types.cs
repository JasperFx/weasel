using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     A STRICT table accepts only INT, INTEGER, REAL, TEXT, BLOB and ANY. Any other declared type
///     has to be mapped before it reaches the DDL, or SQLite rejects the CREATE outright.
/// </summary>
/// <remarks>
///     Neither half of this was caught when it landed, because neither half is reachable from one
///     change alone. weasel#532 made STRICT the only place a declared type is still normalized, and
///     weasel#533 then made <c>numeric</c> and <c>decimal</c> normalize to NUMERIC -- which STRICT
///     does not accept. Each PR was green on its own; the combination was not exercised because no
///     test declared a numeric column on a STRICT table.
/// </remarks>
public class strict_table_declared_types
{
    private static async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection($"Data Source={Path.GetTempFileName()};");
        await conn.OpenAsync();
        await conn.ResetSchemaAsync("main");
        return conn;
    }

    private static async Task<string> tableSqlAsync(SqliteConnection conn, string name)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = $"select sql from sqlite_master where type = 'table' and name = '{name}'";
        return (string)(await cmd.ExecuteScalarAsync())!;
    }

    private static Table StrictTableWith(string declaredType)
    {
        var table = new Table("strict_declared") { StrictTypes = true };
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("quantity", declaredType);
        return table;
    }

    /// <summary>
    ///     The regression from #533: NUMERIC has no STRICT equivalent, so the CREATE failed with
    ///     <c>unknown datatype for strict_declared.quantity: "NUMERIC"</c>.
    /// </summary>
    [Theory]
    [InlineData("numeric")]
    [InlineData("decimal")]
    public async Task a_numeric_column_can_be_declared_on_a_strict_table(string declaredType)
    {
        await using var conn = await openAsync();

        await StrictTableWith(declaredType).CreateAsync(conn);

        (await tableSqlAsync(conn, "strict_declared")).ShouldContain("ANY");
    }

    /// <summary>
    ///     Pre-existing rather than a regression, but the same defect: a parameterized type never
    ///     matched ConvertSynonyms' switch and was emitted verbatim, which STRICT also rejects.
    /// </summary>
    [Theory]
    [InlineData("VARCHAR(255)")]
    [InlineData("NVARCHAR(50)")]
    public async Task a_parameterized_type_can_be_declared_on_a_strict_table(string declaredType)
    {
        await using var conn = await openAsync();

        await StrictTableWith(declaredType).CreateAsync(conn);

        (await tableSqlAsync(conn, "strict_declared")).ShouldContain("TEXT");
    }

    /// <summary>
    ///     Emission and comparison have to agree. The database holds the type that was emitted, so
    ///     normalizing the two sides differently reports drift on a column that already matches --
    ///     and on SQLite a column-type delta rebuilds the table, so the migration would run, change
    ///     nothing, and be needed again on the next run.
    /// </summary>
    [Theory]
    [InlineData("numeric")]
    [InlineData("decimal")]
    [InlineData("VARCHAR(255)")]
    [InlineData("DATETIME")]
    [InlineData("BIGINT")]
    [InlineData("REAL")]
    public async Task a_strict_table_converges(string declaredType)
    {
        await using var conn = await openAsync();
        await StrictTableWith(declaredType).CreateAsync(conn);

        var delta = await StrictTableWith(declaredType).FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    /// <summary>
    ///     ANY is chosen over REAL for NUMERIC because it is the one STRICT type that keeps a whole
    ///     number an integer, which is the conversion weasel#533 exists to prevent.
    /// </summary>
    [Fact]
    public async Task a_numeric_column_on_a_strict_table_keeps_a_whole_number_an_integer()
    {
        await using var conn = await openAsync();
        await StrictTableWith("numeric").CreateAsync(conn);

        await conn.CreateCommand("insert into strict_declared (id, quantity) values (1, 42)")
            .ExecuteNonQueryAsync();

        (await conn.CreateCommand("select typeof(quantity) from strict_declared").ExecuteScalarAsync())
            .ShouldBe("integer");
    }

    [Theory]
    [InlineData("numeric", "ANY")]
    [InlineData("decimal", "ANY")]
    [InlineData("VARCHAR(255)", "TEXT")]
    [InlineData("datetime", "TEXT")]
    [InlineData("bigint", "INTEGER")]
    [InlineData("real", "REAL")]
    [InlineData("blob", "BLOB")]
    public void the_strict_mapping(string declaredType, string expected)
    {
        SqliteProvider.Instance.ToStrictType(declaredType).ShouldBe(expected);
    }
}
