using System.Data.Common;
using Microsoft.Data.Sqlite;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Weasel.Testing;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     SQLite's rows of the shared index scenario matrix (weasel#449). SQLite had **no** index
///     round-trip test at all before this — nothing created an index, read it back, and asserted
///     the delta was <c>None</c>.
/// </summary>
public class index_scenarios: IndexScenarioMatrix
{
    private readonly string _connectionString = $"Data Source={Path.GetTempFileName()};";

    protected override async Task<DbConnection> OpenAsync()
    {
        var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }

    protected override Task ResetSchemaAsync(DbConnection conn)
        => ((SqliteConnection)conn).ResetSchemaAsync("main");

    protected override Migrator CreateMigrator() => new SqliteMigrator();

    protected override ITable NewTable(string name) => new Table(name);

    protected override (int Different, int Extra, int Missing) IndexDifferences(ISchemaObjectDelta delta)
        => delta is TableDelta table
            ? (table.Indexes.Different.Count(), table.Indexes.Extras.Count(), table.Indexes.Missing.Count())
            : (0, 0, 0);

    protected override string DescribeIndexes(ISchemaObjectDelta delta)
        => delta is TableDelta table
            ? string.Join("; ", table.Indexes.Different.Select(x => $"expected [{x.Expected}] actual [{x.Actual}]"))
            : string.Empty;

    /// <summary>SQLite supports partial indexes with a WHERE clause.</summary>
    protected override bool SupportsPartialIndexes => true;
}
