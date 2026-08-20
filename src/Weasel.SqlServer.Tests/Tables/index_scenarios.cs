using System.Data.Common;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Weasel.Testing;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     SQL Server's rows of the shared index scenario matrix (weasel#449). SQL Server was the
///     thinnest provider at nine index tests, with the most modeled index properties after
///     PostgreSQL.
/// </summary>
[Collection("integration")]
public class index_scenarios: IndexScenarioMatrix
{
    private const string SchemaName = "indexscenarios";

    protected override async Task<DbConnection> OpenAsync()
    {
        var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    protected override Task ResetSchemaAsync(DbConnection conn)
        => ((SqlConnection)conn).ResetSchemaAsync(SchemaName);

    protected override Migrator CreateMigrator() => new SqlServerMigrator();

    protected override ITable NewTable(string name) => new Table($"{SchemaName}.{name}");

    protected override (int Different, int Extra, int Missing) IndexDifferences(ISchemaObjectDelta delta)
        => delta is TableDelta table
            ? (table.Indexes.Different.Count(), table.Indexes.Extras.Count(), table.Indexes.Missing.Count())
            : (0, 0, 0);

    protected override string DescribeIndexes(ISchemaObjectDelta delta)
        => delta is TableDelta table
            ? string.Join("; ",
                table.Indexes.Different.Select(x =>
                    $"expected [{x.Expected.ToDDL(new Table($"{SchemaName}.ism_partial"))}] actual [{x.Actual.ToDDL(new Table($"{SchemaName}.ism_partial"))}]"))
            : string.Empty;

    /// <summary>SQL Server calls them filtered indexes.</summary>
    protected override bool SupportsPartialIndexes => true;

    protected override bool SupportsIncludedColumns => true;
}
