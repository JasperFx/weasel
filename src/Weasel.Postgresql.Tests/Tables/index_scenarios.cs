using System.Data.Common;
using Npgsql;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Testing;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     PostgreSQL's rows of the shared index scenario matrix (weasel#449).
/// </summary>
[Collection("indexscenarios")]
public class index_scenarios: IndexScenarioMatrix
{
    private const string SchemaName = "indexscenarios";

    protected override async Task<DbConnection> OpenAsync()
    {
        var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    protected override Task ResetSchemaAsync(DbConnection conn)
        => ((NpgsqlConnection)conn).ResetSchemaAsync(SchemaName);

    protected override Migrator CreateMigrator() => new PostgresqlMigrator();

    protected override ITable NewTable(string name) => new Table($"{SchemaName}.{name}");

    protected override (int Different, int Extra, int Missing) IndexDifferences(ISchemaObjectDelta delta)
        => delta is TableDelta table
            ? (table.Indexes.Different.Count(), table.Indexes.Extras.Count(), table.Indexes.Missing.Count())
            : (0, 0, 0);

    protected override bool SupportsPartialIndexes => true;

    /// <summary>PostgreSQL 11+ supports INCLUDE on a btree index.</summary>
    protected override bool SupportsIncludedColumns => true;
}
