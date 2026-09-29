using System.Data.Common;
using FirebirdSql.Data.FirebirdClient;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Weasel.Testing;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     Firebird's rows of the shared index scenario matrix (weasel#449).
/// </summary>
/// <remarks>
///     <para>
///         The model accepts a partial index on every version, because DDL is written before the server
///         is known, so <see cref="SupportsPartialIndexes" /> is true. On Firebird 3 and 4 the migrator
///         refuses one before anything runs (see <c>index_sort_direction</c>), and the round-trip scenario
///         is skipped there.
///     </para>
///     <para>
///         The schema is the database, so a reset empties it with <c>DropSchemaAsync</c> -- which puts the
///         teardown under every scenario here too.
///     </para>
/// </remarks>
[Collection("integration")]
public class index_scenarios: IndexScenarioMatrix, IAsyncLifetime
{
    private static readonly string ConnectionString = ConnectionSource.ForDatabase(nameof(index_scenarios));
    private FirebirdServerVersion _version;

    public async ValueTask InitializeAsync()
    {
        await ConnectionSource.CreateDatabaseAsync(ConnectionString);

        await using var conn = new FbConnection(ConnectionString);
        await conn.OpenAsync();
        _version = FirebirdServerVersion.Of(conn)!.Value;
    }

    public ValueTask DisposeAsync()
    {
        FbConnection.ClearAllPools();
        return ValueTask.CompletedTask;
    }

    protected override async Task<DbConnection> OpenAsync()
    {
        var conn = new FbConnection(ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    protected override Task ResetSchemaAsync(DbConnection conn) => ((FbConnection)conn).DropSchemaAsync();

    protected override Migrator CreateMigrator() => new FirebirdMigrator();

    protected override ITable NewTable(string name)
    {
        if (name == "ism_partial" && !_version.SupportsPartialIndexes)
        {
            Assert.Skip($"Partial indexes need Firebird 5; this server is Firebird {_version}");
        }

        return new Table(name);
    }

    protected override bool SupportsPartialIndexes => true;

    protected override (int Different, int Extra, int Missing) IndexDifferences(ISchemaObjectDelta delta)
        => delta is TableDelta table
            ? (table.Indexes.Different.Count, table.Indexes.Extras.Count, table.Indexes.Missing.Count)
            : (0, 0, 0);

    protected override string DescribeIndexes(ISchemaObjectDelta delta)
        => delta is TableDelta table
            ? string.Join("; ", table.Indexes.Different.Select(x =>
                  $"expected [{x.Expected.ToDDL(table.Expected)}] actual [{x.Actual.ToDDL(table.Expected)}]"))
              + $" | extras {table.Indexes.Extras.Count} missing {table.Indexes.Missing.Count}"
            : string.Empty;

    /// <summary>Firebird reserves ORDER, and VALUE, which looks just as much like a column.</summary>
    protected override string ReservedWordColumnName => "value";
}
