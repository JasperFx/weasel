using System.Data.Common;
using MySqlConnector;
using Weasel.Core;
using Weasel.MySql.Tables;
using Weasel.Testing;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

/// <summary>
///     MySQL's rows of the shared index scenario matrix (weasel#449).
/// </summary>
/// <remarks>
///     MySQL has no partial indexes, so <c>an_unsupported_index_property_is_refused_rather_than_ignored</c>
///     is the live scenario here: <c>Predicate</c> used to be settable and did nothing at all.
/// </remarks>
[Collection("integration")]
public class index_scenarios: IndexScenarioMatrix
{
    private const string SchemaName = "weasel_testing";

    protected override async Task<DbConnection> OpenAsync()
    {
        var conn = new MySqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    /// <summary>
    ///     The schema is the test database itself — the `weasel` user CI connects as has rights on
    ///     nothing else — so tables are dropped individually rather than by dropping the database.
    /// </summary>
    protected override async Task ResetSchemaAsync(DbConnection conn)
    {
        foreach (var table in new[]
                 {
                     "ism_single", "ism_multi", "ism_unique", "ism_reserved", "ism_spaced",
                     "ism_idempotent", "ism_drift", "ism_removal", "ism_partial", "ism_included",
                     "ism_refused"
                 })
        {
            await conn.CreateCommand($"DROP TABLE IF EXISTS `{SchemaName}`.`{table}`").ExecuteNonQueryAsync();
        }
    }

    protected override Migrator CreateMigrator() => new MySqlMigrator();

    protected override ITable NewTable(string name) => new Table($"{SchemaName}.{name}");

    protected override (int Different, int Extra, int Missing) IndexDifferences(ISchemaObjectDelta delta)
        => delta is TableDelta table
            ? (table.Indexes.Different.Count(), table.Indexes.Extras.Count(), table.Indexes.Missing.Count())
            : (0, 0, 0);
}
