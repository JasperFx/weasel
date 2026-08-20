using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Weasel.Testing;
using Xunit;

namespace Weasel.Oracle.Tests.Tables;

/// <summary>
///     Oracle's rows of the shared index scenario matrix (weasel#449).
/// </summary>
/// <remarks>
///     Oracle has no partial indexes, so <c>an_unsupported_index_property_is_refused_rather_than_ignored</c>
///     is the live scenario here: <c>Predicate</c> used to be settable and did nothing at all.
/// </remarks>
[Collection("integration")]
public class index_scenarios: IndexScenarioMatrix
{
    private const string SchemaName = "WEASEL";

    protected override async Task<DbConnection> OpenAsync()
    {
        var conn = new OracleConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    protected override Task ResetSchemaAsync(DbConnection conn)
        => ((OracleConnection)conn).ResetSchemaAsync(SchemaName);

    protected override Migrator CreateMigrator() => new OracleMigrator();

    protected override ITable NewTable(string name) => new Table($"{SchemaName}.{name}");

    /// <summary>
    ///     Oracle's batched delta path reads columns only — <c>CreateDeltaAsync(DbDataReader)</c>
    ///     says so on the method — because ODP.NET cannot return several result sets from one
    ///     command. So index drift is invisible to <c>SchemaMigration.DetermineAsync</c>, which is
    ///     what <c>ApplyChangesAsync</c> and the whole migration path use. These scenarios go
    ///     through <c>FindDeltaAsync</c>, the only path on Oracle that sees an index at all.
    /// </summary>
    protected override async Task<ISchemaObjectDelta> FindDeltaAsync(DbConnection conn, ITable table)
        => await ((Table)table).FindDeltaAsync((OracleConnection)conn);

    protected override (int Different, int Extra, int Missing) IndexDifferences(ISchemaObjectDelta delta)
        => delta is TableDelta table
            ? (table.Indexes.Different.Count(), table.Indexes.Extras.Count(), table.Indexes.Missing.Count())
            : (0, 0, 0);

    protected override string DescribeIndexes(ISchemaObjectDelta delta)
        => delta is TableDelta table
            ? string.Join("; ", table.Indexes.Different.Select(x =>
                  $"expected [{x.Expected.ToDDL(new Table($"{SchemaName}.ism_single"))}] actual [{x.Actual.ToDDL(new Table($"{SchemaName}.ism_single"))}]"))
              + $" | extras {table.Indexes.Extras.Count()} missing {table.Indexes.Missing.Count()}"
            : string.Empty;

    /// <summary>Oracle reserves ORDER; COMMENT is a reserved word that is also a plausible column.</summary>
    protected override string ReservedWordColumnName => "comment";
}
