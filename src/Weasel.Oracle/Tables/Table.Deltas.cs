using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;

namespace Weasel.Oracle.Tables;

public partial class Table
{
    /// <summary>
    ///     Creates a delta from the six result sets <see cref="ConfigureQueryCommand" /> registers:
    ///     columns, primary key, foreign keys, index metadata, index expressions, index columns.
    /// </summary>
    /// <remarks>
    ///     This used to read columns only, because ODP.NET will not execute several statements from
    ///     one command and a schema object could therefore register one query. Indexes, foreign keys
    ///     and the primary key were invisible to every caller that went through
    ///     <c>SchemaMigration.DetermineAsync</c> — the entire migration path — and only
    ///     <see cref="FetchExistingAsync" /> saw them (weasel#474).
    /// </remarks>
    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await ReadExistingFromReaderAsync(reader, ct).ConfigureAwait(false);
        return new TableDelta(this, existing);
    }

    public async Task<TableDelta> FindDeltaAsync(OracleConnection conn, CancellationToken ct = default)
    {
        var actual = await FetchExistingAsync(conn, ct).ConfigureAwait(false);
        return new TableDelta(this, actual);
    }
}
