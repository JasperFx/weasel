using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;

namespace Weasel.Oracle.Tables;

public partial class Table
{
    /// <summary>
    /// Creates a delta from a DbDataReader. Oracle limitation: this only reads columns,
    /// not PKs, FKs, or indexes, since Oracle doesn't support multiple result sets.
    /// For full schema detection, use FindDeltaAsync instead.
    /// </summary>
    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
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
