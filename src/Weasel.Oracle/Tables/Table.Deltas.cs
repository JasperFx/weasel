using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;

namespace Weasel.Oracle.Tables;

public partial class Table
{
    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readExistingAsync(reader, ct).ConfigureAwait(false);
        return new TableDelta(this, existing);
    }

    public async Task<TableDelta> FindDeltaAsync(OracleConnection conn, CancellationToken ct = default)
    {
        var actual = await FetchExistingAsync(conn, ct).ConfigureAwait(false);
        return new TableDelta(this, actual);
    }
}
