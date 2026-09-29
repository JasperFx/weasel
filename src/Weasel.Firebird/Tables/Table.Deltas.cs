using System.Data.Common;
using FirebirdSql.Data.FirebirdClient;
using Weasel.Core;

namespace Weasel.Firebird.Tables;

public partial class Table
{
    /// <summary>
    ///     Creates a delta from the four result sets <see cref="ConfigureQueryCommand" /> registers.
    /// </summary>
    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await ReadExistingFromReaderAsync(reader, ct).ConfigureAwait(false);
        return new TableDelta(this, existing);
    }

    public async Task<TableDelta> FindDeltaAsync(FbConnection conn, CancellationToken ct = default)
    {
        var actual = await FetchExistingAsync(conn, ct).ConfigureAwait(false);
        return new TableDelta(this, actual);
    }
}
