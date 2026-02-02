using System.Data.Common;
using MySqlConnector;
using Weasel.Core;

namespace Weasel.MySql.Tables;

public partial class Table
{
    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readExistingAsync(reader, ct).ConfigureAwait(false);
        return new TableDelta(this, existing);
    }

    public async Task<TableDelta> FindDeltaAsync(MySqlConnection conn, CancellationToken ct = default)
    {
        var actual = await FetchExistingAsync(conn, ct).ConfigureAwait(false);
        return new TableDelta(this, actual);
    }
}
