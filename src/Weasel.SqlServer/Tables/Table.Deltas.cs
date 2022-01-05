using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using Weasel.Core;

namespace Weasel.SqlServer.Tables
{
    public partial class Table
    {
        public async Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
        {
            var existing = await readExisting(reader).ConfigureAwait(false);
            return new TableDelta(this, existing);
        }

        public async Task<TableDelta> FindDelta(SqlConnection conn)
        {
            var actual = await FetchExisting(conn).ConfigureAwait(false);
            return new TableDelta(this, actual);
        }
    }
}
