using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Weasel.SqlServer.Tables
{
    public partial class Table
    {
        public async Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
        {
            var existing = await readExisting(reader);
            return new TableDelta(this, existing);
        }

        public async Task<TableDelta> FindDelta(SqlConnection conn)
        {
            var actual = await FetchExisting(conn);
            return new TableDelta(this, actual);
        }
    }
}