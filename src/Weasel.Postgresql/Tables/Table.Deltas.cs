using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Npgsql;

namespace Weasel.Postgresql.Tables
{
    public partial class Table
    {
        public async Task<TableDelta> FindDelta(NpgsqlConnection conn)
        {
            var actual = await FetchExisting(conn).ConfigureAwait(false);
            return new TableDelta(this, actual);
        }

        public async Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
        {
            var existing = await readExisting(reader).ConfigureAwait(false);
            return new TableDelta(this, existing);
        }
        
    }

}