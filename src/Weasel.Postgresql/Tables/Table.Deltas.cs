using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Npgsql;

namespace Weasel.Postgresql.Tables
{
    public partial class Table
    {
        internal async Task<TableDelta> FindDelta(NpgsqlConnection conn)
        {
            var actual = await FetchExisting(conn);
            return new TableDelta(this, actual);
        }

        public async Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
        {
            var existing = await readExisting(reader);
            return new TableDelta(this, existing);
        }
        
    }

}