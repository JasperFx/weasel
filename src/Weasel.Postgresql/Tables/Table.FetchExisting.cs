using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;

namespace Weasel.Postgresql.Tables
{
    public partial class Table
    {
        internal async Task<Table> FetchExisting(NpgsqlConnection conn)
        {
            var cmd = conn.CreateCommand();
            var builder = new CommandBuilder(cmd);

            ConfigureQueryCommand(builder);

            cmd.CommandText = builder.ToString();

            using var reader = await cmd.ExecuteReaderAsync();
            return await readExistingTable(reader);
        }
        
        private async Task<Table> readExistingTable(DbDataReader reader)
        {
            var columns = await readColumns(reader);
            // var pks = readPrimaryKeys(reader);
            // var indexes = readIndexes(reader);
            // var constraints = readConstraints(reader);

            if (!columns.Any())
                return null;

            var existing = new Table(Identifier);
            foreach (var column in columns)
            {
                existing.AddColumn(column);
            }

            // if (pks.Any())
            // {
            //     existing.SetPrimaryKey(pks.First());
            // }
            //
            // existing.ActualIndices = indexes;
            // existing.ActualForeignKeys = constraints;

            return existing;
        }
        
        private static async Task<List<TableColumn>> readColumns(DbDataReader reader)
        {
            var columns = new List<TableColumn>();
            while (await reader.ReadAsync())
            {
                var column = new TableColumn(reader.GetString(0), reader.GetString(1));

                if (column.Type.Equals("user-defined"))
                {
                    column.Type = await reader.GetFieldValueAsync<string>(3);
                }

                if (!await reader.IsDBNullAsync(2))
                {
                    var length = await reader.GetFieldValueAsync<int>(2);
                    column.Type = $"{column.Type}({length})";
                }

                columns.Add(column);
            }
            return columns;
        }
    }
}