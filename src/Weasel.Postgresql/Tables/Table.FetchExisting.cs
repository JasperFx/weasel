using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;

namespace Weasel.Postgresql.Tables
{
    public partial class Table
    {
                public void ConfigureQueryCommand(CommandBuilder builder)
        {
            var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
            var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

            builder.Append($@"
select column_name, data_type, character_maximum_length, udt_name
from information_schema.columns where table_schema = :{schemaParam} and table_name = :{nameParam}
order by ordinal_position;

select a.attname, format_type(a.atttypid, a.atttypmod) as data_type
from pg_index i
join   pg_attribute a on a.attrelid = i.indrelid and a.attnum = ANY(i.indkey)
where attrelid = (select pg_class.oid
                  from pg_class
                  join pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
                  where n.nspname = :{schemaParam} and relname = :{nameParam})
and i.indisprimary;

SELECT
  R.rolname                AS user_name,
  ns.nspname               AS schema_name,
  pg_catalog.textin(pg_catalog.regclassout(idx.indrelid :: REGCLASS)) AS table_name,
  i.relname                AS index_name,
  pg_get_indexdef(i.oid) as ddl,
  idx.indisunique          AS is_unique,
  idx.indisprimary         AS is_primary,
  am.amname                AS index_type,
  idx.indkey,
       ARRAY(
           SELECT pg_get_indexdef(idx.indexrelid, k + 1, TRUE)
           FROM
             generate_subscripts(idx.indkey, 1) AS k
           ORDER BY k
       ) AS index_keys,
  (idx.indexprs IS NOT NULL) OR (idx.indkey::int[] @> array[0]) AS is_functional,
  idx.indpred IS NOT NULL AS is_partial
FROM pg_index AS idx
  JOIN pg_class AS i
    ON i.oid = idx.indexrelid
  JOIN pg_am AS am
    ON i.relam = am.oid
  JOIN pg_namespace AS NS ON i.relnamespace = NS.OID
  JOIN pg_roles AS R ON i.relowner = r.oid
WHERE
  nspname = :{schemaParam} AND
  NOT nspname LIKE 'pg%';

SELECT c.conname                                     AS constraint_name,
       c.contype                                     AS constraint_type,
       sch.nspname                                   AS schema_name,
       tbl.relname                                   AS table_name,
       ARRAY_AGG(col.attname ORDER BY u.attposition) AS columns,
       pg_get_constraintdef(c.oid)                   AS definition
FROM pg_constraint c
       JOIN LATERAL UNNEST(c.conkey) WITH ORDINALITY AS u(attnum, attposition) ON TRUE
       JOIN pg_class tbl ON tbl.oid = c.conrelid
       JOIN pg_namespace sch ON sch.oid = tbl.relnamespace
       JOIN pg_attribute col ON (col.attrelid = tbl.oid AND col.attnum = u.attnum)
WHERE
	c.contype = 'f' and
	sch.nspname = :{schemaParam} and
	tbl.relname = :{nameParam}
GROUP BY constraint_name, constraint_type, schema_name, table_name, definition;

");
        }
        

        
        
        internal async Task<Table> FetchExisting(NpgsqlConnection conn)
        {
            var cmd = conn.CreateCommand();
            var builder = new CommandBuilder(cmd);

            ConfigureQueryCommand(builder);

            builder.Compile();

            using var reader = await cmd.ExecuteReaderAsync();
            return await readExistingTable(reader);
        }
        
        private async Task<Table> readExistingTable(DbDataReader reader)
        {
            var columns = await readColumns(reader);
            var pks = await readPrimaryKeys(reader);
            var indexes = await readIndexes(reader);
            var constraints = await readConstraints(reader);

            if (!columns.Any())
                return null;

            var existing = new Table(Identifier);
            foreach (var column in columns)
            {
                existing.AddColumn(column);
            }

            foreach (var pkColumn in pks)
            {
                existing.ColumnFor(pkColumn).IsPrimaryKey = true;
            }

            existing.ActualIndices = indexes;
            existing.ActualForeignKeys = constraints;

            return existing;
        }
        
        private static async Task<List<TableColumn>> readColumns(DbDataReader reader)
        {
            var columns = new List<TableColumn>();
            while (await reader.ReadAsync())
            {
                var column = new TableColumn(await reader.GetFieldValueAsync<string>(0), await reader.GetFieldValueAsync<string>(1));

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
        
        private async Task<List<ActualForeignKey>> readConstraints(DbDataReader reader)
        {
            await reader.NextResultAsync();
            var constraints = new List<ActualForeignKey>();
            while (await reader.ReadAsync())
            {
                constraints.Add(new ActualForeignKey(Identifier, await reader.GetFieldValueAsync<string>(0), await reader.GetFieldValueAsync<string>(5)));
            }

            return constraints;
        }

        private async Task<Dictionary<string, ActualIndex>> readIndexes(DbDataReader reader)
        {
            var dict = new Dictionary<string, ActualIndex>();

            await reader.NextResultAsync();
            while (await reader.ReadAsync())
            {
                if (await reader.IsDBNullAsync(2))
                    continue;
                
                var isPrimary = await reader.GetFieldValueAsync<bool>(6);
                if (isPrimary) continue;

                var schemaName = await reader.GetFieldValueAsync<string>(1);
                var tableName = await reader.GetFieldValueAsync<string>(2);

                

                if ((Identifier.Schema == schemaName && Identifier.Name == tableName) || Identifier.QualifiedName == tableName)
                {
                    var index = new ActualIndex(Identifier, await reader.GetFieldValueAsync<string>(3),
                        await reader.GetFieldValueAsync<string>(4));

                    dict.Add(index.Name, index);
                }
            }

            return dict;
        }
        
        private static async Task<List<string>> readPrimaryKeys(DbDataReader reader)
        {
            var pks = new List<string>();
            await reader.NextResultAsync();
            while (await reader.ReadAsync())
            {
                pks.Add(await reader.GetFieldValueAsync<string>(0));
            }
            return pks;
        }

        
    }
    
    
}