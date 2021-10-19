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

select a.attname, format_type(a.atttypid, a.atttypmod) as data_type, c.relname as pk_name
from pg_index i
join   pg_attribute a on a.attrelid = i.indrelid and a.attnum = ANY(i.indkey)
join   pg_class c on c.oid = i.indexrelid
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

            if (PartitionStrategy != PartitionStrategy.None)
            {
                builder.Append($@"
select
    col.column_name,
    partition_strategy
from
    (select
         partrelid,
         partnatts,
         case partstrat
             when 'l' then 'list'
             when 'r' then 'range' end as partition_strategy,
         unnest(partattrs) column_index
     from
         pg_partitioned_table) pt
        join
    pg_class par
    on
            par.oid = pt.partrelid
        join
    information_schema.columns col
    on
                col.table_schema = par.relnamespace::regnamespace::text
            and col.table_name = par.relname
            and ordinal_position = pt.column_index
where
    col.table_schema = :{schemaParam} and table_name = :{nameParam}
order by column_index;
");
            }
        }

        public async Task<Table?> FetchExisting(NpgsqlConnection conn)
        {
            var builder = new CommandBuilder();

            ConfigureQueryCommand(builder);

            using var reader = await builder.ExecuteReaderAsync(conn);
            return await readExisting(reader);
        }
        
        private async Task<Table?> readExisting(DbDataReader reader)
        {
            var existing = new Table(Identifier);
            
            await readColumns(reader, existing);

            var pks = await readPrimaryKeys(reader);
            await readIndexes(reader, existing);
            await readConstraints(reader, existing);

            foreach (var pkColumn in pks.Columns)
            {
                existing.ColumnFor(pkColumn)!.IsPrimaryKey = true;
            }
            existing.PrimaryKeyName = pks.PrimaryKeyName!;

            if (PartitionStrategy != PartitionStrategy.None)
            {
                await readPartitions(reader, existing);
            }
            
            return !existing.Columns.Any() 
                ? null 
                : existing;
        }

        private async Task readPartitions(DbDataReader reader, Table existing)
        {
            await reader.NextResultAsync();

            while (await reader.ReadAsync())
            {
                var strategy = await reader.GetFieldValueAsync<string>(1);
                var columnOrExpression = await reader.GetFieldValueAsync<string>(0);

                existing.PartitionExpressions.Add(columnOrExpression);

                switch (strategy)
                {
                    case "range":
                        existing.PartitionStrategy = PartitionStrategy.Range;
                        break;
                }
            }
        }

        private static async Task readColumns(DbDataReader reader, Table existing)
        {
            while (await reader.ReadAsync())
            {
                var column = await readColumn(reader);

                existing._columns.Add(column);
            }
        }

        private static async Task<TableColumn> readColumn(DbDataReader reader)
        {
            var column = new TableColumn(await reader.GetFieldValueAsync<string>(0),
                await reader.GetFieldValueAsync<string>(1));

            if (column.Type.Equals("user-defined"))
            {
                column.Type = await reader.GetFieldValueAsync<string>(3);
            }

            if (!await reader.IsDBNullAsync(2))
            {
                var length = await reader.GetFieldValueAsync<int>(2);
                column.Type = $"{column.Type}({length})";
            }

            return column;
        }

        private async Task readConstraints(DbDataReader reader, Table existing)
        {
            await reader.NextResultAsync();
            while (await reader.ReadAsync())
            {
                var name = await reader.GetFieldValueAsync<string>(0);
                var definition = await reader.GetFieldValueAsync<string>(5);

                var fk = new ForeignKey(name);
                fk.Parse(definition);
                
                existing.ForeignKeys.Add(fk);
            }
        }

        private async Task readIndexes(DbDataReader reader, Table existing)
        {
            await reader.NextResultAsync();
            while (await reader.ReadAsync())
            {
                if (await reader.IsDBNullAsync(2))
                    continue;
                
                var isPrimary = await reader.GetFieldValueAsync<bool>(6);
                if (isPrimary) continue;

                var schemaName = await reader.GetFieldValueAsync<string>(1);
                var tableName = await reader.GetFieldValueAsync<string>(2);
                var ddl = await reader.GetFieldValueAsync<string>(4);
                

                if ((Identifier.Schema == schemaName && Identifier.Name == tableName) || Identifier.QualifiedName == tableName)
                {
                    var index = IndexDefinition.Parse(ddl);

                    existing.Indexes.Add(index);
                }
            }
        }
        
        private static async Task<(string? PrimaryKeyName, List<string> Columns)> readPrimaryKeys(DbDataReader reader)
        {
            var pks = new List<string>();
            string? primaryKeyName = null;
            await reader.NextResultAsync();
            while (await reader.ReadAsync())
            {
                var columnName = await reader.GetFieldValueAsync<string>(0);
                primaryKeyName ??= await reader.GetFieldValueAsync<string>(2);
                pks.Add(columnName);
            }
            return (primaryKeyName, pks);
        }

        
    }
    
    
}