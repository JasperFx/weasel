using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Weasel.Core;

namespace Weasel.SqlServer.Tables
{
    public partial class Table
    {
        public void ConfigureQueryCommand(CommandBuilder builder)
        {
            var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
            var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

            builder.Append($@"
select column_name, data_type, character_maximum_length
from information_schema.columns where table_schema = @{schemaParam} and table_name = @{nameParam}
order by ordinal_position;

select 
    COLUMN_NAME, 
    CONSTRAINT_NAME 
from 
    INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE 
where 
    TABLE_SCHEMA = @{schemaParam} and 
    TABLE_NAME = @{nameParam} and 
    CONSTRAINT_NAME in (select constraint_name from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where TABLE_CONSTRAINTS.TABLE_NAME = @{nameParam} and TABLE_CONSTRAINTS.TABLE_SCHEMA = @{schemaParam} and CONSTRAINT_TYPE = 'PRIMARY KEY')

   select 
          parent.name as constraint_name,
          fkt.name as referenced_table,
          fks.name as referenced_schema,
          c.name,
          cfk.name as referenced_name,
          parent.delete_referential_action_desc,
          parent.update_referential_action_desc

   from sys.foreign_key_columns fk 
       inner join sys.foreign_keys parent on fk.constraint_object_id = parent.object_id
       inner join sys.tables t on fk.parent_object_id = t.object_id
       inner join sys.schemas s on t.schema_id = s.schema_id
       inner join sys.tables fkt on fk.referenced_object_id = fkt.object_id
       inner join sys.schemas fks on fkt.schema_id = fks.schema_id
       inner join sys.columns c on fk.parent_object_id = c.object_id and fk.parent_column_id = c.column_id
       inner join sys.columns cfk on fk.referenced_object_id = cfk.object_id and fk.referenced_column_id = cfk.column_id
   where
        s.name = @{schemaParam} and
        t.name = @{nameParam};




select 
    i.index_id,
    i.name, 
    i.type_desc as type,
    i.is_unique,
    i.fill_factor,
    i.has_filter,
    i.filter_definition
from 
    sys.indexes i
    inner join sys.tables t on t.object_id = i.object_id
    inner join sys.schemas s on s.schema_id = t.schema_id
where
    t.name = @{nameParam} and
    s.name = @{schemaParam} and
    i.is_primary_key = 0;


select 
    ic.index_id,
    c.name,
    ic.is_descending_key
       
from
    sys.index_columns ic 
    inner join sys.tables t on t.object_id = ic.object_id
    inner join sys.schemas s on s.schema_id = t.schema_id
    inner join sys.columns c on c.object_id = ic.object_id and c.column_id = ic.column_id
where
        t.name = @{nameParam} and
        s.name = @{schemaParam}
order by 
    ic.index_id,
    ic.index_column_id;



");


        }

        public async Task<Table> FetchExisting(SqlConnection conn)
        {
            var builder = new CommandBuilder();

            ConfigureQueryCommand(builder);

            using var reader = await builder.ExecuteReaderAsync(conn);
            return await readExisting(reader);
        }

        private async Task<Table> readExisting(DbDataReader reader)
        {
            var existing = new Table(Identifier);

            await readColumns(reader, existing);

            var (pks, primaryKeyName) = await readPrimaryKeys(reader);
            foreach (var pkColumn in pks) existing.ColumnFor(pkColumn).IsPrimaryKey = true;
            existing.PrimaryKeyName = primaryKeyName;

            
            await readForeignKeys(reader, existing);
            
            await readIndexes(reader, existing);

            return !existing.Columns.Any()
                ? null
                : existing;
        }

        private async Task readForeignKeys(DbDataReader reader, Table existing)
        {
            await reader.NextResultAsync();
            while (await reader.ReadAsync())
            {
                var fkName = await reader.GetFieldValueAsync<string>(0);
                var tableName = await reader.GetFieldValueAsync<string>(1);
                var schemaName = await reader.GetFieldValueAsync<string>(2);
                var columnName = await reader.GetFieldValueAsync<string>(3);
                var referencedName = await reader.GetFieldValueAsync<string>(4);

                var onDelete = await reader.GetFieldValueAsync<string>(5);
                var onUpdate = await reader.GetFieldValueAsync<string>(6);

                var fk = existing.FindOrCreateForeignKey(fkName);
                fk.LinkedTable = new DbObjectName(schemaName, tableName);
                fk.ReadReferentialActions(onDelete, onUpdate);

                fk.LinkColumns(columnName, referencedName);
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


        private async Task readIndexes(DbDataReader reader, Table existing)
        {
            var hasResults = await reader.NextResultAsync();
            var indexes = new Dictionary<int, IndexDefinition>();
            
            while (hasResults && await reader.ReadAsync())
            {
                var id = await reader.GetFieldValueAsync<int>(0);
                
                // This is an odd Sql Server centric quirk I think, this is really detecting
                // no indexes
                if (await reader.IsDBNullAsync(1))
                {
                    hasResults = false;
                    break;
                }
                
                var name = await reader.GetFieldValueAsync<string>(1);
                var typeDesc = await reader.GetFieldValueAsync<string>(2);


                var index = new IndexDefinition(name)
                {
                    IsClustered = typeDesc == "CLUSTERED", 
                    IsUnique = await reader.GetFieldValueAsync<bool>(3)
                };

                if (!(await reader.IsDBNullAsync(4)))
                {
                    index.FillFactor =  await reader.GetFieldValueAsync<byte>(4);
                }

                if (!(await reader.IsDBNullAsync(6)) &&  await reader.GetFieldValueAsync<bool>(5))
                {
                    index.Predicate = await reader.GetFieldValueAsync<string>(6);
                }
                
                indexes.Add(id, index);
                
                existing.Indexes.Add(index);

            }

            await reader.NextResultAsync();

            while (hasResults && await reader.ReadAsync())
            {
                var id = await reader.GetFieldValueAsync<int>(0);
                if (indexes.TryGetValue(id, out var index))
                {
                    var name = await reader.GetFieldValueAsync<string>(1);
                    index.AddColumn(name);
                    
                    var isDesc = await reader.GetFieldValueAsync<bool>(2);

                    if (isDesc)
                    {
                        index.SortOrder = SortOrder.Desc;
                    }
                }
                

            }
        }

        private static async Task<(List<string>, string)> readPrimaryKeys(DbDataReader reader)
        {
            string pkName = null;
            var pks = new List<string>();
            await reader.NextResultAsync();
            while (await reader.ReadAsync())
            {
                pks.Add(await reader.GetFieldValueAsync<string>(0));
                pkName = await reader.GetFieldValueAsync<string>(1);
            }

            return (pks, pkName);
        }
    }
}