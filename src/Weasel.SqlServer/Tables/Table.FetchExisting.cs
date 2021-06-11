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
    cols.column_name
from 
    information_schema.KEY_COLUMN_USAGE cols inner join information_schema.TABLE_CONSTRAINTS cons on cols.table_schema = cons.table_schema and cols.table_name = cons.table_name
where
   cons.CONSTRAINT_TYPE = 'PRIMARY KEY' and
   cols.table_schema = @{schemaParam} and 
   cols.table_name = @{nameParam};

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
        t.name = @{nameParam}
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

            var pks = await readPrimaryKeys(reader);
            foreach (var pkColumn in pks) existing.ColumnFor(pkColumn).IsPrimaryKey = true;

            
            await readForeignKeys(reader, existing);
            
            // await readIndexes(reader, existing);
            // await readConstraints(reader, existing);
            //
            // 
            //
            // if (PartitionStrategy != PartitionStrategy.None)
            // {
            //     await readPartitions(reader, existing);
            // }

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


        private async Task readIndexes(DbDataReader reader, Table existing)
        {
            await reader.NextResultAsync();
            while (await reader.ReadAsync())
            {
                if (await reader.IsDBNullAsync(2))
                {
                    continue;
                }

                var isPrimary = await reader.GetFieldValueAsync<bool>(6);
                if (isPrimary)
                {
                    continue;
                }

                var schemaName = await reader.GetFieldValueAsync<string>(1);
                var tableName = await reader.GetFieldValueAsync<string>(2);
                var ddl = await reader.GetFieldValueAsync<string>(4);


                if (Identifier.Schema == schemaName && Identifier.Name == tableName ||
                    Identifier.QualifiedName == tableName)
                {
                    var index = IndexDefinition.Parse(ddl);

                    existing.Indexes.Add(index);
                }
            }
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