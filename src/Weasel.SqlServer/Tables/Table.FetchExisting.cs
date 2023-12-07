using System.Data.Common;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.SqlServer.Tables;

public partial class Table
{
    public void ConfigureQueryCommand(DbCommandBuilder builder)
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

    public async Task<Table?> FetchExistingAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);

        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        return await readExistingAsync(reader, ct).ConfigureAwait(false);
    }

    private async Task<Table?> readExistingAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = new Table(Identifier);

        await readColumnsAsync(reader, existing, ct).ConfigureAwait(false);

        var (pks, primaryKeyName) = await readPrimaryKeysAsync(reader, ct).ConfigureAwait(false);
        foreach (var pkColumn in pks) existing.ColumnFor(pkColumn)!.IsPrimaryKey = true;
        existing.PrimaryKeyName = primaryKeyName;


        await readForeignKeysAsync(reader, existing, ct).ConfigureAwait(false);

        await readIndexesAsync(reader, existing, ct).ConfigureAwait(false);

        return !existing.Columns.Any()
            ? null
            : existing;
    }

    private async Task readForeignKeysAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        await reader.NextResultAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var fkName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var tableName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
            var schemaName = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);
            var columnName = await reader.GetFieldValueAsync<string>(3, ct).ConfigureAwait(false);
            var referencedName = await reader.GetFieldValueAsync<string>(4, ct).ConfigureAwait(false);

            var onDelete = await reader.GetFieldValueAsync<string>(5, ct).ConfigureAwait(false);
            var onUpdate = await reader.GetFieldValueAsync<string>(6, ct).ConfigureAwait(false);

            var fk = existing.FindOrCreateForeignKey(fkName);
            fk.LinkedTable = new SqlServerObjectName(schemaName, tableName);
            fk.ReadReferentialActions(onDelete, onUpdate);

            fk.LinkColumns(columnName, referencedName);
        }
    }


    private static async Task readColumnsAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var column = await readColumnAsync(reader, ct).ConfigureAwait(false);

            existing._columns.Add(column);
        }
    }

    private static async Task<TableColumn> readColumnAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var column = new TableColumn(
            await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false),
            await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false)
        );

        if (column.Type.Equals("user-defined"))
        {
            column.Type = await reader.GetFieldValueAsync<string>(3, ct).ConfigureAwait(false);
        }

        if (!await reader.IsDBNullAsync(2, ct).ConfigureAwait(false))
        {
            var length = await reader.GetFieldValueAsync<int>(2, ct).ConfigureAwait(false);
            column.Type = $"{column.Type}({length})";
        }

        return column;
    }


    private async Task readIndexesAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        var hasResults = await reader.NextResultAsync(ct).ConfigureAwait(false);
        var indexes = new Dictionary<int, IndexDefinition>();

        while (hasResults && await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var id = await reader.GetFieldValueAsync<int>(0, ct).ConfigureAwait(false);

            // This is an odd Sql Server centric quirk I think, this is really detecting
            // no primary keys
            if (await reader.IsDBNullAsync(1, ct).ConfigureAwait(false))
            {
                continue;
            }

            var name = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
            var typeDesc = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);


            var index = new IndexDefinition(name)
            {
                IsClustered = typeDesc == "CLUSTERED",
                IsUnique = await reader.GetFieldValueAsync<bool>(3, ct).ConfigureAwait(false)
            };

            if (!await reader.IsDBNullAsync(4, ct).ConfigureAwait(false))
            {
                index.FillFactor = await reader.GetFieldValueAsync<byte>(4, ct).ConfigureAwait(false);
            }

            if (!await reader.IsDBNullAsync(6, ct).ConfigureAwait(false) &&
                await reader.GetFieldValueAsync<bool>(5, ct).ConfigureAwait(false))
            {
                index.Predicate = await reader.GetFieldValueAsync<string>(6, ct).ConfigureAwait(false);
            }

            indexes.Add(id, index);

            existing.Indexes.Add(index);
        }

        await reader.NextResultAsync(ct).ConfigureAwait(false);

        while (hasResults && await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var id = await reader.GetFieldValueAsync<int>(0, ct).ConfigureAwait(false);
            if (indexes.TryGetValue(id, out var index))
            {
                var name = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
                index.AddColumn(name);

                var isDesc = await reader.GetFieldValueAsync<bool>(2, ct).ConfigureAwait(false);

                if (isDesc)
                {
                    index.SortOrder = SortOrder.Desc;
                }
            }
        }
    }

    private static async Task<(List<string>, string)> readPrimaryKeysAsync(
        DbDataReader reader,
        CancellationToken ct = default
    )
    {
        string? pkName = null;
        var pks = new List<string>();
        await reader.NextResultAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            pks.Add(await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false));
            pkName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
        }

        return (pks, pkName!);
    }
}
