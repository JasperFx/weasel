using System.Data.Common;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.SqlServer.Tables;

public partial class Table
{
    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

        builder.Append($@"
-- Columns come from sys.columns rather than information_schema.columns because the latter exposes
-- no is_identity, and reports length only in characters -- it cannot express decimal(18,2) or
-- datetime2(3) at all, so both read back as a bare type name.
select
    c.name,
    tp.name as type_name,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity,
    dc.definition as default_definition
from sys.columns c
    inner join sys.tables t on t.object_id = c.object_id
    inner join sys.schemas s on s.schema_id = t.schema_id
    inner join sys.types tp on tp.user_type_id = c.user_type_id
    left join sys.default_constraints dc on dc.parent_object_id = c.object_id and dc.parent_column_id = c.column_id
where s.name = @{schemaParam} and t.name = @{nameParam}
order by c.column_id;

-- CONSTRAINT_COLUMN_USAGE cannot express key order, so the primary key comes from sys.index_columns
-- via the backing index: key_ordinal is the declared order, which for a composite key is not
-- necessarily the table's column order.
select
    c.name as COLUMN_NAME,
    kc.name as CONSTRAINT_NAME
from sys.key_constraints kc
    inner join sys.tables t on t.object_id = kc.parent_object_id
    inner join sys.schemas s on s.schema_id = t.schema_id
    inner join sys.indexes i on i.object_id = kc.parent_object_id and i.index_id = kc.unique_index_id
    inner join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id
    inner join sys.columns c on c.object_id = ic.object_id and c.column_id = ic.column_id
where s.name = @{schemaParam} and t.name = @{nameParam} and kc.type = 'PK'
order by ic.key_ordinal;

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
   order by parent.name, fk.constraint_column_id;




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
    ic.is_descending_key,
    ic.is_included_column

from
    sys.index_columns ic
    inner join sys.tables t on t.object_id = ic.object_id
    inner join sys.schemas s on s.schema_id = t.schema_id
    inner join sys.columns c on c.object_id = ic.object_id and c.column_id = ic.column_id
where
        t.name = @{nameParam} and
        s.name = @{schemaParam} and
        -- An index aligned with the table's partition scheme carries an implicit row for the
        -- partitioning column: key_ordinal 0, is_included_column 0, partition_ordinal >= 1. It is
        -- not a column the index declares, and reading it as one makes every aligned index compare
        -- unequal to itself. Discriminating on partition_ordinal instead would be wrong the other
        -- way: an aligned UNIQUE index is required to carry the partitioning column IN its key,
        -- where it has both a key_ordinal and a partition_ordinal.
        (ic.key_ordinal >= 1 or ic.is_included_column = 1)
-- key_ordinal is the index's key order; index_column_id is just the column's position in the
-- table. Ordering by the latter silently reorders a composite index -- (ProductId, Id) came back
-- as (Id, ProductId), which is a different index. Included columns carry key_ordinal 0, so they
-- are separated out first.
order by
    ic.index_id,
    ic.is_included_column,
    ic.key_ordinal,
    ic.index_column_id;


select
    pf.name as partition_function,
    ps.name as partition_scheme,
    pc.name as partition_column,
    pt.name as partition_type,
    pf.boundary_value_on_right,
    prv.value as boundary_value
from sys.tables tbl
    inner join sys.schemas sch on tbl.schema_id = sch.schema_id
    inner join sys.indexes idx on tbl.object_id = idx.object_id and idx.index_id in (0, 1)
    inner join sys.partition_schemes ps on idx.data_space_id = ps.data_space_id
    inner join sys.partition_functions pf on ps.function_id = pf.function_id
    inner join sys.index_columns icp on icp.object_id = tbl.object_id and icp.index_id = idx.index_id and icp.partition_ordinal >= 1
    inner join sys.columns pc on pc.object_id = tbl.object_id and pc.column_id = icp.column_id
    inner join sys.partition_parameters pp on pp.function_id = pf.function_id
    inner join sys.types pt on pt.user_type_id = pp.user_type_id
    left join sys.partition_range_values prv on prv.function_id = pf.function_id
where sch.name = @{schemaParam} and tbl.name = @{nameParam}
order by prv.boundary_id;

select cc.name, cc.definition
from sys.check_constraints cc
    inner join sys.tables t on t.object_id = cc.parent_object_id
    inner join sys.schemas s on s.schema_id = t.schema_id
where s.name = @{schemaParam} and t.name = @{nameParam};

select comp.name, comp.definition, comp.is_persisted
from sys.computed_columns comp
    inner join sys.tables t on t.object_id = comp.object_id
    inner join sys.schemas s on s.schema_id = t.schema_id
where s.name = @{schemaParam} and t.name = @{nameParam};

");
    }

    public async Task<Table?> FetchExistingAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);

        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var result = await readExistingAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return result;
    }

    private async Task<Table?> readExistingAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = new Table(Identifier);

        await readColumnsAsync(reader, existing, ct).ConfigureAwait(false);

        var (pks, primaryKeyName) = await readPrimaryKeysAsync(reader, ct).ConfigureAwait(false);
        foreach (var pkColumn in pks) existing.ColumnFor(pkColumn)!.IsPrimaryKey = true;
        existing.PrimaryKeyName = primaryKeyName;
        // Declared key order, which for a composite key need not match the table's column order.
        existing.SetPrimaryKeyOrder(pks);


        await readForeignKeysAsync(reader, existing, ct).ConfigureAwait(false);

        await readIndexesAsync(reader, existing, ct).ConfigureAwait(false);

        await readPartitioningAsync(reader, existing, ct).ConfigureAwait(false);

        await readCheckConstraintsAsync(reader, existing, ct).ConfigureAwait(false);

        await readComputedColumnsAsync(reader, existing, ct).ConfigureAwait(false);

        return !existing.Columns.Any()
            ? null
            : existing;
    }

    private static async Task readComputedColumnsAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        await reader.NextResultAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var definition = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
            var isPersisted = await reader.GetFieldValueAsync<bool>(2, ct).ConfigureAwait(false);

            // sys.computed_columns renders "([first_name]+' '+[last_name])"; stored
            // raw — canonicalization happens at comparison time
            if (existing.ColumnFor(name) is { } column)
            {
                column.ComputedExpression = definition;
                column.ComputedColumnIsStored = isPersisted;
            }
        }
    }

    private static async Task readCheckConstraintsAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        await reader.NextResultAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var definition = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);

            // sys.check_constraints.definition renders "([Price]>(0))"; stored
            // raw — canonicalization happens at comparison time
            existing.CheckConstraints.Add(new TableCheckConstraint(name, definition));
        }
    }

    private async Task readPartitioningAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        var hasResults = await reader.NextResultAsync(ct).ConfigureAwait(false);
        if (!hasResults)
        {
            return;
        }

        Partitioning.SqlServerPartitionInfo? info = null;

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            info ??= new Partitioning.SqlServerPartitionInfo();

            info.PartitionFunctionName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            info.PartitionSchemeName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
            info.Column = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);
            info.SqlDataType = await reader.GetFieldValueAsync<string>(3, ct).ConfigureAwait(false);
            info.IsRangeRight = await reader.GetFieldValueAsync<bool>(4, ct).ConfigureAwait(false);

            // sys.partition_range_values.value is a sql_variant carrying the boundary as its native CLR
            // type (DateTime, DateTimeOffset, int, bool, ...). Format it through the SAME canonical
            // formatter used to declare boundaries so a round-trip compares equal with no spurious rebuild.
            if (!await reader.IsDBNullAsync(5, ct).ConfigureAwait(false))
            {
                var boundary = reader.GetValue(5);
                info.BoundaryValues.Add(Partitioning.RangePartitioning.FormatSqlValue(boundary));
            }
        }

        existing.PartitionInfo = info;
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
        var name = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        var typeName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
        var maxLength = await reader.GetFieldValueAsync<short>(2, ct).ConfigureAwait(false);
        var precision = await reader.GetFieldValueAsync<byte>(3, ct).ConfigureAwait(false);
        var scale = await reader.GetFieldValueAsync<byte>(4, ct).ConfigureAwait(false);

        var column = new TableColumn(name, FormatStoreType(typeName, maxLength, precision, scale))
        {
            AllowNulls = await reader.GetFieldValueAsync<bool>(5, ct).ConfigureAwait(false),
            IsAutoNumber = await reader.GetFieldValueAsync<bool>(6, ct).ConfigureAwait(false)
        };

        if (!await reader.IsDBNullAsync(7, ct).ConfigureAwait(false))
        {
            column.DefaultExpression = await reader.GetFieldValueAsync<string>(7, ct).ConfigureAwait(false);
        }

        return column;
    }

    internal static string FormatStoreType(string typeName, short maxLength, byte precision, byte scale)
    {
        switch (typeName.ToLowerInvariant())
        {
            case "nchar":
            case "nvarchar":
                // Unicode lengths are stored doubled; -1 is the MAX sentinel and must not be halved.
                return maxLength == -1 ? $"{typeName}(max)" : $"{typeName}({maxLength / 2})";

            case "char":
            case "varchar":
            case "binary":
            case "varbinary":
                return maxLength == -1 ? $"{typeName}(max)" : $"{typeName}({maxLength})";

            case "decimal":
            case "numeric":
                return $"{typeName}({precision},{scale})";

            case "time":
            case "datetime2":
            case "datetimeoffset":
                return $"{typeName}({scale})";

            default:
                return typeName;
        }
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

                // Non-key (INCLUDE) columns are part of the covering-index leaf
                // pages, not the key — reading them as key columns produced
                // spurious drop/recreate migrations for covering indexes
                var isIncluded = await reader.GetFieldValueAsync<bool>(3, ct).ConfigureAwait(false);
                if (isIncluded)
                {
                    index.AddIncludedColumn(name);
                    continue;
                }

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
