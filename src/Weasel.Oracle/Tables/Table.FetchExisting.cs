using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle.Tables;

public partial class Table
{
    public void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema.ToUpperInvariant()).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name.ToUpperInvariant()).ParameterName;

        builder.Append($@"
SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
FROM all_tab_columns
WHERE owner = :{schemaParam} AND table_name = :{nameParam}
ORDER BY column_id
");

        builder.Append($@"
SELECT cols.column_name, cons.constraint_name
FROM all_constraints cons
JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
WHERE cons.owner = :{schemaParam}
  AND cons.table_name = :{nameParam}
  AND cons.constraint_type = 'P'
ORDER BY cols.position
");

        builder.Append($@"
SELECT
    cons.constraint_name,
    ref_cons.table_name AS referenced_table,
    ref_cons.owner AS referenced_schema,
    cols.column_name,
    ref_cols.column_name AS referenced_column,
    cons.delete_rule
FROM all_constraints cons
JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
JOIN all_constraints ref_cons ON cons.r_constraint_name = ref_cons.constraint_name AND cons.r_owner = ref_cons.owner
JOIN all_cons_columns ref_cols ON ref_cons.constraint_name = ref_cols.constraint_name
    AND ref_cons.owner = ref_cols.owner AND cols.position = ref_cols.position
WHERE cons.owner = :{schemaParam}
  AND cons.table_name = :{nameParam}
  AND cons.constraint_type = 'R'
ORDER BY cons.constraint_name, cols.position
");

        builder.Append($@"
SELECT
    i.index_name,
    i.uniqueness,
    i.index_type
FROM all_indexes i
WHERE i.owner = :{schemaParam}
  AND i.table_name = :{nameParam}
  AND NOT EXISTS (
      SELECT 1 FROM all_constraints c
      WHERE c.owner = i.owner AND c.index_name = i.index_name AND c.constraint_type = 'P'
  )
");

        builder.Append($@"
SELECT
    ic.index_name,
    ic.column_name,
    ic.descend
FROM all_ind_columns ic
JOIN all_indexes i ON ic.index_owner = i.owner AND ic.index_name = i.index_name
WHERE ic.index_owner = :{schemaParam}
  AND ic.table_name = :{nameParam}
  AND NOT EXISTS (
      SELECT 1 FROM all_constraints c
      WHERE c.owner = i.owner AND c.index_name = i.index_name AND c.constraint_type = 'P'
  )
ORDER BY ic.index_name, ic.column_position
");
    }

    public async Task<Table?> FetchExistingAsync(OracleConnection conn, CancellationToken ct = default)
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
        if (primaryKeyName != null)
        {
            existing.PrimaryKeyName = primaryKeyName;
        }

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

            var onDelete = await reader.IsDBNullAsync(5, ct).ConfigureAwait(false)
                ? null
                : await reader.GetFieldValueAsync<string>(5, ct).ConfigureAwait(false);

            var fk = existing.FindOrCreateForeignKey(fkName);
            fk.LinkedTable = new OracleObjectName(schemaName, tableName);
            fk.ReadReferentialActions(onDelete);

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
        var columnName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        var dataType = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
        var dataLength = await reader.IsDBNullAsync(2, ct).ConfigureAwait(false)
            ? (int?)null
            : Convert.ToInt32(await reader.GetFieldValueAsync<decimal>(2, ct).ConfigureAwait(false));
        var dataPrecision = await reader.IsDBNullAsync(3, ct).ConfigureAwait(false)
            ? (int?)null
            : Convert.ToInt32(await reader.GetFieldValueAsync<decimal>(3, ct).ConfigureAwait(false));
        var dataScale = await reader.IsDBNullAsync(4, ct).ConfigureAwait(false)
            ? (int?)null
            : Convert.ToInt32(await reader.GetFieldValueAsync<decimal>(4, ct).ConfigureAwait(false));
        var nullable = await reader.GetFieldValueAsync<string>(5, ct).ConfigureAwait(false);

        var type = BuildOracleType(dataType, dataLength, dataPrecision, dataScale);
        var column = new TableColumn(columnName, type)
        {
            AllowNulls = nullable == "Y"
        };

        return column;
    }

    private static string BuildOracleType(string dataType, int? dataLength, int? dataPrecision, int? dataScale)
    {
        switch (dataType.ToUpperInvariant())
        {
            case "VARCHAR2":
            case "NVARCHAR2":
            case "CHAR":
            case "NCHAR":
            case "RAW":
                return dataLength.HasValue ? $"{dataType}({dataLength})" : dataType;

            case "NUMBER":
                if (dataPrecision.HasValue && dataScale.HasValue && dataScale.Value > 0)
                {
                    return $"NUMBER({dataPrecision},{dataScale})";
                }
                if (dataPrecision.HasValue)
                {
                    return $"NUMBER({dataPrecision})";
                }
                return "NUMBER";

            case "FLOAT":
                return dataPrecision.HasValue ? $"FLOAT({dataPrecision})" : "FLOAT";

            default:
                return dataType;
        }
    }

    private async Task readIndexesAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        var hasResults = await reader.NextResultAsync(ct).ConfigureAwait(false);
        var indexes = new Dictionary<string, IndexDefinition>(StringComparer.OrdinalIgnoreCase);

        while (hasResults && await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var uniqueness = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
            var indexType = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);

            var index = new IndexDefinition(name)
            {
                IsUnique = uniqueness == "UNIQUE",
                IndexType = indexType == "BITMAP" ? OracleIndexType.Bitmap :
                           indexType.Contains("FUNCTION") ? OracleIndexType.FunctionBased :
                           OracleIndexType.BTree
            };

            indexes.Add(name, index);
            existing.Indexes.Add(index);
        }

        await reader.NextResultAsync(ct).ConfigureAwait(false);

        while (hasResults && await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var indexName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            if (indexes.TryGetValue(indexName, out var index))
            {
                var columnName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
                index.AddColumn(columnName);

                var descend = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);

                if (descend == "DESC")
                {
                    index.SortOrder = SortOrder.Desc;
                }
            }
        }
    }

    private static async Task<(List<string>, string?)> readPrimaryKeysAsync(
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

        return (pks, pkName);
    }
}
