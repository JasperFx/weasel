using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle.Tables;

public partial class Table
{
    // Oracle doesn't support multiple result sets in a single command like PostgreSQL or SQL Server.
    // ConfigureQueryCommand includes columns and primary key info in a single query via LEFT JOIN.
    // For full schema detection (including FKs and indexes), use FetchExistingAsync directly.
    public void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema.ToUpperInvariant()).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name.ToUpperInvariant()).ParameterName;

        builder.Append($@"
SELECT c.column_name, c.data_type, c.data_length, c.data_precision, c.data_scale, c.nullable,
       CASE WHEN pk_cols.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_pk,
       pk_cons.constraint_name AS pk_name
FROM all_tab_columns c
LEFT JOIN all_constraints pk_cons
    ON pk_cons.owner = c.owner AND pk_cons.table_name = c.table_name AND pk_cons.constraint_type = 'P'
LEFT JOIN all_cons_columns pk_cols
    ON pk_cols.owner = pk_cons.owner AND pk_cols.constraint_name = pk_cons.constraint_name AND pk_cols.column_name = c.column_name
WHERE c.owner = :{schemaParam} AND c.table_name = :{nameParam}
ORDER BY c.column_id
");
    }

    /// <summary>
    /// Reads table metadata from a DbDataReader including columns and primary key info.
    /// Oracle limitation: FKs and indexes are not included since Oracle doesn't support
    /// multiple result sets. For full schema detection, use FetchExistingAsync instead.
    /// </summary>
    internal async Task<Table?> ReadExistingFromReaderAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = new Table(Identifier);
        string? pkName = null;

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var column = await readColumnAsync(reader, ct).ConfigureAwait(false);

            // Read PK info from the additional columns in the query
            if (reader.FieldCount > 6)
            {
                var isPk = !await reader.IsDBNullAsync(6, ct).ConfigureAwait(false)
                    && Convert.ToInt32(await reader.GetFieldValueAsync<decimal>(6, ct).ConfigureAwait(false)) == 1;
                if (isPk)
                {
                    column.IsPrimaryKey = true;
                }

                if (!await reader.IsDBNullAsync(7, ct).ConfigureAwait(false))
                {
                    pkName = await reader.GetFieldValueAsync<string>(7, ct).ConfigureAwait(false);
                }
            }

            existing._columns.Add(column);
        }

        if (!existing.Columns.Any()) return null;

        if (pkName != null)
        {
            existing.PrimaryKeyName = pkName;
        }

        return existing;
    }

    public async Task<Table?> FetchExistingAsync(OracleConnection conn, CancellationToken ct = default)
    {
        var schemaName = Identifier.Schema.ToUpperInvariant();
        var tableName = Identifier.Name.ToUpperInvariant();

        var existing = new Table(Identifier);

        // Query 1: Columns
        await readColumnsAsync(conn, existing, schemaName, tableName, ct).ConfigureAwait(false);

        if (!existing.Columns.Any())
        {
            return null;
        }

        // Query 2: Primary Keys
        var (pks, primaryKeyName) = await readPrimaryKeysAsync(conn, schemaName, tableName, ct).ConfigureAwait(false);
        foreach (var pkColumn in pks) existing.ColumnFor(pkColumn)!.IsPrimaryKey = true;
        if (primaryKeyName != null)
        {
            existing.PrimaryKeyName = primaryKeyName;
        }

        // Query 3: Foreign Keys
        await readForeignKeysAsync(conn, existing, schemaName, tableName, ct).ConfigureAwait(false);

        // Query 4 & 5: Indexes
        await readIndexesAsync(conn, existing, schemaName, tableName, ct).ConfigureAwait(false);

        return existing;
    }

    private static async Task readColumnsAsync(OracleConnection conn, Table existing, string schemaName, string tableName, CancellationToken ct = default)
    {
        var sql = @"
SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
FROM all_tab_columns
WHERE owner = :schemaName AND table_name = :tableName
ORDER BY column_id";

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.Add(new OracleParameter("schemaName", schemaName));
        cmd.Parameters.Add(new OracleParameter("tableName", tableName));

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
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
        var upperType = dataType.ToUpperInvariant();

        switch (upperType)
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
                // Handle TIMESTAMP variants - Oracle reports them with precision like "TIMESTAMP(6) WITH TIME ZONE"
                // but we store them without precision. Normalize by stripping the precision.
                if (upperType.StartsWith("TIMESTAMP"))
                {
                    // Strip precision like (6) from "TIMESTAMP(6) WITH TIME ZONE" -> "TIMESTAMP WITH TIME ZONE"
                    var normalized = System.Text.RegularExpressions.Regex.Replace(upperType, @"\(\d+\)", "");
                    return normalized.Replace("  ", " "); // Clean up any double spaces
                }
                return dataType;
        }
    }

    private static async Task<(List<string>, string?)> readPrimaryKeysAsync(
        OracleConnection conn,
        string schemaName,
        string tableName,
        CancellationToken ct = default
    )
    {
        var sql = @"
SELECT cols.column_name, cons.constraint_name
FROM all_constraints cons
JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
WHERE cons.owner = :schemaName
  AND cons.table_name = :tableName
  AND cons.constraint_type = 'P'
ORDER BY cols.position";

        string? pkName = null;
        var pks = new List<string>();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.Add(new OracleParameter("schemaName", schemaName));
        cmd.Parameters.Add(new OracleParameter("tableName", tableName));

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            pks.Add(await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false));
            pkName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
        }

        return (pks, pkName);
    }

    private async Task readForeignKeysAsync(OracleConnection conn, Table existing, string schemaName, string tableName, CancellationToken ct = default)
    {
        var sql = @"
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
WHERE cons.owner = :schemaName
  AND cons.table_name = :tableName
  AND cons.constraint_type = 'R'
ORDER BY cons.constraint_name, cols.position";

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.Add(new OracleParameter("schemaName", schemaName));
        cmd.Parameters.Add(new OracleParameter("tableName", tableName));

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var fkName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var refTableName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
            var refSchemaName = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);
            var columnName = await reader.GetFieldValueAsync<string>(3, ct).ConfigureAwait(false);
            var referencedName = await reader.GetFieldValueAsync<string>(4, ct).ConfigureAwait(false);

            var onDelete = await reader.IsDBNullAsync(5, ct).ConfigureAwait(false)
                ? null
                : await reader.GetFieldValueAsync<string>(5, ct).ConfigureAwait(false);

            var fk = existing.FindOrCreateForeignKey(fkName);
            fk.LinkedTable = new OracleObjectName(refSchemaName, refTableName);
            fk.ReadReferentialActions(onDelete);

            fk.LinkColumns(columnName, referencedName);
        }
    }

    private async Task readIndexesAsync(OracleConnection conn, Table existing, string schemaName, string tableName, CancellationToken ct = default)
    {
        // Query 4: Index metadata
        var indexSql = @"
SELECT
    i.index_name,
    i.uniqueness,
    i.index_type
FROM all_indexes i
WHERE i.owner = :schemaName
  AND i.table_name = :tableName
  AND NOT EXISTS (
      SELECT 1 FROM all_constraints c
      WHERE c.owner = i.owner AND c.index_name = i.index_name AND c.constraint_type = 'P'
  )";

        var indexes = new Dictionary<string, IndexDefinition>(StringComparer.OrdinalIgnoreCase);

        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = indexSql;
            cmd.Parameters.Add(new OracleParameter("schemaName", schemaName));
            cmd.Parameters.Add(new OracleParameter("tableName", tableName));

            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
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
        }

        // Query 5: Index columns
        var columnSql = @"
SELECT
    ic.index_name,
    ic.column_name,
    ic.descend
FROM all_ind_columns ic
JOIN all_indexes i ON ic.index_owner = i.owner AND ic.index_name = i.index_name
WHERE ic.index_owner = :indexOwner
  AND ic.table_owner = :tableOwner
  AND ic.table_name = :tableName
  AND NOT EXISTS (
      SELECT 1 FROM all_constraints c
      WHERE c.owner = i.owner AND c.index_name = i.index_name AND c.constraint_type = 'P'
  )
ORDER BY ic.index_name, ic.column_position";

        // For function-based indexes, we need to map system-generated column names back to actual columns.
        // Query all_ind_expressions separately because column_expression is a LONG type.
        var expressionMap = new Dictionary<(string indexName, int position), string>();
        var expressionSql = @"
SELECT
    index_name,
    column_position,
    column_expression
FROM all_ind_expressions
WHERE index_owner = :indexOwner
  AND table_owner = :tableOwner
  AND table_name = :tableName";

        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = expressionSql;
            cmd.Parameters.Add(new OracleParameter("indexOwner", schemaName));
            cmd.Parameters.Add(new OracleParameter("tableOwner", schemaName));
            cmd.Parameters.Add(new OracleParameter("tableName", tableName));

            // Set InitialLONGFetchSize to read LONG data types (column_expression is LONG)
            cmd.InitialLONGFetchSize = 4000;

            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var idxName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
                var position = Convert.ToInt32(await reader.GetFieldValueAsync<decimal>(1, ct).ConfigureAwait(false));
                if (!await reader.IsDBNullAsync(2, ct).ConfigureAwait(false))
                {
                    var expression = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);
                    expressionMap[(idxName.ToUpperInvariant(), position)] = expression;
                }
            }
        }

        var columnPositions = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = columnSql;
            cmd.Parameters.Add(new OracleParameter("indexOwner", schemaName));
            cmd.Parameters.Add(new OracleParameter("tableOwner", schemaName));
            cmd.Parameters.Add(new OracleParameter("tableName", tableName));

            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var indexName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
                if (indexes.TryGetValue(indexName, out var index))
                {
                    // Track column position for this index
                    if (!columnPositions.ContainsKey(indexName))
                    {
                        columnPositions[indexName] = 1;
                    }
                    var position = columnPositions[indexName]++;

                    var columnName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
                    var descend = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);

                    // For function-based indexes (DESC), Oracle stores system-generated column names
                    // like SYS_NC00006$. Extract the actual column name from the expression.
                    if (columnName.StartsWith("SYS_", StringComparison.OrdinalIgnoreCase) &&
                        expressionMap.TryGetValue((indexName.ToUpperInvariant(), position), out var expression))
                    {
                        // Expression is like SYS_OP_DESCEND("USER_NAME") or just "USER_NAME"
                        // Extract the column name from within quotes
                        var match = System.Text.RegularExpressions.Regex.Match(expression, "\"([^\"]+)\"");
                        if (match.Success)
                        {
                            columnName = match.Groups[1].Value;
                        }
                    }

                    index.AddColumn(columnName);

                    if (descend == "DESC")
                    {
                        index.SortOrder = SortOrder.Desc;
                    }
                }
            }
        }

        // Remove any indexes that have no columns (possibly system-generated or function-based indexes
        // that we can't properly detect)
        var emptyIndexes = existing.Indexes.Where(i => !i.Columns.Any()).ToList();
        foreach (var emptyIndex in emptyIndexes)
        {
            existing.Indexes.Remove(emptyIndex);
        }
    }
}
