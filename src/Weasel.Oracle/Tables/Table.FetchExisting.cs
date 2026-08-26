using System.Data.Common;
using System.Text.RegularExpressions;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle.Tables;

public partial class Table
{
    private const string ColumnSql = @"
SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
FROM all_tab_columns
WHERE owner = :schemaName AND table_name = :tableName
ORDER BY column_id";

    private const string PrimaryKeySql = @"
SELECT cols.column_name, cons.constraint_name
FROM all_constraints cons
JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
WHERE cons.owner = :schemaName
  AND cons.table_name = :tableName
  AND cons.constraint_type = 'P'
ORDER BY cols.position";

    private const string ForeignKeySql = @"
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

    private const string IndexSql = @"
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

    private const string IndexExpressionSql = @"
SELECT
    index_name,
    column_position,
    column_expression
FROM all_ind_expressions
WHERE index_owner = :indexOwner
  AND table_owner = :tableOwner
  AND table_name = :tableName";

    private const string IndexColumnSql = @"
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

    /// <summary>
    ///     Register every introspection query this table needs — columns, primary key, foreign
    ///     keys, index metadata, index expressions, index columns — as five statements separated by
    ///     <see cref="CommandBuilderBase{TCommand,TParameter,TParameterType}.StartNewCommand" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Until weasel#474 this registered one query and spent it on columns, because ODP.NET
    ///         will not execute several statements from a single command. Indexes, foreign keys and
    ///         the primary key were therefore invisible to <c>SchemaMigration.DetermineAsync</c> —
    ///         which is the whole migration path — and only <see cref="FetchExistingAsync" /> saw
    ///         them. <c>OracleDbCommandBuilder</c> splits on the boundary and the reader chains
    ///         across the pieces, so the statements arrive as ordinary consecutive result sets.
    ///     </para>
    ///     <para>
    ///         <c>all_ind_expressions.column_expression</c> is a LONG, so the command needs
    ///         <c>InitialLONGFetchSize</c> set or it reads back empty — the same trap the Oracle
    ///         view slice hit in weasel#450.
    ///     </para>
    /// </remarks>
    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        if (builder.Command is OracleCommand oracleCommand)
        {
            oracleCommand.InitialLONGFetchSize = -1;
        }

        var schema = builder.AddParameter(Identifier.Schema.ToUpperInvariant()).ParameterName;
        var name = builder.AddParameter(Identifier.Name.ToUpperInvariant()).ParameterName;

        builder.Append(ColumnSql.Replace(":schemaName", $":{schema}").Replace(":tableName", $":{name}"));
        builder.StartNewCommand();

        builder.Append(PrimaryKeySql.Replace(":schemaName", $":{schema}").Replace(":tableName", $":{name}"));
        builder.StartNewCommand();

        builder.Append(ForeignKeySql.Replace(":schemaName", $":{schema}").Replace(":tableName", $":{name}"));
        builder.StartNewCommand();

        builder.Append(IndexSql.Replace(":schemaName", $":{schema}").Replace(":tableName", $":{name}"));
        builder.StartNewCommand();

        builder.Append(IndexExpressionSql.Replace(":indexOwner", $":{schema}").Replace(":tableOwner", $":{schema}")
            .Replace(":tableName", $":{name}"));
        builder.StartNewCommand();

        builder.Append(IndexColumnSql.Replace(":indexOwner", $":{schema}").Replace(":tableOwner", $":{schema}")
            .Replace(":tableName", $":{name}"));
    }

    /// <summary>
    ///     Read the six result sets <see cref="ConfigureQueryCommand" /> registers, in order.
    /// </summary>
    internal async Task<Table?> ReadExistingFromReaderAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = new Table(Identifier);

        await readColumnsFromReaderAsync(reader, existing, ct).ConfigureAwait(false);

        if (!existing.Columns.Any())
        {
            // The table does not exist. The remaining result sets are still there and still have to
            // be walked, or the next schema object in the batch reads this table's rows.
            for (var i = 0; i < 5; i++) await reader.NextResultAsync(ct).ConfigureAwait(false);
            return null;
        }

        await reader.NextResultAsync(ct).ConfigureAwait(false);
        var (pks, primaryKeyName) = await readPrimaryKeysFromReaderAsync(reader, ct).ConfigureAwait(false);
        foreach (var pkColumn in pks)
        {
            var column = existing.ColumnFor(pkColumn);
            if (column != null) column.IsPrimaryKey = true;
        }

        // The catalog query orders by key position, so this is the DECLARED key order, which for a
        // composite key need not match the order the columns appear in the table. Flagging alone
        // would discard it. Comparison stays order-insensitive unless the model pins an order.
        existing.SetPrimaryKeyOrder(pks);

        if (primaryKeyName != null)
        {
            existing.PrimaryKeyName = primaryKeyName;
        }

        await reader.NextResultAsync(ct).ConfigureAwait(false);
        await readForeignKeysFromReaderAsync(reader, existing, ct).ConfigureAwait(false);

        await reader.NextResultAsync(ct).ConfigureAwait(false);
        var indexes = await readIndexMetadataFromReaderAsync(reader, existing, ct).ConfigureAwait(false);

        await reader.NextResultAsync(ct).ConfigureAwait(false);
        var expressions = await readIndexExpressionsFromReaderAsync(reader, ct).ConfigureAwait(false);

        await reader.NextResultAsync(ct).ConfigureAwait(false);
        await readIndexColumnsFromReaderAsync(reader, existing, indexes, expressions, ct).ConfigureAwait(false);

        return existing;
    }

    /// <summary>
    ///     Read this table directly, one command per query. Kept alongside the batched path in
    ///     <see cref="ConfigureQueryCommand" /> because it is the convenient entry point for a
    ///     caller holding an <see cref="OracleConnection" /> and wanting one table; both share the
    ///     same SQL and the same readers, so they cannot drift apart.
    /// </summary>
    public async Task<Table?> FetchExistingAsync(OracleConnection conn, CancellationToken ct = default)
    {
        var schemaName = Identifier.Schema.ToUpperInvariant();
        var tableName = Identifier.Name.ToUpperInvariant();

        var existing = new Table(Identifier);

        await using (var reader = await queryAsync(conn, ColumnSql, schemaName, tableName, ct).ConfigureAwait(false))
        {
            await readColumnsFromReaderAsync(reader, existing, ct).ConfigureAwait(false);
        }

        if (!existing.Columns.Any())
        {
            return null;
        }

        await using (var reader =
                     await queryAsync(conn, PrimaryKeySql, schemaName, tableName, ct).ConfigureAwait(false))
        {
            var (pks, primaryKeyName) = await readPrimaryKeysFromReaderAsync(reader, ct).ConfigureAwait(false);
            applyPrimaryKey(existing, pks, primaryKeyName);
        }

        await using (var reader =
                     await queryAsync(conn, ForeignKeySql, schemaName, tableName, ct).ConfigureAwait(false))
        {
            await readForeignKeysFromReaderAsync(reader, existing, ct).ConfigureAwait(false);
        }

        Dictionary<string, IndexDefinition> indexes;
        await using (var reader = await queryAsync(conn, IndexSql, schemaName, tableName, ct).ConfigureAwait(false))
        {
            indexes = await readIndexMetadataFromReaderAsync(reader, existing, ct).ConfigureAwait(false);
        }

        Dictionary<(string, int), string> expressions;
        await using (var reader =
                     await queryAsync(conn, IndexExpressionSql, schemaName, tableName, ct).ConfigureAwait(false))
        {
            expressions = await readIndexExpressionsFromReaderAsync(reader, ct).ConfigureAwait(false);
        }

        await using (var reader =
                     await queryAsync(conn, IndexColumnSql, schemaName, tableName, ct).ConfigureAwait(false))
        {
            await readIndexColumnsFromReaderAsync(reader, existing, indexes, expressions, ct).ConfigureAwait(false);
        }

        return existing;
    }

    private static void applyPrimaryKey(Table existing, List<string> pks, string? primaryKeyName)
    {
        foreach (var pkColumn in pks)
        {
            var column = existing.ColumnFor(pkColumn);
            if (column != null)
            {
                column.IsPrimaryKey = true;
            }
        }

        // The catalog query orders by key position, so this is the DECLARED key order, which for a
        // composite key need not match the order the columns appear in the table. Flagging alone
        // would discard it. Comparison stays order-insensitive unless the model pins an order.
        existing.SetPrimaryKeyOrder(pks);

        if (primaryKeyName != null)
        {
            existing.PrimaryKeyName = primaryKeyName;
        }
    }

    /// <summary>
    ///     Run one of the introspection queries. Each binds the schema under one name or another
    ///     and most bind the table, so whichever the SQL mentions is what gets supplied.
    /// </summary>
    private static async Task<DbDataReader> queryAsync(
        OracleConnection conn, string sql, string schemaName, string tableName, CancellationToken ct)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.BindByName = true;

        // column_expression on all_ind_expressions is a LONG, and ODP.NET reads a LONG back empty
        // unless told how much to fetch. -1 is "all of it" (weasel#450).
        cmd.InitialLONGFetchSize = -1;

        foreach (var name in new[] { "schemaName", "indexOwner", "tableOwner" })
        {
            if (sql.Contains(':' + name, StringComparison.Ordinal))
            {
                cmd.Parameters.Add(new OracleParameter(name, schemaName));
            }
        }

        if (sql.Contains(":tableName", StringComparison.Ordinal))
        {
            cmd.Parameters.Add(new OracleParameter("tableName", tableName));
        }

        return await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
    }

    private static async Task readColumnsFromReaderAsync(
        DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var column = await readColumnAsync(reader, ct).ConfigureAwait(false);
            existing._columns.Add(column);
        }
    }

    private static async Task<(List<string>, string?)> readPrimaryKeysFromReaderAsync(
        DbDataReader reader, CancellationToken ct = default)
    {
        string? pkName = null;
        var pks = new List<string>();

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            pks.Add(await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false));
            pkName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
        }

        return (pks, pkName);
    }

    private static async Task readForeignKeysFromReaderAsync(
        DbDataReader reader, Table existing, CancellationToken ct = default)
    {
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

    private static async Task<Dictionary<string, IndexDefinition>> readIndexMetadataFromReaderAsync(
        DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        var indexes = new Dictionary<string, IndexDefinition>(StringComparer.OrdinalIgnoreCase);

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

        return indexes;
    }

    /// <summary>
    ///     For a descending or function-based index Oracle stores a system-generated column name
    ///     like <c>SYS_NC00006$</c>, and the real column only appears in the expression.
    /// </summary>
    private static async Task<Dictionary<(string, int), string>> readIndexExpressionsFromReaderAsync(
        DbDataReader reader, CancellationToken ct = default)
    {
        var expressionMap = new Dictionary<(string, int), string>();

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

        return expressionMap;
    }

    private static async Task readIndexColumnsFromReaderAsync(
        DbDataReader reader,
        Table existing,
        Dictionary<string, IndexDefinition> indexes,
        Dictionary<(string, int), string> expressionMap,
        CancellationToken ct = default)
    {
        var columnPositions = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var indexName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            if (!indexes.TryGetValue(indexName, out var index))
            {
                continue;
            }

            if (!columnPositions.ContainsKey(indexName))
            {
                columnPositions[indexName] = 1;
            }

            var position = columnPositions[indexName]++;

            var columnName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
            var descend = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);

            if (columnName.StartsWith("SYS_", StringComparison.OrdinalIgnoreCase)
                && expressionMap.TryGetValue((indexName.ToUpperInvariant(), position), out var expression))
            {
                // SYS_OP_DESCEND("USER_NAME"), or just "USER_NAME"
                var match = Regex.Match(expression, "\"([^\"]+)\"");
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

        // An index with no columns is one this reader could not make sense of -- a system-generated
        // or function-based index whose expression did not name a column. Better to leave it out of
        // the model than to report drift against something that cannot be reproduced.
        foreach (var emptyIndex in existing.Indexes.Where(i => !i.Columns.Any()).ToList())
        {
            existing.Indexes.Remove(emptyIndex);
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

}
