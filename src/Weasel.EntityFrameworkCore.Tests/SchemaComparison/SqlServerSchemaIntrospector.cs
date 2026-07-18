using Microsoft.Data.SqlClient;

namespace Weasel.EntityFrameworkCore.Tests.SchemaComparison;

/// <summary>
///     Reads a complete <see cref="SchemaSnapshot" /> of one SQL Server schema
///     straight from the sys.* catalog views — independent of both EF Core's
///     and Weasel's own introspection.
/// </summary>
public static class SqlServerSchemaIntrospector
{
    public static async Task<SchemaSnapshot> SnapshotAsync(SqlConnection conn, string schemaName)
    {
        var columns = await readColumnsAsync(conn, schemaName);
        var indexes = await readIndexesAsync(conn, schemaName);
        var foreignKeys = await readForeignKeysAsync(conn, schemaName);
        var checks = await readCheckConstraintsAsync(conn, schemaName);
        var sequences = await readSequencesAsync(conn, schemaName);

        var tableNames = columns.Keys.OrderBy(x => x).ToList();

        var tables = tableNames.Select(tableName =>
        {
            var tableIndexes = indexes.TryGetValue(tableName, out var idx) ? idx : [];
            var pk = tableIndexes.FirstOrDefault(i => i.IsPrimaryKey);

            return new TableSnapshot
            {
                Name = tableName,
                Columns = columns[tableName],
                PrimaryKeyName = pk?.Name,
                PrimaryKeyColumns = pk?.KeyColumns ?? [],
                ForeignKeys = foreignKeys.TryGetValue(tableName, out var fks) ? fks : [],
                Indexes = tableIndexes,
                CheckConstraints = checks.TryGetValue(tableName, out var cks) ? cks : []
            };
        }).ToList();

        return new SchemaSnapshot(schemaName, tables, sequences);
    }

    private static async Task<List<SequenceSnapshot>> readSequencesAsync(SqlConnection conn, string schemaName)
    {
        const string sql = """
            select name, cast(start_value as bigint), cast(increment as bigint)
            from sys.sequences
            where schema_id = SCHEMA_ID(@schema)
            """;

        var results = new List<SequenceSnapshot>();

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("schema", schemaName);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            results.Add(new SequenceSnapshot
            {
                Name = reader.GetString(0),
                StartValue = reader.GetInt64(1),
                IncrementBy = reader.GetInt64(2)
            });
        }

        return results;
    }

    private static async Task<Dictionary<string, List<ColumnSnapshot>>> readColumnsAsync(
        SqlConnection conn, string schemaName)
    {
        const string sql = """
            select t.name,
                   c.name,
                   ty.name,
                   c.max_length,
                   c.precision,
                   c.scale,
                   c.is_nullable,
                   c.is_identity,
                   c.is_computed,
                   dc.definition
            from sys.tables t
            join sys.schemas s on s.schema_id = t.schema_id
            join sys.columns c on c.object_id = t.object_id
            join sys.types ty on ty.user_type_id = c.user_type_id
            left join sys.default_constraints dc on dc.object_id = c.default_object_id
            where s.name = @schema
            order by t.name, c.column_id
            """;

        var results = new Dictionary<string, List<ColumnSnapshot>>();

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("schema", schemaName);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var tableName = reader.GetString(0);
            var column = new ColumnSnapshot
            {
                Name = reader.GetString(1),
                DataType = formatDataType(
                    reader.GetString(2), reader.GetInt16(3), reader.GetByte(4), reader.GetByte(5)),
                IsNullable = reader.GetBoolean(6),
                IsIdentity = reader.GetBoolean(7),
                IsComputed = reader.GetBoolean(8),
                DefaultExpression = reader.IsDBNull(9) ? null : reader.GetString(9)
            };

            results.GetOrAdd(tableName).Add(column);
        }

        return results;
    }

    private static string formatDataType(string typeName, short maxLength, byte precision, byte scale)
    {
        switch (typeName)
        {
            case "nvarchar" or "nchar":
                return maxLength == -1 ? $"{typeName}(max)" : $"{typeName}({maxLength / 2})";
            case "varchar" or "char" or "varbinary" or "binary":
                return maxLength == -1 ? $"{typeName}(max)" : $"{typeName}({maxLength})";
            case "decimal" or "numeric":
                return $"{typeName}({precision},{scale})";
            case "datetime2" or "datetimeoffset" or "time":
                return scale == 7 ? typeName : $"{typeName}({scale})";
            default:
                return typeName;
        }
    }

    private static async Task<Dictionary<string, List<IndexSnapshot>>> readIndexesAsync(
        SqlConnection conn, string schemaName)
    {
        const string sql = """
            select t.name,
                   i.name,
                   i.is_unique,
                   i.is_primary_key,
                   i.is_unique_constraint,
                   i.type_desc,
                   i.filter_definition,
                   ic.key_ordinal,
                   ic.is_included_column,
                   ic.is_descending_key,
                   col.name
            from sys.indexes i
            join sys.tables t on t.object_id = i.object_id
            join sys.schemas s on s.schema_id = t.schema_id
            join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id
            join sys.columns col on col.object_id = ic.object_id and col.column_id = ic.column_id
            where s.name = @schema and i.type > 0
            order by t.name, i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id
            """;

        var rows = new List<(string Table, string Index, bool IsUnique, bool IsPk, bool IsUniqueConstraint,
            string TypeDesc, string? Filter, bool IsIncluded, bool IsDescending, string Column)>();

        await using (var cmd = new SqlCommand(sql, conn))
        {
            cmd.Parameters.AddWithValue("schema", schemaName);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                rows.Add((reader.GetString(0), reader.GetString(1), reader.GetBoolean(2), reader.GetBoolean(3),
                    reader.GetBoolean(4), reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetString(6),
                    reader.GetBoolean(8), reader.GetBoolean(9), reader.GetString(10)));
            }
        }

        var results = new Dictionary<string, List<IndexSnapshot>>();
        foreach (var indexGroup in rows.GroupBy(r => (r.Table, r.Index)))
        {
            var first = indexGroup.First();
            var keyColumns = indexGroup.Where(r => !r.IsIncluded).ToList();

            results.GetOrAdd(first.Table).Add(new IndexSnapshot
            {
                Name = first.Index,
                IsUnique = first.IsUnique,
                IsPrimaryKey = first.IsPk,
                IsConstraintBacked = first.IsPk || first.IsUniqueConstraint,
                Method = first.TypeDesc,
                Predicate = first.Filter,
                KeyColumns = keyColumns.Select(r => r.Column).ToList(),
                IsDescending = keyColumns.Select(r => r.IsDescending).ToList(),
                IncludedColumns = indexGroup.Where(r => r.IsIncluded).Select(r => r.Column).ToList()
            });
        }

        return results;
    }

    private static async Task<Dictionary<string, List<ForeignKeySnapshot>>> readForeignKeysAsync(
        SqlConnection conn, string schemaName)
    {
        const string sql = """
            select t.name,
                   fk.name,
                   pc.name as dependent_column,
                   ps.name as principal_schema,
                   pt.name as principal_table,
                   prc.name as principal_column,
                   fk.delete_referential_action_desc,
                   fkc.constraint_column_id
            from sys.foreign_keys fk
            join sys.tables t on t.object_id = fk.parent_object_id
            join sys.schemas s on s.schema_id = t.schema_id
            join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
            join sys.columns pc on pc.object_id = fkc.parent_object_id and pc.column_id = fkc.parent_column_id
            join sys.tables pt on pt.object_id = fk.referenced_object_id
            join sys.schemas ps on ps.schema_id = pt.schema_id
            join sys.columns prc on prc.object_id = fkc.referenced_object_id and prc.column_id = fkc.referenced_column_id
            where s.name = @schema
            order by t.name, fk.name, fkc.constraint_column_id
            """;

        var rows = new List<(string Table, string Fk, string Column, string PrincipalSchema, string PrincipalTable,
            string PrincipalColumn, string DeleteAction)>();

        await using (var cmd = new SqlCommand(sql, conn))
        {
            cmd.Parameters.AddWithValue("schema", schemaName);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
                    reader.GetString(4), reader.GetString(5), reader.GetString(6)));
            }
        }

        var results = new Dictionary<string, List<ForeignKeySnapshot>>();
        foreach (var fkGroup in rows.GroupBy(r => (r.Table, r.Fk)))
        {
            var first = fkGroup.First();
            results.GetOrAdd(first.Table).Add(new ForeignKeySnapshot
            {
                Name = first.Fk,
                Columns = fkGroup.Select(r => r.Column).ToList(),
                PrincipalSchema = first.PrincipalSchema,
                PrincipalTable = first.PrincipalTable,
                PrincipalColumns = fkGroup.Select(r => r.PrincipalColumn).ToList(),
                OnDelete = first.DeleteAction.Replace('_', ' ')
            });
        }

        return results;
    }

    private static async Task<Dictionary<string, List<CheckConstraintSnapshot>>> readCheckConstraintsAsync(
        SqlConnection conn, string schemaName)
    {
        const string sql = """
            select t.name, cc.name, cc.definition
            from sys.check_constraints cc
            join sys.tables t on t.object_id = cc.parent_object_id
            join sys.schemas s on s.schema_id = t.schema_id
            where s.name = @schema
            """;

        var results = new Dictionary<string, List<CheckConstraintSnapshot>>();

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("schema", schemaName);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            results.GetOrAdd(reader.GetString(0)).Add(new CheckConstraintSnapshot
            {
                Name = reader.GetString(1),
                Expression = reader.GetString(2)
            });
        }

        return results;
    }

    private static List<TValue> GetOrAdd<TValue>(this Dictionary<string, List<TValue>> dict, string key)
    {
        if (!dict.TryGetValue(key, out var list))
        {
            list = new List<TValue>();
            dict[key] = list;
        }

        return list;
    }
}
