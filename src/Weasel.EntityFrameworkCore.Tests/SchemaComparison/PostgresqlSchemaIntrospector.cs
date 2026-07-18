using Npgsql;

namespace Weasel.EntityFrameworkCore.Tests.SchemaComparison;

/// <summary>
///     Reads a complete <see cref="SchemaSnapshot" /> of one PostgreSQL schema
///     straight from the system catalogs. Deliberately independent from both
///     EF Core's and Weasel's own introspection so it can act as the neutral
///     referee between them.
/// </summary>
public static class PostgresqlSchemaIntrospector
{
    public static async Task<SchemaSnapshot> SnapshotAsync(NpgsqlConnection conn, string schemaName)
    {
        var columns = await readColumnsAsync(conn, schemaName);
        var keyConstraints = await readKeyConstraintsAsync(conn, schemaName);
        var foreignKeys = await readForeignKeysAsync(conn, schemaName);
        var indexes = await readIndexesAsync(conn, schemaName);
        var checks = await readCheckConstraintsAsync(conn, schemaName);

        var tableNames = columns.Keys
            .Union(keyConstraints.Keys)
            .Union(foreignKeys.Keys)
            .Union(indexes.Keys)
            .OrderBy(x => x)
            .ToList();

        var tables = tableNames.Select(tableName =>
        {
            var (pkName, pkColumns) = keyConstraints.TryGetValue(tableName, out var keys)
                ? keys.Where(k => k.IsPrimary).Select(k => (k.Name, k.Columns)).FirstOrDefault()
                : default;

            return new TableSnapshot
            {
                Name = tableName,
                Columns = columns.TryGetValue(tableName, out var cols) ? cols : [],
                PrimaryKeyName = pkName,
                PrimaryKeyColumns = pkColumns ?? [],
                ForeignKeys = foreignKeys.TryGetValue(tableName, out var fks) ? fks : [],
                Indexes = indexes.TryGetValue(tableName, out var idx) ? idx : [],
                CheckConstraints = checks.TryGetValue(tableName, out var cks) ? cks : []
            };
        }).ToList();

        return new SchemaSnapshot(schemaName, tables);
    }

    private static async Task<Dictionary<string, List<ColumnSnapshot>>> readColumnsAsync(
        NpgsqlConnection conn, string schemaName)
    {
        const string sql = """
            select c.table_name,
                   c.column_name,
                   c.data_type,
                   c.udt_name,
                   c.character_maximum_length,
                   c.numeric_precision,
                   c.numeric_scale,
                   c.datetime_precision,
                   c.is_nullable,
                   c.column_default,
                   c.is_identity,
                   c.is_generated
            from information_schema.columns c
            where c.table_schema = :schema
            order by c.table_name, c.ordinal_position
            """;

        var results = new Dictionary<string, List<ColumnSnapshot>>();

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("schema", schemaName);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var tableName = reader.GetString(0);
            var dataType = reader.GetString(2);
            var udtName = reader.GetString(3);
            var charLength = reader.IsDBNull(4) ? (int?)null : reader.GetInt32(4);
            var precision = reader.IsDBNull(5) ? (int?)null : reader.GetInt32(5);
            var scale = reader.IsDBNull(6) ? (int?)null : reader.GetInt32(6);
            var defaultExpr = reader.IsDBNull(9) ? null : reader.GetString(9);

            var datetimePrecision = reader.IsDBNull(7) ? (int?)null : reader.GetInt32(7);

            var column = new ColumnSnapshot
            {
                Name = reader.GetString(1),
                DataType = formatDataType(dataType, udtName, charLength, precision, scale, datetimePrecision),
                IsNullable = reader.GetString(8) == "YES",
                DefaultExpression = defaultExpr,
                IsIdentity = reader.GetString(10) == "YES",
                IsSerialStyle = defaultExpr?.StartsWith("nextval(", StringComparison.OrdinalIgnoreCase) == true,
                IsComputed = reader.GetString(11) != "NEVER"
            };

            results.GetOrAdd(tableName).Add(column);
        }

        return results;
    }

    private static string formatDataType(
        string dataType, string udtName, int? charLength, int? precision, int? scale, int? datetimePrecision)
    {
        switch (dataType)
        {
            case "character varying":
                return charLength.HasValue ? $"varchar({charLength})" : "varchar";
            case "character":
                return charLength.HasValue ? $"char({charLength})" : "char";
            case "numeric":
                if (precision.HasValue && scale.HasValue) return $"numeric({precision},{scale})";
                if (precision.HasValue) return $"numeric({precision})";
                return "numeric";
            case "timestamp with time zone" or "timestamp without time zone" or "time with time zone" or "time without time zone":
                // 6 is PostgreSQL's default; only surface an explicit reduced precision
                if (datetimePrecision is { } p and not 6)
                {
                    var baseWord = dataType.StartsWith("timestamp") ? "timestamp" : "time";
                    return $"{baseWord}({p}){dataType[baseWord.Length..]}";
                }

                return dataType;
            case "ARRAY":
                return $"{udtName.TrimStart('_')}[]";
            case "USER-DEFINED":
                return udtName;
            default:
                return dataType;
        }
    }

    private record KeyConstraint(string Name, List<string> Columns, bool IsPrimary);

    private static async Task<Dictionary<string, List<KeyConstraint>>> readKeyConstraintsAsync(
        NpgsqlConnection conn, string schemaName)
    {
        const string sql = """
            select t.relname,
                   con.conname,
                   con.contype,
                   (select array_agg(a.attname order by k.ord)
                      from unnest(con.conkey) with ordinality k(attnum, ord)
                      join pg_attribute a on a.attrelid = con.conrelid and a.attnum = k.attnum)
            from pg_constraint con
            join pg_class t on t.oid = con.conrelid
            join pg_namespace n on n.oid = t.relnamespace
            where n.nspname = :schema and con.contype in ('p', 'u')
            """;

        var results = new Dictionary<string, List<KeyConstraint>>();

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("schema", schemaName);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var tableName = reader.GetString(0);
            var constraint = new KeyConstraint(
                reader.GetString(1),
                reader.GetFieldValue<string[]>(3).ToList(),
                reader.GetChar(2) == 'p');

            results.GetOrAdd(tableName).Add(constraint);
        }

        return results;
    }

    private static async Task<Dictionary<string, List<ForeignKeySnapshot>>> readForeignKeysAsync(
        NpgsqlConnection conn, string schemaName)
    {
        const string sql = """
            select t.relname,
                   con.conname,
                   (select array_agg(a.attname order by k.ord)
                      from unnest(con.conkey) with ordinality k(attnum, ord)
                      join pg_attribute a on a.attrelid = con.conrelid and a.attnum = k.attnum),
                   fn.nspname,
                   ft.relname,
                   (select array_agg(a.attname order by k.ord)
                      from unnest(con.confkey) with ordinality k(attnum, ord)
                      join pg_attribute a on a.attrelid = con.confrelid and a.attnum = k.attnum),
                   con.confdeltype
            from pg_constraint con
            join pg_class t on t.oid = con.conrelid
            join pg_namespace n on n.oid = t.relnamespace
            join pg_class ft on ft.oid = con.confrelid
            join pg_namespace fn on fn.oid = ft.relnamespace
            where n.nspname = :schema and con.contype = 'f'
            """;

        var results = new Dictionary<string, List<ForeignKeySnapshot>>();

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("schema", schemaName);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var tableName = reader.GetString(0);
            var fk = new ForeignKeySnapshot
            {
                Name = reader.GetString(1),
                Columns = reader.GetFieldValue<string[]>(2),
                PrincipalSchema = reader.GetString(3),
                PrincipalTable = reader.GetString(4),
                PrincipalColumns = reader.GetFieldValue<string[]>(5),
                OnDelete = reader.GetChar(6) switch
                {
                    'c' => "CASCADE",
                    'n' => "SET NULL",
                    'd' => "SET DEFAULT",
                    'r' => "RESTRICT",
                    _ => "NO ACTION"
                }
            };

            results.GetOrAdd(tableName).Add(fk);
        }

        return results;
    }

    private static async Task<Dictionary<string, List<IndexSnapshot>>> readIndexesAsync(
        NpgsqlConnection conn, string schemaName)
    {
        const string sql = """
            select t.relname as table_name,
                   ic.relname as index_name,
                   i.indisunique,
                   i.indisprimary,
                   am.amname,
                   pg_get_expr(i.indpred, i.indrelid) as predicate,
                   i.indnkeyatts,
                   (select array_agg(o::int order by ord)
                      from unnest(i.indoption) with ordinality u(o, ord)) as options,
                   (select array_agg(coalesce(a.attname, pg_get_indexdef(i.indexrelid, k.ord::int, true)) order by k.ord)
                      from unnest(i.indkey) with ordinality k(attnum, ord)
                      left join pg_attribute a on a.attrelid = t.oid and a.attnum = k.attnum and k.attnum <> 0) as columns,
                   exists(select 1 from pg_constraint c where c.conindid = i.indexrelid) as constraint_backed
            from pg_index i
            join pg_class ic on ic.oid = i.indexrelid
            join pg_class t on t.oid = i.indrelid
            join pg_namespace n on n.oid = t.relnamespace
            join pg_am am on am.oid = ic.relam
            where n.nspname = :schema
            order by t.relname, ic.relname
            """;

        var results = new Dictionary<string, List<IndexSnapshot>>();

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("schema", schemaName);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var tableName = reader.GetString(0);
            var keyColumnCount = reader.GetInt16(6);
            var options = reader.GetFieldValue<int[]>(7);
            var allColumns = reader.GetFieldValue<string[]>(8);

            var index = new IndexSnapshot
            {
                Name = reader.GetString(1),
                IsUnique = reader.GetBoolean(2),
                IsPrimaryKey = reader.GetBoolean(3),
                Method = reader.GetString(4),
                Predicate = reader.IsDBNull(5) ? null : reader.GetString(5),
                KeyColumns = allColumns.Take(keyColumnCount).ToList(),
                IncludedColumns = allColumns.Skip(keyColumnCount).ToList(),
                // pg_index.indoption bit 0x1 = DESC
                IsDescending = options.Take(keyColumnCount).Select(o => (o & 1) == 1).ToList(),
                IsConstraintBacked = reader.GetBoolean(9)
            };

            results.GetOrAdd(tableName).Add(index);
        }

        return results;
    }

    private static async Task<Dictionary<string, List<CheckConstraintSnapshot>>> readCheckConstraintsAsync(
        NpgsqlConnection conn, string schemaName)
    {
        const string sql = """
            select t.relname, con.conname, pg_get_constraintdef(con.oid)
            from pg_constraint con
            join pg_class t on t.oid = con.conrelid
            join pg_namespace n on n.oid = t.relnamespace
            where n.nspname = :schema and con.contype = 'c'
            """;

        var results = new Dictionary<string, List<CheckConstraintSnapshot>>();

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("schema", schemaName);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var tableName = reader.GetString(0);
            results.GetOrAdd(tableName).Add(new CheckConstraintSnapshot
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
