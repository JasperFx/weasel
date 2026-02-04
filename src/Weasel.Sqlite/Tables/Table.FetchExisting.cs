using System.Data.Common;
using Microsoft.Data.Sqlite;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Sqlite.Tables;

public partial class Table
{
    public void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        // SQLite PRAGMA statements don't support parameter binding, so we use the table name directly
        // Sanitize the table name to prevent SQL injection by escaping single quotes
        var sanitizedName = Identifier.Name.Replace("'", "''");

        builder.Append($@"
-- Get table SQL definition
SELECT sql FROM sqlite_master
WHERE type = 'table' AND name = '{sanitizedName}';

-- Get column information using PRAGMA (PRAGMA doesn't support parameter binding)
SELECT * FROM pragma_table_info('{sanitizedName}');

-- Get index information
SELECT name, sql FROM sqlite_master
WHERE type = 'index' AND tbl_name = '{sanitizedName}' AND sql IS NOT NULL;

-- Get foreign key information (PRAGMA doesn't support parameter binding)
SELECT * FROM pragma_foreign_key_list('{sanitizedName}');
");
    }

    public async Task<Table?> FetchExistingAsync(SqliteConnection conn, CancellationToken ct = default)
    {
        // SQLite PRAGMAs don't support parameter binding, so we build the query directly
        // We sanitize the table name to prevent SQL injection
        var tableName = Identifier.Name.Replace("'", "''"); // Escape single quotes

        var sql = $@"
-- Get table SQL definition
SELECT sql FROM sqlite_master
WHERE type = 'table' AND name = '{tableName}';

-- Get column information using PRAGMA
SELECT * FROM pragma_table_info('{tableName}');

-- Get index information
SELECT name, sql FROM sqlite_master
WHERE type = 'index' AND tbl_name = '{tableName}' AND sql IS NOT NULL;

-- Get foreign key information
SELECT * FROM pragma_foreign_key_list('{tableName}');
";

        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            var result = await readExistingAsync(reader, ct).ConfigureAwait(false);
            await reader.CloseAsync().ConfigureAwait(false);
            return result;
        }
        catch (SqliteException)
        {
            // Table doesn't exist
            return null;
        }
    }

    private async Task<Table?> readExistingAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = new Table(Identifier);

        // Read table SQL (first result set)
        var tableSql = await readTableSqlAsync(reader, ct).ConfigureAwait(false);
        if (string.IsNullOrEmpty(tableSql))
        {
            return null; // Table doesn't exist
        }

        // Read columns (second result set)
        await readColumnsAsync(reader, existing, ct).ConfigureAwait(false);

        // Read indexes (third result set)
        await readIndexesAsync(reader, existing, ct).ConfigureAwait(false);

        // Read foreign keys (fourth result set)
        await readForeignKeysAsync(reader, existing, ct).ConfigureAwait(false);

        return !existing.Columns.Any() ? null : existing;
    }

    private static async Task<string?> readTableSqlAsync(DbDataReader reader, CancellationToken ct = default)
    {
        string? tableSql = null;

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            if (!await reader.IsDBNullAsync(0, ct).ConfigureAwait(false))
            {
                tableSql = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            }
        }

        await reader.NextResultAsync(ct).ConfigureAwait(false);
        return tableSql;
    }

    private async Task readColumnsAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        var primaryKeys = new List<string>();

        // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false); // name
            var type = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false); // type
            var notNull = await reader.GetFieldValueAsync<long>(3, ct).ConfigureAwait(false); // notnull
            var defaultValue = await reader.IsDBNullAsync(4, ct).ConfigureAwait(false)
                ? null
                : await reader.GetFieldValueAsync<string>(4, ct).ConfigureAwait(false); // dflt_value
            var pk = await reader.GetFieldValueAsync<long>(5, ct).ConfigureAwait(false); // pk

            var column = new TableColumn(name, type)
            {
                Parent = existing,
                AllowNulls = notNull == 0,
                DefaultExpression = defaultValue,
                IsPrimaryKey = pk > 0
            };

            if (pk > 0)
            {
                primaryKeys.Add(name);
            }

            existing._columns.Add(column);
        }

        if (primaryKeys.Any())
        {
            existing.ReadPrimaryKeyColumns(primaryKeys);
        }

        await reader.NextResultAsync(ct).ConfigureAwait(false);
    }

    private async Task readIndexesAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        // sqlite_master returns: name, sql
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var indexName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var indexSql = await reader.IsDBNullAsync(1, ct).ConfigureAwait(false)
                ? null
                : await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);

            if (string.IsNullOrEmpty(indexSql))
            {
                continue; // Skip auto-created indexes (like for PRIMARY KEY)
            }

            // Parse index from SQL
            var index = ParseIndexFromSql(indexName, indexSql);
            if (index != null)
            {
                existing.Indexes.Add(index);
            }
        }

        await reader.NextResultAsync(ct).ConfigureAwait(false);
    }

    private async Task readForeignKeysAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        // PRAGMA foreign_key_list returns: id, seq, table, from, to, on_update, on_delete, match
        var foreignKeyGroups = new Dictionary<long, ForeignKey>();

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var id = await reader.GetFieldValueAsync<long>(0, ct).ConfigureAwait(false);
            var seq = await reader.GetFieldValueAsync<long>(1, ct).ConfigureAwait(false);
            var table = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);
            var from = await reader.GetFieldValueAsync<string>(3, ct).ConfigureAwait(false);
            var to = await reader.GetFieldValueAsync<string>(4, ct).ConfigureAwait(false);
            var onUpdate = await reader.GetFieldValueAsync<string>(5, ct).ConfigureAwait(false);
            var onDelete = await reader.GetFieldValueAsync<string>(6, ct).ConfigureAwait(false);

            if (!foreignKeyGroups.TryGetValue(id, out var fk))
            {
                fk = new ForeignKey($"fk_{existing.Identifier.Name}_{table}_{id}")
                {
                    LinkedTable = new SqliteObjectName(table)
                };
                fk.ReadReferentialActions(onDelete, onUpdate);
                foreignKeyGroups[id] = fk;
                existing.ForeignKeys.Add(fk);
            }

            fk.LinkColumns(from, to);
        }
    }

    private IndexDefinition? ParseIndexFromSql(string indexName, string sql)
    {
        // Basic parsing of CREATE INDEX statements
        // Example: CREATE INDEX idx_name ON table_name (column1, column2)
        // Example: CREATE UNIQUE INDEX idx_name ON table_name (column1 DESC) WHERE condition

        try
        {
            var index = new IndexDefinition(indexName);

            // Check for UNIQUE
            if (sql.Contains("UNIQUE INDEX", StringComparison.OrdinalIgnoreCase))
            {
                index.IsUnique = true;
            }

            // Extract WHERE clause for partial indexes
            var whereIndex = sql.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase);
            if (whereIndex > 0)
            {
                var endIndex = sql.LastIndexOf(')');
                if (endIndex > whereIndex)
                {
                    index.Predicate = sql.Substring(whereIndex + 7, endIndex - whereIndex - 7).Trim();
                }
                else
                {
                    index.Predicate = sql.Substring(whereIndex + 7).Trim();
                }
            }

            // Extract columns from between parentheses
            var openParen = sql.IndexOf('(');
            var closeParen = whereIndex > 0 ? whereIndex : sql.LastIndexOf(')');

            if (openParen > 0 && closeParen > openParen)
            {
                var columnsPart = sql.Substring(openParen + 1, closeParen - openParen - 1).Trim();

                // Check if this is an expression index (contains functions)
                if (columnsPart.Contains("(") || columnsPart.Contains("json_extract"))
                {
                    index.Expression = columnsPart;
                }
                else
                {
                    // Simple column index
                    var columns = columnsPart.Split(',')
                        .Select(c => c.Trim().Split(' ')[0].Trim('"', '[', ']'))
                        .ToArray();
                    index.Columns = columns;

                    // Check for DESC
                    if (columnsPart.Contains(" DESC", StringComparison.OrdinalIgnoreCase))
                    {
                        index.SortOrder = SortOrder.Desc;
                    }
                }
            }

            return index;
        }
        catch
        {
            // If parsing fails, return null and skip this index
            return null;
        }
    }

    public void ReadPrimaryKeyColumns(List<string> pks)
    {
        _primaryKeyColumns.Clear();
        _primaryKeyColumns.AddRange(pks);
    }

    /// <summary>
    /// Check if this table exists in the database
    /// </summary>
    public async Task<bool> ExistsInDatabaseAsync(SqliteConnection conn, CancellationToken ct = default)
    {
        var cmd = conn.CreateCommand();

        var schema = Identifier.Schema;
        var sqliteMaster = schema.Equals("main", StringComparison.OrdinalIgnoreCase)
            ? "sqlite_master"
            : $"{schema}.sqlite_master";

        cmd.CommandText = $"SELECT COUNT(*) FROM {sqliteMaster} WHERE type = 'table' AND name = @name";
        cmd.Parameters.AddWithValue("@name", Identifier.Name);

        var count = (long)(await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) ?? 0L);
        return count > 0;
    }
}
