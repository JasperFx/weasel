using System.Data.Common;
using Microsoft.Data.Sqlite;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Sqlite.Tables;

public partial class Table
{
    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        // SQLite PRAGMA statements don't support parameter binding, so we use the table name directly
        // Sanitize the table name to prevent SQL injection by escaping single quotes
        var sanitizedName = Identifier.Name.Replace("'", "''");

        builder.Append($@"
-- Get table SQL definition
SELECT sql FROM sqlite_master
WHERE type = 'table' AND name = '{sanitizedName}';

-- Get column information using PRAGMA (PRAGMA doesn't support parameter binding)
{ColumnQuery(sanitizedName)}

-- Get index information
SELECT name, sql FROM sqlite_master
WHERE type = 'index' AND tbl_name = '{sanitizedName}' AND sql IS NOT NULL;

-- Get foreign key information (PRAGMA doesn't support parameter binding)
SELECT * FROM pragma_foreign_key_list('{sanitizedName}');

-- Get the triggers on this table. Not because the table owns them -- it does not, see
-- TriggerBase -- but because DROP TABLE takes them with it, and this table may have to be
-- rebuilt to change a column (weasel#452).
SELECT sql FROM sqlite_master
WHERE type = 'trigger' AND tbl_name = '{sanitizedName}' AND sql IS NOT NULL;
");
    }

    /// <summary>
    /// The column introspection query, shared by <see cref="ConfigureQueryCommand" /> and
    /// <see cref="FetchExistingAsync" /> so the two can never drift apart.
    /// <para>
    /// <c>table_xinfo</c> rather than <c>table_info</c>: <c>table_info</c> omits generated columns
    /// entirely, so a table declaring one was read back without it and the delta re-added it on every
    /// single run -- the second migration then failed with <c>duplicate column name</c> (weasel#426).
    /// </para>
    /// <para>
    /// The columns are listed explicitly, and in <c>table_info</c>'s order, because
    /// <see cref="readColumnsAsync" /> reads them positionally. <c>hidden</c> distinguishes the two:
    /// 0 is an ordinary column, 2 and 3 are VIRTUAL and STORED generated columns (both of which we
    /// want), and 1 is a virtual table's hidden column -- an fts5 table's, say -- which
    /// <c>table_info</c> never showed us and which we have no business reporting as a real column.
    /// </para>
    /// </summary>
    private static string ColumnQuery(string sanitizedName) =>
        $"""SELECT cid, name, type, "notnull", dflt_value, pk FROM pragma_table_xinfo('{sanitizedName}') WHERE hidden <> 1;""";

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
{ColumnQuery(tableName)}

-- Get index information
SELECT name, sql FROM sqlite_master
WHERE type = 'index' AND tbl_name = '{tableName}' AND sql IS NOT NULL;

-- Get foreign key information
SELECT * FROM pragma_foreign_key_list('{tableName}');

-- Get the triggers on this table (see ConfigureQueryCommand)
SELECT sql FROM sqlite_master
WHERE type = 'trigger' AND tbl_name = '{tableName}' AND sql IS NOT NULL;
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
            // Table doesn't exist. readTableSqlAsync already advanced past result set 1 to result set 2.
            // We must skip the remaining result sets (columns, indexes) to leave the reader on result set 4
            // (foreign keys), matching the position after reading all 4 result sets in the happy path.
            // This keeps the reader in sync when SchemaMigration.DetermineAsync batches multiple tables.
            await reader.NextResultAsync(ct).ConfigureAwait(false); // Skip columns → indexes
            await reader.NextResultAsync(ct).ConfigureAwait(false); // Skip indexes → foreign keys
            await reader.NextResultAsync(ct).ConfigureAwait(false); // Skip foreign keys → triggers
            return null;
        }

        // Read columns (second result set)
        await readColumnsAsync(reader, existing, ct).ConfigureAwait(false);

        // Read indexes (third result set)
        await readIndexesAsync(reader, existing, ct).ConfigureAwait(false);

        // Read foreign keys (fourth result set)
        await readForeignKeysAsync(reader, existing, ct).ConfigureAwait(false);

        // Read the triggers on this table (fifth result set)
        await reader.NextResultAsync(ct).ConfigureAwait(false);
        await readTriggersAsync(reader, existing, ct).ConfigureAwait(false);

        return !existing.Columns.Any() ? null : existing;
    }

    /// <summary>
    ///     Capture the <c>CREATE TRIGGER</c> statements for this table exactly as SQLite stores
    ///     them, so <c>TableDelta</c> can put them back after rebuilding the table.
    /// </summary>
    /// <remarks>
    ///     Verbatim, and from the catalog rather than from the model, on purpose: a trigger Weasel
    ///     never declared is still the user's, and dropping the table would take it with no warning.
    /// </remarks>
    private static async Task readTriggersAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) continue;

            var sql = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            if (!string.IsNullOrWhiteSpace(sql))
            {
                existing.ExistingTriggers.Add(sql.Trim());
            }
        }
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

        // ColumnQuery projects: cid, name, type, notnull, dflt_value, pk
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

    private IndexDefinition ParseIndexFromSql(string indexName, string sql)
    {
        // Parse CREATE [UNIQUE] INDEX name ON table (columns/expressions) [WHERE predicate]

        try
        {
            var index = new IndexDefinition(indexName);

            // Check for UNIQUE
            if (sql.Contains("UNIQUE INDEX", StringComparison.OrdinalIgnoreCase))
            {
                index.IsUnique = true;
            }

            // Find the column list opening paren and its balanced closing paren
            var openParen = sql.IndexOf('(');
            if (openParen < 0)
            {
                // Unparseable — store raw SQL as expression for fallback comparison
                index.Expression = sql;
                return index;
            }

            var closeParen = findMatchingParen(sql, openParen);
            if (closeParen < 0)
            {
                index.Expression = sql;
                return index;
            }

            var columnsPart = sql.Substring(openParen + 1, closeParen - openParen - 1).Trim();

            // Everything after the closing paren is the tail (may contain WHERE clause)
            var tail = sql.Substring(closeParen + 1);
            var whereMatch = tail.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase);
            if (whereMatch < 0)
            {
                // Also check if tail starts with "WHERE" (no leading space after paren)
                whereMatch = tail.TrimStart().StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase)
                    ? 0
                    : -1;
            }

            if (whereMatch >= 0)
            {
                var predicateStart = tail.IndexOf("WHERE", whereMatch, StringComparison.OrdinalIgnoreCase) + 5;
                index.Predicate = tail.Substring(predicateStart).Trim().TrimEnd(';');
            }

            // Determine if this is an expression index (contains nested parentheses or function calls)
            if (columnsPart.Contains('('))
            {
                index.Expression = columnsPart;
            }
            else
            {
                // Simple column index — parse individual columns
                parseSimpleColumns(index, columnsPart);
            }

            return index;
        }
        catch
        {
            // On parse failure, return an index with the raw SQL as expression for fallback comparison
            var fallback = new IndexDefinition(indexName) { Expression = sql };
            return fallback;
        }
    }

    /// <summary>
    /// Find the position of the closing parenthesis that matches the opening paren at <paramref name="openPos"/>.
    /// </summary>
    private static int findMatchingParen(string sql, int openPos)
    {
        var depth = 1;
        for (var i = openPos + 1; i < sql.Length; i++)
        {
            switch (sql[i])
            {
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    if (depth == 0)
                    {
                        return i;
                    }

                    break;
                case '\'':
                    // Skip string literals
                    i = sql.IndexOf('\'', i + 1);
                    if (i < 0)
                    {
                        return -1;
                    }

                    break;
            }
        }

        return -1;
    }

    /// <summary>
    /// Parse simple (non-expression) column definitions from the index column list.
    /// Handles per-column sort orders (ASC/DESC) and COLLATE clauses.
    /// </summary>
    /// <summary>
    ///     Split one entry of an index column list into the column name and whatever follows it
    ///     (ASC / DESC / COLLATE). A delimited name is read to its closing delimiter, doubling
    ///     included, so a space inside it does not end the name.
    /// </summary>
    private static (string Name, string Remainder) takeColumnName(string entry)
    {
        var open = entry[0];
        if (open is not ('"' or '[' or '`'))
        {
            var space = entry.IndexOf(' ');
            return space < 0 ? (entry, string.Empty) : (entry[..space], entry[(space + 1)..]);
        }

        var close = open == '[' ? ']' : open;
        var name = new System.Text.StringBuilder();

        for (var i = 1; i < entry.Length; i++)
        {
            if (entry[i] != close)
            {
                name.Append(entry[i]);
                continue;
            }

            // A doubled close delimiter is an escaped literal one, not the end of the name.
            if (i + 1 < entry.Length && entry[i + 1] == close)
            {
                name.Append(close);
                i++;
                continue;
            }

            return (name.ToString(), entry[(i + 1)..].Trim());
        }

        // Unterminated: treat the whole thing as the name rather than losing it.
        return (name.ToString(), string.Empty);
    }

    private static void parseSimpleColumns(IndexDefinition index, string columnsPart)
    {
        var columnNames = new List<string>();
        var hasDesc = false;
        var hasAsc = false;
        string? collation = null;

        foreach (var raw in columnsPart.Split(','))
        {
            var entry = raw.Trim();
            if (entry.Length == 0)
            {
                continue;
            }

            // The name has to come off before the rest is tokenized on spaces: a delimited name
            // may contain one. Splitting first turned an index over "order date" into an index
            // over "order" -- and, since weasel#458, a column name with a space is something a
            // caller can legitimately have (weasel#449).
            var (columnName, remainder) = takeColumnName(entry);
            columnNames.Add(columnName);

            var parts = remainder.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            // Scan remaining tokens for sort order and collation
            for (var i = 0; i < parts.Length; i++)
            {
                if (parts[i].Equals("DESC", StringComparison.OrdinalIgnoreCase))
                {
                    hasDesc = true;
                }
                else if (parts[i].Equals("ASC", StringComparison.OrdinalIgnoreCase))
                {
                    hasAsc = true;
                }
                else if (parts[i].Equals("COLLATE", StringComparison.OrdinalIgnoreCase) && i + 1 < parts.Length)
                {
                    collation = parts[i + 1];
                    i++; // skip the collation name
                }
            }
        }

        index.Columns = columnNames.ToArray();

        // If all columns share the same sort order, set it on the index.
        // Mixed sort orders (some ASC, some DESC) are stored as expression for accurate DDL round-tripping.
        if (hasDesc && hasAsc)
        {
            // Mixed sort orders — store as expression to preserve per-column ordering
            index.Columns = null;
            index.Expression = columnsPart;
        }
        else if (hasDesc)
        {
            index.SortOrder = SortOrder.Desc;
        }

        if (collation != null)
        {
            index.Collation = collation;
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
