using System.Data.Common;
using MySqlConnector;
using Weasel.Core;

namespace Weasel.MySql.Tables;

public partial class Table
{
    public void ConfigureQueryCommand(Core.DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

        builder.Append($@"
-- Columns
SELECT
    COLUMN_NAME,
    COLUMN_TYPE,
    IS_NULLABLE,
    COLUMN_KEY,
    COLUMN_DEFAULT,
    EXTRA
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = @{schemaParam} AND TABLE_NAME = @{nameParam}
ORDER BY ORDINAL_POSITION;

-- Primary Key
SELECT
    kcu.COLUMN_NAME,
    tc.CONSTRAINT_NAME
FROM information_schema.TABLE_CONSTRAINTS tc
JOIN information_schema.KEY_COLUMN_USAGE kcu
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
    AND tc.TABLE_NAME = kcu.TABLE_NAME
WHERE tc.TABLE_SCHEMA = @{schemaParam}
    AND tc.TABLE_NAME = @{nameParam}
    AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
ORDER BY kcu.ORDINAL_POSITION;

-- Foreign Keys
SELECT
    tc.CONSTRAINT_NAME,
    kcu.COLUMN_NAME,
    kcu.REFERENCED_TABLE_SCHEMA,
    kcu.REFERENCED_TABLE_NAME,
    kcu.REFERENCED_COLUMN_NAME,
    rc.DELETE_RULE,
    rc.UPDATE_RULE
FROM information_schema.TABLE_CONSTRAINTS tc
JOIN information_schema.KEY_COLUMN_USAGE kcu
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
    AND tc.TABLE_NAME = kcu.TABLE_NAME
JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
    ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
    AND tc.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
WHERE tc.TABLE_SCHEMA = @{schemaParam}
    AND tc.TABLE_NAME = @{nameParam}
    AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION;

-- Indexes (excluding primary key)
SELECT
    s.INDEX_NAME,
    s.COLUMN_NAME,
    s.NON_UNIQUE,
    s.INDEX_TYPE,
    s.SEQ_IN_INDEX
FROM information_schema.STATISTICS s
WHERE s.TABLE_SCHEMA = @{schemaParam}
    AND s.TABLE_NAME = @{nameParam}
    AND s.INDEX_NAME != 'PRIMARY'
ORDER BY s.INDEX_NAME, s.SEQ_IN_INDEX;
");
    }

    public async Task<Table?> FetchExistingAsync(MySqlConnection conn, CancellationToken ct = default)
    {
        var builder = new Core.DbCommandBuilder(conn);

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
        foreach (var pkColumn in pks)
        {
            var col = existing.ColumnFor(pkColumn);
            if (col != null)
            {
                col.IsPrimaryKey = true;
            }
        }

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
        var columnType = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
        var isNullable = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);
        var columnKey = await reader.GetFieldValueAsync<string>(3, ct).ConfigureAwait(false);
        var extra = await reader.IsDBNullAsync(5, ct).ConfigureAwait(false)
            ? ""
            : await reader.GetFieldValueAsync<string>(5, ct).ConfigureAwait(false);

        var column = new TableColumn(columnName, columnType.ToUpperInvariant())
        {
            AllowNulls = isNullable.Equals("YES", StringComparison.OrdinalIgnoreCase),
            IsPrimaryKey = columnKey.Equals("PRI", StringComparison.OrdinalIgnoreCase),
            IsAutoNumber = extra.Contains("auto_increment", StringComparison.OrdinalIgnoreCase)
        };

        if (!await reader.IsDBNullAsync(4, ct).ConfigureAwait(false))
        {
            column.DefaultExpression = await reader.GetFieldValueAsync<string>(4, ct).ConfigureAwait(false);
        }

        return column;
    }

    private static async Task<(List<string>, string?)> readPrimaryKeysAsync(
        DbDataReader reader,
        CancellationToken ct = default)
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

    private async Task readForeignKeysAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        await reader.NextResultAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var fkName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var columnName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
            var refSchema = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);
            var refTable = await reader.GetFieldValueAsync<string>(3, ct).ConfigureAwait(false);
            var refColumn = await reader.GetFieldValueAsync<string>(4, ct).ConfigureAwait(false);
            var onDelete = await reader.GetFieldValueAsync<string>(5, ct).ConfigureAwait(false);
            var onUpdate = await reader.GetFieldValueAsync<string>(6, ct).ConfigureAwait(false);

            var fk = existing.FindOrCreateForeignKey(fkName);
            fk.LinkedTable = new MySqlObjectName(refSchema, refTable);
            fk.ReadReferentialActions(onDelete, onUpdate);
            fk.LinkColumns(columnName, refColumn);
        }
    }

    private async Task readIndexesAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        await reader.NextResultAsync(ct).ConfigureAwait(false);

        var indexes = new Dictionary<string, IndexDefinition>(StringComparer.OrdinalIgnoreCase);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var indexName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var columnName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
            var nonUnique = await reader.GetFieldValueAsync<long>(2, ct).ConfigureAwait(false);
            var indexType = await reader.GetFieldValueAsync<string>(3, ct).ConfigureAwait(false);

            if (!indexes.TryGetValue(indexName, out var index))
            {
                index = new IndexDefinition(indexName)
                {
                    IsUnique = nonUnique == 0,
                    IndexType = ParseIndexType(indexType)
                };

                indexes.Add(indexName, index);
                existing.Indexes.Add(index);
            }

            index.AddColumn(columnName);
        }
    }

    private static MySqlIndexType ParseIndexType(string typeDesc)
    {
        return typeDesc.ToUpperInvariant() switch
        {
            "HASH" => MySqlIndexType.Hash,
            "FULLTEXT" => MySqlIndexType.Fulltext,
            "SPATIAL" => MySqlIndexType.Spatial,
            _ => MySqlIndexType.BTree
        };
    }
}
