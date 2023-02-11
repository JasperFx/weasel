using System.Data.Common;
using Npgsql;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Postgresql.Tables;

public partial class Table
{
    public void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;
        var nameWithSchemaParam = builder.AddParameter(Identifier.QualifiedName).ParameterName;

        builder.Append($@"
select column_name, data_type, character_maximum_length, udt_name
from information_schema.columns where table_schema = :{schemaParam} and table_name = :{nameParam}
order by ordinal_position;

select a.attname, format_type(a.atttypid, a.atttypmod) as data_type
from pg_index i
join   pg_attribute a on a.attrelid = i.indrelid and a.attnum = ANY(i.indkey)
where attrelid = (select pg_class.oid
                  from pg_class
                  join pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
                  where n.nspname = :{schemaParam} and relname = :{nameParam})
and i.indisprimary;

SELECT *
FROM (
    SELECT
      R.rolname                AS user_name,
      ns.nspname               AS schema_name,
      pg_catalog.textin(pg_catalog.regclassout(idx.indrelid :: REGCLASS)) AS table_name,
      i.relname                AS index_name,
      pg_get_indexdef(i.oid) as ddl,
      idx.indisunique          AS is_unique,
      idx.indisprimary         AS is_primary,
      am.amname                AS index_type,
      idx.indkey,
           ARRAY(
               SELECT pg_get_indexdef(idx.indexrelid, k + 1, TRUE)
               FROM
                 generate_subscripts(idx.indkey, 1) AS k
               ORDER BY k
           ) AS index_keys,
      (idx.indexprs IS NOT NULL) OR (idx.indkey::int[] @> array[0]) AS is_functional,
      idx.indpred IS NOT NULL AS is_partial
    FROM pg_index AS idx
      JOIN pg_class AS i
        ON i.oid = idx.indexrelid
      JOIN pg_am AS am
        ON i.relam = am.oid
      JOIN pg_namespace AS NS ON i.relnamespace = NS.OID
      JOIN pg_roles AS R ON i.relowner = r.oid
    WHERE
      nspname = :{schemaParam} AND
      NOT nspname LIKE 'pg%'
) ind
WHERE
      ind.table_name = :{nameParam} OR
      ind.table_name = :{nameWithSchemaParam};

SELECT c.conname                                     AS constraint_name,
       c.contype                                     AS constraint_type,
       sch.nspname                                   AS schema_name,
       tbl.relname                                   AS table_name,
       ARRAY_AGG(col.attname ORDER BY u.attposition) AS columns,
       pg_get_constraintdef(c.oid)                   AS definition
FROM pg_constraint c
       JOIN LATERAL UNNEST(c.conkey) WITH ORDINALITY AS u(attnum, attposition) ON TRUE
       JOIN pg_class tbl ON tbl.oid = c.conrelid
       JOIN pg_namespace sch ON sch.oid = tbl.relnamespace
       JOIN pg_attribute col ON (col.attrelid = tbl.oid AND col.attnum = u.attnum)
WHERE
	c.contype = 'f' and
	sch.nspname = :{schemaParam} and
	tbl.relname = :{nameParam}
GROUP BY constraint_name, constraint_type, schema_name, table_name, definition;

SHOW max_identifier_length;
");

        if (PartitionStrategy != PartitionStrategy.None)
        {
            builder.Append($@"
select
    col.column_name,
    partition_strategy
from
    (select
         partrelid,
         partnatts,
         case partstrat
             when 'l' then 'list'
             when 'r' then 'range' end as partition_strategy,
         unnest(partattrs) column_index
     from
         pg_partitioned_table) pt
        join
    pg_class par
    on
            par.oid = pt.partrelid
        join
    information_schema.columns col
    on
                col.table_schema = par.relnamespace::regnamespace::text
            and col.table_name = par.relname
            and ordinal_position = pt.column_index
where
    col.table_schema = :{schemaParam} and table_name = :{nameParam}
order by column_index;
");
        }
    }

    public async Task<Table?> FetchExistingAsync(NpgsqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);

        ConfigureQueryCommand(builder);

        await using var reader = await builder.ExecuteReaderAsync(conn, ct).ConfigureAwait(false);
        return await readExistingAsync(reader, ct).ConfigureAwait(false);
    }

    private async Task<Table?> readExistingAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = new Table(Identifier);

        await readColumnsAsync(reader, existing, ct).ConfigureAwait(false);

        var pks = await readPrimaryKeysAsync(reader, ct).ConfigureAwait(false);
        await readIndexesAsync(reader, existing, ct).ConfigureAwait(false);
        await readConstraintsAsync(reader, existing, ct).ConfigureAwait(false);

        foreach (var pkColumn in pks) existing.ColumnFor(pkColumn)!.IsPrimaryKey = true;

        await readMaxIdentifierLength(reader, existing, ct).ConfigureAwait(false);

        if (PartitionStrategy != PartitionStrategy.None)
        {
            await readPartitionsAsync(reader, existing, ct).ConfigureAwait(false);
        }

        return !existing.Columns.Any()
            ? null
            : existing;
    }

    private static async Task readMaxIdentifierLength(
        DbDataReader reader,
        Table existing,
        CancellationToken ct = default
    )
    {
        await reader.NextResultAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var str = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            if (str != null && int.TryParse(str, out var maxIdentifierLength))
            {
                existing.MaxIdentifierLength = maxIdentifierLength;
            }
        }
    }

    private async Task readPartitionsAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        await reader.NextResultAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var strategy = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
            var columnOrExpression = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);

            existing.PartitionExpressions.Add(columnOrExpression);

            switch (strategy)
            {
                case "range":
                    existing.PartitionStrategy = PartitionStrategy.Range;
                    break;
            }
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
        var column = new TableColumn(await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false),
            await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false));

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

    private async Task readConstraintsAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        await reader.NextResultAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var schema = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);
            var definition = await reader.GetFieldValueAsync<string>(5, ct).ConfigureAwait(false);

            var fk = new ForeignKey(name);
            fk.Parse(definition, schema);

            existing.ForeignKeys.Add(fk);
        }
    }

    private async Task readIndexesAsync(DbDataReader reader, Table existing, CancellationToken ct = default)
    {
        await reader.NextResultAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            if (await reader.IsDBNullAsync(2, ct).ConfigureAwait(false))
            {
                continue;
            }

            var isPrimary = await reader.GetFieldValueAsync<bool>(6, ct).ConfigureAwait(false);
            if (isPrimary)
            {
                existing.PrimaryKeyName = await reader.GetFieldValueAsync<string>(3, ct).ConfigureAwait(false);
                continue;
            }

            var schemaName = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);
            var tableName = await reader.GetFieldValueAsync<string>(2, ct).ConfigureAwait(false);
            var ddl = await reader.GetFieldValueAsync<string>(4, ct).ConfigureAwait(false);


            if ((Identifier.Schema == schemaName && Identifier.Name == tableName) ||
                Identifier.QualifiedName == tableName)
            {
                var index = IndexDefinition.Parse(ddl);

                existing.Indexes.Add(index);
            }
        }
    }

    private static async Task<List<string>> readPrimaryKeysAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var pks = new List<string>();
        await reader.NextResultAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            pks.Add(await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false));
        }

        return pks;
    }
}
