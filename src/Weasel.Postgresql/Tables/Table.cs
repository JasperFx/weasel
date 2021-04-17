using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Baseline;
using Npgsql;

namespace Weasel.Postgresql.Tables
{
    public partial class Table : ISchemaObject
    {
        private readonly List<TableColumn> _columns = new List<TableColumn>();

        public IReadOnlyList<TableColumn> Columns => _columns;

        public IList<ForeignKey> ForeignKeys { get; } = new List<ForeignKey>();
        public IList<IIndexDefinition> Indexes { get; } = new List<IIndexDefinition>();
        

        public void Write(DdlRules rules, StringWriter writer)
        {
            if (rules.TableCreation == CreationStyle.DropThenCreate)
            {
                writer.WriteLine("DROP TABLE IF EXISTS {0} CASCADE;", Identifier);
                writer.WriteLine("CREATE TABLE {0} (", Identifier);
            }
            else
            {
                writer.WriteLine("CREATE TABLE IF NOT EXISTS {0} (", Identifier);
            }

            var columnLength = Columns.Max(x => x.Name.Length) + 4;
            var typeLength = Columns.Max(x => x.Type.Length) + 4;

            var lines = Columns.Select(column =>
                $"    {column.Name.PadRight(columnLength)}{column.Type.PadRight(typeLength)}{column.CheckDeclarations()}")
                .ToList();

            for (int i = 0; i < lines.Count - 1; i++)
            {
                writer.WriteLine(lines[i] + ",");
            }

            writer.WriteLine(lines.Last());

            // TODO -- write multi-column primary keys
            // if (PrimaryKeys.Any())
            // {
            //     writer.WriteLine($"   ,PRIMARY KEY ({PrimaryKeys.Select(x => x.Name).Join(", ")})");
            // }

            writer.WriteLine(");");

            // TODO -- support OriginWriter
            //writer.WriteLine(OriginWriter.OriginStatement("TABLE", Identifier.QualifiedName));

            foreach (var foreignKey in ForeignKeys)
            {
                writer.WriteLine();
                writer.WriteLine(foreignKey.ToDDL(this));
            }
            
            
            foreach (var index in Indexes)
            {
                writer.WriteLine();
                writer.WriteLine(index.ToDDL(this));
            }
        }

        public void WriteDropStatement(DdlRules rules, StringWriter writer)
        {
            writer.WriteLine($"DROP TABLE IF EXISTS {Identifier} CASCADE;");
        }

        public DbObjectName Identifier { get; }
        public void ConfigureQueryCommand(CommandBuilder builder)
        {
            var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
            var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

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
  NOT nspname LIKE 'pg%' AND
  i.relname like 'mt_%';

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
	c.conname like 'mt_%' and
	c.contype = 'f' and
	sch.nspname = :{schemaParam} and
	tbl.relname = :{nameParam}
GROUP BY constraint_name, constraint_type, schema_name, table_name, definition;

");
        }
        

        



        public SchemaPatchDifference CreatePatch(DbDataReader reader, SchemaPatch patch, AutoCreate autoCreate)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<DbObjectName> AllNames()
        {
            throw new NotImplementedException();
        }

        public Table(DbObjectName name)
        {
            Identifier = name ?? throw new ArgumentNullException(nameof(name));
            PrimaryKeyConstraintName = $"pk_{name.Name}";
        }

        public Table(string tableName) : this(new DbObjectName(tableName))
        {
            
        }
        
        /// <summary>
        /// The identifier for the primary key (if any). It may be valuable
        /// to name the constraint for upserts and other operations
        /// </summary>
        public string PrimaryKeyConstraintName { get; set; }

        public ColumnExpression AddColumn(TableColumn column)
        {
            _columns.Add(column);
            column.Parent = this;

            return new ColumnExpression(this, column);
        }

        public ColumnExpression AddColumn(string columnName, string columnType)
        {
            var column = new TableColumn(columnName, columnType) {Parent = this};
            return AddColumn(column);
        }

        public ColumnExpression AddColumn<T>(string columnName)
        {
            if (typeof(T).IsEnum)
            {
                throw new InvalidOperationException(
                    "Database column types cannot be automatically derived for enums. Explicitly specify as varchar or integer");
            }

            var type = TypeMappings.GetPgType(typeof(T), EnumStorage.AsInteger);
            return AddColumn(columnName, type);
        }

        public IEnumerable<TableColumn> PrimaryKeyColumns()
        {
            return _columns.Where(x => x.IsPrimaryKey);
        }

        public async Task<bool> ExistsInDatabase(NpgsqlConnection conn)
        {
            var cmd = conn
                .CreateCommand(
                    "SELECT * FROM pg_stat_user_tables WHERE relname = :table AND schemaname = :schema;")
                .With("table", Identifier.Name)
                .With("schema", Identifier.Schema);

            await using var reader = await cmd.ExecuteReaderAsync();
            var any = await reader.ReadAsync();
            return any;
        }

        public class ColumnExpression
        {
            private readonly Table _parent;
            private readonly TableColumn _column;

            public ColumnExpression(Table parent, TableColumn column)
            {
                _parent = parent;
                _column = column;
            }

            public ColumnExpression ForeignKeyTo(string referencedTableName, string referencedColumnName, string fkName = null)
            {
                return ForeignKeyTo(new DbObjectName(referencedTableName), referencedColumnName, fkName);
            }
            
            public ColumnExpression ForeignKeyTo(Table referencedTable, string referencedColumnName, string fkName = null)
            {
                return ForeignKeyTo(referencedTable.Identifier, referencedColumnName, fkName);
            }

            public ColumnExpression ForeignKeyTo(DbObjectName referencedIdentifier, string referencedColumnName,
                string fkName = null)
            {
                var fk = new ForeignKey(fkName ?? _parent.Identifier.ToIndexName("fkey", _column.Name))
                {
                    LinkedTable = referencedIdentifier,
                    ColumnNames = new[] {_column.Name},
                    LinkedNames = new[] {referencedColumnName}
                };

                _parent.ForeignKeys.Add(fk);

                return this;
            }
            
            /// <summary>
            /// Marks this column as being part of the parent table's primary key
            /// </summary>
            /// <returns></returns>
            public ColumnExpression AsPrimaryKey()
            {
                _column.IsPrimaryKey = true;
                return this;
            }
            
            public ColumnExpression AllowNulls()
            {
                _column.ColumnChecks.RemoveAll(x => x is INullConstraint);
                _column.ColumnChecks.Insert(0, new AllowNulls());
                return this;
            }

            public ColumnExpression NotNull()
            {
                _column.ColumnChecks.RemoveAll(x => x is INullConstraint);
                _column.ColumnChecks.Insert(0, new NotNull());
                return this;
            }

            public void AddIndex(Action<IndexDefinition> configure = null)
            {
                var index = new IndexDefinition(_parent.Identifier.ToIndexName("idx", _column.Name))
                {
                    ColumnNames = new[]{_column.Name}
                };

                _parent.Indexes.Add(index);
                
                configure?.Invoke(index);
            }
        }
    }
}