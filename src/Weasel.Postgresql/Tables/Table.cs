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

        public IReadOnlyList<string> PrimaryKeyColumns => _columns.Where(x => x.IsPrimaryKey).Select(x => x.Name).ToList();

        /// <summary>
        /// Generate the CREATE TABLE SQL expression with default
        /// DDL rules. This is useful for quick diagnostics
        /// </summary>
        /// <returns></returns>
        public string ToBasicCreateTableSql()
        {
            var writer = new StringWriter();
            var rules = new DdlRules{Formatting = DdlFormatting.Concise};
            Write(rules, writer);

            return writer.ToString();
        }
        
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
            
            if (rules.Formatting == DdlFormatting.Pretty)
            {
                var columnLength = Columns.Max(x => x.Name.Length) + 4;
                var typeLength = Columns.Max(x => x.Type.Length) + 4;

                var lines = Columns.Select(column =>
                        $"    {column.Name.PadRight(columnLength)}{column.Type.PadRight(typeLength)}{column.CheckDeclarations()}")
                    .ToList();

                if (PrimaryKeyColumns.Any())
                {
                    lines.Add(primaryKeyDeclaration());
                }

                for (int i = 0; i < lines.Count - 1; i++)
                {
                    writer.WriteLine(lines[i] + ",");
                }

                writer.WriteLine(lines.Last());
            }
            else
            {
                var lines = Columns
                    .Select(column => column.ToDeclaration())
                    .ToList();
                
                if (PrimaryKeyColumns.Any())
                {
                    lines.Add(primaryKeyDeclaration());
                }

                for (int i = 0; i < lines.Count - 1; i++)
                {
                    writer.WriteLine(lines[i] + ",");
                }

                writer.WriteLine(lines.Last());
            }
            



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

        private string primaryKeyDeclaration()
        {
            return $"CONSTRAINT {PrimaryKeyName} PRIMARY KEY ({PrimaryKeyColumns.Join(", ")})";
        }

        public void WriteDropStatement(DdlRules rules, StringWriter writer)
        {
            writer.WriteLine($"DROP TABLE IF EXISTS {Identifier} CASCADE;");
        }

        public DbObjectName Identifier { get; }

        private string _primaryKeyName = null;

        public string PrimaryKeyName
        {
            get => _primaryKeyName.IsNotEmpty() ? _primaryKeyName : $"pkey_{Identifier.Name}_{PrimaryKeyColumns.Join("_")}";
            set => _primaryKeyName = value;
        }
        
        public TableColumn ColumnFor(string columnName)
        {
            return Columns.FirstOrDefault(x => x.Name == columnName);
        }
        
        
        public bool HasColumn(string columnName)
        {
            return Columns.Any(x => x.Name == columnName);
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
        }

        public Table(string tableName) : this(DbObjectName.Parse(tableName))
        {
            
        }

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

            public ColumnExpression ForeignKeyTo(string referencedTableName, string referencedColumnName, string fkName = null, CascadeAction onDelete = CascadeAction.NoAction, CascadeAction onUpdate = CascadeAction.NoAction)
            {
                return ForeignKeyTo(new DbObjectName(referencedTableName), referencedColumnName, fkName, onDelete, onUpdate);
            }
            
            public ColumnExpression ForeignKeyTo(Table referencedTable, string referencedColumnName, string fkName = null, CascadeAction onDelete = CascadeAction.NoAction, CascadeAction onUpdate = CascadeAction.NoAction)
            {
                return ForeignKeyTo(referencedTable.Identifier, referencedColumnName, fkName, onDelete, onUpdate);
            }

            public ColumnExpression ForeignKeyTo(DbObjectName referencedIdentifier, string referencedColumnName,
                string fkName = null, CascadeAction onDelete = CascadeAction.NoAction, CascadeAction onUpdate = CascadeAction.NoAction)
            {
                var fk = new ForeignKey(fkName ?? _parent.Identifier.ToIndexName("fkey", _column.Name))
                {
                    LinkedTable = referencedIdentifier,
                    ColumnNames = new[] {_column.Name},
                    LinkedNames = new[] {referencedColumnName},
                    OnDelete = onDelete,
                    OnUpdate = onUpdate
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

            public ColumnExpression AddIndex(Action<IndexDefinition> configure = null)
            {
                var index = new IndexDefinition(_parent.Identifier.ToIndexName("idx", _column.Name))
                {
                    ColumnNames = new[]{_column.Name}
                };

                _parent.Indexes.Add(index);
                
                configure?.Invoke(index);

                return this;
            }

            public ColumnExpression Serial()
            {
                _column.Type = "SERIAL";
                return this;
            }

            public ColumnExpression DefaultValue(string value)
            {
                return DefaultValueByExpression($"'{value}'");
            }
            
            public ColumnExpression DefaultValue(int value)
            {
                return DefaultValueByExpression(value.ToString());
            }
            
            public ColumnExpression DefaultValue(long value)
            {
                return DefaultValueByExpression(value.ToString());
            }
            
            public ColumnExpression DefaultValue(double value)
            {
                return DefaultValueByExpression(value.ToString());
            }

            public ColumnExpression DefaultValueFromSequence(Sequence sequence)
            {
                return DefaultValueFromSequence(sequence.Identifier);
            }
            
            public ColumnExpression DefaultValueFromSequence(DbObjectName sequenceName)
            {
                return DefaultValueByExpression($"nextval('{sequenceName}')");
            }

            public ColumnExpression DefaultValueByExpression(string expression)
            {
                var check = new DefaultValue(expression);
                _column.ColumnChecks.Add(check);

                return this;
            }
        }

        public void RemoveColumn(string columnName)
        {
            _columns.RemoveAll(x => x.Name.EqualsIgnoreCase(columnName));
        }

        public ColumnExpression ModifyColumn(string columnName)
        {
            var column = ColumnFor(columnName) ?? throw new ArgumentOutOfRangeException($"Column '{columnName}' does not exist in table {Identifier}");
            return new ColumnExpression(this, column);
        }
    }
}