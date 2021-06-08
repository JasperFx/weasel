using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Baseline;
using Weasel.Core;

namespace Weasel.SqlServer.Tables
{
    public enum PartitionStrategy
    {
        /// <summary>
        ///     No partitioning
        /// </summary>
        None,

        /// <summary>
        ///     Postgresql PARTITION BY RANGE semantics
        /// </summary>
        Range


        //List
    }

    public partial class Table : ISchemaObject
    {
        private readonly List<TableColumn> _columns = new();

        private string _primaryKeyName;

        public Table(DbObjectName name)
        {
            Identifier = name ?? throw new ArgumentNullException(nameof(name));
        }

        public Table(string tableName) : this(DbObjectName.Parse(tableName))
        {
        }

        public IReadOnlyList<TableColumn> Columns => _columns;

        public IList<ForeignKey> ForeignKeys { get; } = new List<ForeignKey>();
        public IList<IndexDefinition> Indexes { get; } = new List<IndexDefinition>();

        public IReadOnlyList<string> PrimaryKeyColumns =>
            _columns.Where(x => x.IsPrimaryKey).Select(x => x.Name).ToList();

        public IList<string> PartitionExpressions { get; } = new List<string>();


        /// <summary>
        ///     PARTITION strategy for this table
        /// </summary>
        public PartitionStrategy PartitionStrategy { get; set; } = PartitionStrategy.None;

        public string PrimaryKeyName
        {
            get => _primaryKeyName.IsNotEmpty()
                ? _primaryKeyName
                : $"pkey_{Identifier.Name}_{PrimaryKeyColumns.Join("_")}";
            set => _primaryKeyName = value;
        }

        public void WriteCreateStatement(DdlRules rules, TextWriter writer)
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
                        $"    {column.Name.PadRight(columnLength)}{column.Type.PadRight(typeLength)}{column.Declaration()}")
                    .ToList();

                if (PrimaryKeyColumns.Any())
                {
                    lines.Add(PrimaryKeyDeclaration());
                }

                for (var i = 0; i < lines.Count - 1; i++)
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
                    lines.Add(PrimaryKeyDeclaration());
                }

                for (var i = 0; i < lines.Count - 1; i++)
                {
                    writer.WriteLine(lines[i] + ",");
                }

                writer.WriteLine(lines.Last());
            }

            switch (PartitionStrategy)
            {
                case PartitionStrategy.None:
                    writer.WriteLine(");");
                    break;

                case PartitionStrategy.Range:
                    writer.WriteLine($") PARTITION BY RANGE ({PartitionExpressions.Join(", ")});");
                    break;
            }


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

        public void WriteDropStatement(DdlRules rules, TextWriter writer)
        {
            writer.WriteLine($"DROP TABLE IF EXISTS {Identifier} CASCADE;");
        }

        public DbObjectName Identifier { get; }


        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;

            foreach (var index in Indexes) yield return new DbObjectName(Identifier.Schema, index.Name);

            foreach (var fk in ForeignKeys) yield return new DbObjectName(Identifier.Schema, fk.Name);
        }

        /// <summary>
        ///     Generate the CREATE TABLE SQL expression with default
        ///     DDL rules. This is useful for quick diagnostics
        /// </summary>
        /// <returns></returns>
        public string ToBasicCreateTableSql()
        {
            var writer = new StringWriter();
            var rules = new DdlRules {Formatting = DdlFormatting.Concise};
            WriteCreateStatement(rules, writer);

            return writer.ToString();
        }


        internal string PrimaryKeyDeclaration()
        {
            return $"CONSTRAINT {PrimaryKeyName} PRIMARY KEY ({PrimaryKeyColumns.Join(", ")})";
        }

        public TableColumn ColumnFor(string columnName)
        {
            return Columns.FirstOrDefault(x => x.Name == columnName);
        }


        public bool HasColumn(string columnName)
        {
            return Columns.Any(x => x.Name == columnName);
        }

        public IndexDefinition IndexFor(string indexName)
        {
            return Indexes.FirstOrDefault(x => x.Name == indexName);
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

        public ColumnExpression AddColumn<T>() where T : TableColumn, new()
        {
            var column = new T();
            return AddColumn(column);
        }

        public ColumnExpression AddColumn<T>(string columnName)
        {
            if (typeof(T).IsEnum)
            {
                throw new InvalidOperationException(
                    "Database column types cannot be automatically derived for enums. Explicitly specify as varchar or integer");
            }

            var type = SqlServerProvider.Instance.GetDatabaseType(typeof(T), EnumStorage.AsInteger);
            return AddColumn(columnName, type);
        }

        public async Task<bool> ExistsInDatabase(SqlConnection conn)
        {
            var cmd = conn
                .CreateCommand(
                    "SELECT * FROM information_schema.tables WHERE table_name = :table AND table_schema = :schema;")
                .With("table", Identifier.Name)
                .With("schema", Identifier.Schema);

            await using var reader = await cmd.ExecuteReaderAsync();
            var any = await reader.ReadAsync();
            return any;
        }

        public void RemoveColumn(string columnName)
        {
            _columns.RemoveAll(x => x.Name.EqualsIgnoreCase(columnName));
        }

        public ColumnExpression ModifyColumn(string columnName)
        {
            var column = ColumnFor(columnName) ??
                         throw new ArgumentOutOfRangeException(
                             $"Column '{columnName}' does not exist in table {Identifier}");
            return new ColumnExpression(this, column);
        }

        public bool HasIndex(string indexName)
        {
            return Indexes.Any(x => x.Name == indexName);
        }

        public void PartitionByRange(params string[] columnOrExpressions)
        {
            PartitionStrategy = PartitionStrategy.Range;
            PartitionExpressions.Clear();
            PartitionExpressions.AddRange(columnOrExpressions);
        }

        public void ClearPartitions()
        {
            PartitionStrategy = PartitionStrategy.None;
            PartitionExpressions.Clear();
        }

        public class ColumnExpression
        {
            private readonly Table _parent;

            public ColumnExpression(Table parent, TableColumn column)
            {
                _parent = parent;
                Column = column;
            }

            internal TableColumn Column { get; }

            public ColumnExpression ForeignKeyTo(string referencedTableName, string referencedColumnName,
                string fkName = null, CascadeAction onDelete = CascadeAction.NoAction,
                CascadeAction onUpdate = CascadeAction.NoAction)
            {
                return ForeignKeyTo(DbObjectName.Parse(referencedTableName), referencedColumnName, fkName, onDelete,
                    onUpdate);
            }

            public ColumnExpression ForeignKeyTo(Table referencedTable, string referencedColumnName,
                string fkName = null, CascadeAction onDelete = CascadeAction.NoAction,
                CascadeAction onUpdate = CascadeAction.NoAction)
            {
                return ForeignKeyTo(referencedTable.Identifier, referencedColumnName, fkName, onDelete, onUpdate);
            }

            public ColumnExpression ForeignKeyTo(DbObjectName referencedIdentifier, string referencedColumnName,
                string fkName = null, CascadeAction onDelete = CascadeAction.NoAction,
                CascadeAction onUpdate = CascadeAction.NoAction)
            {
                var fk = new ForeignKey(fkName ?? _parent.Identifier.ToIndexName("fkey", Column.Name))
                {
                    LinkedTable = referencedIdentifier,
                    ColumnNames = new[] {Column.Name},
                    LinkedNames = new[] {referencedColumnName},
                    OnDelete = onDelete,
                    OnUpdate = onUpdate
                };

                _parent.ForeignKeys.Add(fk);

                return this;
            }

            /// <summary>
            ///     Marks this column as being part of the parent table's primary key
            /// </summary>
            /// <returns></returns>
            public ColumnExpression AsPrimaryKey()
            {
                Column.IsPrimaryKey = true;
                Column.AllowNulls = false;
                return this;
            }

            public ColumnExpression AllowNulls()
            {
                Column.AllowNulls = true;
                return this;
            }

            public ColumnExpression NotNull()
            {
                Column.AllowNulls = false;
                return this;
            }

            public ColumnExpression AddIndex(Action<IndexDefinition> configure = null)
            {
                var index = new IndexDefinition(_parent.Identifier.ToIndexName("idx", Column.Name))
                {
                    Columns = new[] {Column.Name}
                };

                _parent.Indexes.Add(index);

                configure?.Invoke(index);

                return this;
            }

            public ColumnExpression Serial()
            {
                Column.Type = "SERIAL";
                return this;
            }

            public ColumnExpression DefaultValueByString(string value)
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
                return DefaultValueByExpression(value.ToString(CultureInfo.InvariantCulture));
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
                Column.DefaultExpression = expression;

                return this;
            }

            public ColumnExpression PartitionByRange()
            {
                _parent.PartitionStrategy = PartitionStrategy.Range;
                _parent.PartitionExpressions.Add(Column.Name);

                Column.IsPrimaryKey = true;

                return this;
            }
        }
    }
}