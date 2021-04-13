using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;

namespace Weasel.Postgresql.Tables
{
    public class Table : ISchemaObject
    {
        private readonly List<TableColumn> _columns = new List<TableColumn>();

        public IReadOnlyList<TableColumn> Columns => _columns;

        public void Write(DdlRules rules, StringWriter writer)
        {
            throw new NotImplementedException();
        }

        public void WriteDropStatement(DdlRules rules, StringWriter writer)
        {
            throw new NotImplementedException();
        }

        public DbObjectName Identifier { get; }
        public void ConfigureQueryCommand(CommandBuilder builder)
        {
            throw new NotImplementedException();
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

        public void AddColumn(TableColumn column)
        {
            _columns.Add(column);
            column.Parent = this;
        }

        public TableColumn AddColumn(string columnName, string columnType)
        {
            var column = new TableColumn(columnName, columnType) {Parent = this};
            _columns.Add(column);

            return column;
        }

        public TableColumn AddColumn<T>(string columnName)
        {
            if (typeof(T).IsEnum)
            {
                throw new InvalidOperationException(
                    "Database column types cannot be automatically derived for enums. Explicitly specify as varchar or integer");
            }

            var type = TypeMappings.GetPgType(typeof(T), EnumStorage.AsInteger);
            return AddColumn(columnName, type);
        }
    }
}