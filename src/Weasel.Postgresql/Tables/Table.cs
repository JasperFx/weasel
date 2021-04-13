using System;
using System.Collections.Generic;

namespace Weasel.Postgresql.Tables
{
    public class Table
    {
        public readonly List<TableColumn> Columns = new List<TableColumn>();
        
        public DbObjectName Identifier { get; }
        
        public Table(DbObjectName name)
        {
            Identifier = name ?? throw new ArgumentNullException(nameof(name));
        }

        public Table(string tableName) : this(new DbObjectName(tableName))
        {
            
        }

        public TableColumn AddColumn(string columnName, string columnType)
        {
            var column = new TableColumn(columnName, columnType);
            Columns.Add(column);

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