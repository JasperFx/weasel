using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Weasel.Core;
using Baseline;


namespace Weasel.SqlServer.Tests.Tables
{
    public class TableType : ISchemaObject
    {
        private readonly List<TableTypeColumn> _columns = new List<TableTypeColumn>();
        
        public DbObjectName Identifier { get; }

        public TableType(DbObjectName identifier)
        {
            Identifier = identifier;
        }

        public IReadOnlyList<TableTypeColumn> Columns => _columns;

        /// <summary>
        /// Add a column to the table type with the database type
        /// matching the .Net type referenced by T
        /// </summary>
        /// <param name="columnName"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public ITableTypeColumn AddColumn<T>(string columnName)
        {
            if (typeof(T).IsEnum)
            {
                throw new InvalidOperationException(
                    "Database column types cannot be automatically derived for enums. Explicitly specify as varchar or integer");
            }

            var type = SqlServerProvider.Instance.GetDatabaseType(typeof(T), EnumStorage.AsInteger);
            return AddColumn(columnName, type);
        }

        public ITableTypeColumn AddColumn(string columnName, string databaseType)
        {
            var column = new TableTypeColumn(columnName, databaseType);
            _columns.Add(column);

            return column;
        }

        public void WriteCreateStatement(DdlRules rules, TextWriter writer)
        {
            writer.Write($"CREATE TYPE {Identifier.QualifiedName} AS TABLE (");
            
            writer.Write(_columns.Select(x => x.Declaration()).Join(", "));
            writer.WriteLine(")");
        }

        public void WriteDropStatement(DdlRules rules, TextWriter writer)
        {
            writer.WriteLine($"drop type {Identifier};");
        }

        public void ConfigureQueryCommand(CommandBuilder builder)
        {
            builder.Append($@"
select sys.columns.name, sys.types.name, sys.columns.is_nullable
from 
    sys.table_types inner join sys.columns on sys.table_types.type_table_object_id = sys.columns.object_id
    inner join sys.types on sys.columns.system_type_id = sys.types.system_type_id
    inner join sys.schemas on sys.table_types.schema_id = sys.schemas.schema_id
where
    sys.table_types.name = '{Identifier.Name}' and sys.schemas.name = '{Identifier.Schema}'
order by
    sys.columns.column_id
");
        }

        public async Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
        {
            var existing = await readExisting(reader);
            return new TableTypeDelta(this, existing);
        }

        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;
        }

        public interface ITableTypeColumn
        {
            void AllowNulls();
            void NotNull();
        }
        
        public class TableTypeColumn : ITableTypeColumn
        {
            public string Name { get; }
            public string DatabaseType { get; set; }
            
            public bool AllowNulls { get; set; }

            public TableTypeColumn(string name, string databaseType)
            {
                Name = name;
                DatabaseType = databaseType;
            }


            void ITableTypeColumn.AllowNulls()
            {
                AllowNulls = true;
            }

            public void NotNull()
            {
                AllowNulls = false;
            }

            public string Declaration()
            {
                var nullability = AllowNulls ? "NULL" : "NOT NULL";
                return $"{Name} {DatabaseType} {nullability}";
            }

            protected bool Equals(TableTypeColumn other)
            {
                return Name == other.Name && DatabaseType == other.DatabaseType && AllowNulls == other.AllowNulls;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                {
                    return false;
                }

                if (ReferenceEquals(this, obj))
                {
                    return true;
                }

                if (obj.GetType() != this.GetType())
                {
                    return false;
                }

                return Equals((TableTypeColumn) obj);
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(Name, DatabaseType, AllowNulls);
            }
        }
        
        public async Task<TableType> FetchExisting(SqlConnection conn)
        {
            var builder = new CommandBuilder();

            ConfigureQueryCommand(builder);

            using var reader = await builder.ExecuteReaderAsync(conn);
            return await readExisting(reader);
        }

        public async Task<TableTypeDelta> FindDelta(SqlConnection conn)
        {
            var actual = await FetchExisting(conn);
            return new TableTypeDelta(this, actual);
        }

        private async Task<TableType> readExisting(DbDataReader reader)
        {
            var existing = new TableType(Identifier);
            while (await reader.ReadAsync())
            {
                var column = new TableTypeColumn(await reader.GetFieldValueAsync<string>(0),
                    await reader.GetFieldValueAsync<string>(1));
                column.AllowNulls = await reader.GetFieldValueAsync<bool>(2);
                
                existing._columns.Add(column);
            }

            return existing._columns.Any() ? existing : null;
        }
        
        
    }
}