using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Npgsql;

namespace Weasel.Postgresql.Tables
{
    public class Table : ISchemaObject
    {
        private readonly List<TableColumn> _columns = new List<TableColumn>();

        public IReadOnlyList<TableColumn> Columns => _columns;

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

            // foreach (var foreignKey in ForeignKeys)
            // {
            //     writer.WriteLine();
            //     writer.WriteLine(foreignKey.ToDDL());
            // }
            //
            // foreach (var index in Indexes)
            // {
            //     writer.WriteLine();
            //     writer.WriteLine(index.ToDDL());
            // }
        }

        public void WriteDropStatement(DdlRules rules, StringWriter writer)
        {
            writer.WriteLine($"DROP TABLE IF EXISTS {Identifier} CASCADE;");
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
    }
}