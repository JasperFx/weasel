using System.Data.Common;
using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.SqlServer.Tables;

public class TableType: ISchemaObject
{
    private readonly List<TableTypeColumn> _columns = new();

    public TableType(DbObjectName identifier)
    {
        Identifier = identifier;
    }

    public IReadOnlyList<TableTypeColumn> Columns => _columns;

    public DbObjectName Identifier { get; }

    public void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        writer.Write($"CREATE TYPE {Identifier.QualifiedName} AS TABLE (");

        writer.Write(_columns.Select(x => x.Declaration()).Join(", "));
        writer.WriteLine(")");
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"drop type {Identifier};");
    }

    public void ConfigureQueryCommand(DbCommandBuilder builder)
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

    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readExistingAsync(reader, ct).ConfigureAwait(false);
        return new TableTypeDelta(this, existing);
    }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;
    }

    /// <summary>
    ///     Add a column to the table type with the database type
    ///     matching the .Net type referenced by T
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

    public async Task<TableType?> FetchExistingAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);

        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var result = await readExistingAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return result;
    }

    public async Task<TableTypeDelta> FindDeltaAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var actual = await FetchExistingAsync(conn, ct).ConfigureAwait(false);
        return new TableTypeDelta(this, actual);
    }

    private async Task<TableType?> readExistingAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = new TableType(Identifier);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var column = new TableTypeColumn(await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false),
                await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false));
            column.AllowNulls = await reader.GetFieldValueAsync<bool>(2, ct).ConfigureAwait(false);

            existing._columns.Add(column);
        }

        return existing._columns.Any() ? existing : null;
    }

    public interface ITableTypeColumn
    {
        void AllowNulls();
        void NotNull();
    }

    public class TableTypeColumn: ITableTypeColumn
    {
        public TableTypeColumn(string name, string databaseType)
        {
            Name = name;
            DatabaseType = databaseType;
        }

        public string Name { get; }
        public string DatabaseType { get; set; }

        public bool AllowNulls { get; set; }


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
            return Name.EqualsIgnoreCase(other.Name) && DatabaseType.EqualsIgnoreCase(other.DatabaseType) &&
                   AllowNulls == other.AllowNulls;
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != GetType())
            {
                return false;
            }

            return Equals((TableTypeColumn)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Name, DatabaseType, AllowNulls);
        }
    }
}
