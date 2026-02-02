using JasperFx.Core;
using Weasel.Core.Tables;

namespace Weasel.Postgresql.Tables;

/// <summary>
/// PostgreSQL implementation of INamed interface
/// </summary>
public interface INamed : Weasel.Core.Tables.INamed
{
}

/// <summary>
/// PostgreSQL-specific table column implementation
/// </summary>
public class TableColumn : TableColumnBase<ColumnCheck>, INamed
{
    public TableColumn(string name, string type) : base(name, type)
    {
    }

    /// <summary>
    /// Reference to the parent table
    /// </summary>
    public Table Parent { get; internal set; } = null!;

    /// <summary>
    /// The quoted column name using PostgreSQL quoting rules
    /// </summary>
    public override string QuotedName => SchemaUtils.QuoteName(Name);

    protected override bool EqualsColumn(TableColumnBase<ColumnCheck> other)
    {
        return string.Equals(QuotedName, ((TableColumn)other).QuotedName) &&
               string.Equals(PostgresqlProvider.Instance.ConvertSynonyms(RawType()),
                   PostgresqlProvider.Instance.ConvertSynonyms(other.RawType()));
    }

    public override string AlterColumnTypeSql(Core.DbObjectName tableIdentifier, TableColumnBase<ColumnCheck> changeActual)
    {
        return $"alter table {tableIdentifier} alter column {QuotedName.PadRight(QuotedName.Length)} type {Type};";
    }

    /// <summary>
    /// Generates the SQL to alter this column's type (convenience overload for Table)
    /// </summary>
    public virtual string AlterColumnTypeSql(Table table, TableColumn changeActual)
    {
        return AlterColumnTypeSql(table.Identifier, changeActual);
    }

    public override string AddColumnSql(Core.DbObjectName tableIdentifier)
    {
        return $"alter table {tableIdentifier} add column {ToDeclaration()};";
    }

    /// <summary>
    /// Generates the SQL to add this column to an existing table (convenience overload for Table)
    /// </summary>
    public string AddColumnSql(Table table)
    {
        return AddColumnSql(table.Identifier);
    }

    /// <summary>
    /// Generates the SQL to drop this column from a table (convenience overload for Table)
    /// </summary>
    public string DropColumnSql(Table table)
    {
        return DropColumnSql(table.Identifier);
    }

    /// <summary>
    /// Generates a PL/pgSQL function argument declaration for this column
    /// </summary>
    public virtual string ToFunctionArgumentDeclaration()
    {
        if (Type.StartsWith("varchar", StringComparison.OrdinalIgnoreCase))
        {
            return $"{ToArgumentName()} text";
        }

        if (Type.StartsWith("nvarchar", StringComparison.OrdinalIgnoreCase))
        {
            return $"{ToArgumentName()} nvarchar";
        }

        return $"{ToArgumentName()} {Type}";
    }

    /// <summary>
    /// Gets the argument name for use in PL/pgSQL functions
    /// </summary>
    public string ToArgumentName()
    {
        return "p_" + Name;
    }

    /// <summary>
    /// Generates an update expression for use in PL/pgSQL functions
    /// </summary>
    public string ToFunctionUpdate()
    {
        return $"{Name} = {ToArgumentName()}";
    }
}

/// <summary>
/// Abstract base class for PostgreSQL column check constraints
/// </summary>
public abstract class ColumnCheck : ColumnCheckBase
{
}

/// <summary>
/// SERIAL column constraint for PostgreSQL
/// </summary>
public class SerialValue : ColumnCheck
{
    public override string Declaration() => "SERIAL";
}

/// <summary>
/// BIGSERIAL column constraint for PostgreSQL
/// </summary>
public class BigSerialValue : ColumnCheck
{
    public override string Declaration() => "BIGSERIAL";
}

/// <summary>
/// SMALLSERIAL column constraint for PostgreSQL
/// </summary>
public class SmallSerialValue : ColumnCheck
{
    public override string Declaration() => "SMALLSERIAL";
}
