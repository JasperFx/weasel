using JasperFx.Core;
using Weasel.Core.Tables;

namespace Weasel.SqlServer.Tables;

/// <summary>
/// SQL Server implementation of INamed interface
/// </summary>
public interface INamed : Weasel.Core.Tables.INamed
{
}

/// <summary>
/// SQL Server-specific table column implementation
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
    /// Indicates whether this column is an auto-incrementing identity column
    /// </summary>
    public bool IsAutoNumber { get; set; }

    /// <summary>
    /// The quoted column name using SQL Server quoting rules
    /// </summary>
    public override string QuotedName => SchemaUtils.QuoteName(Name);

    /// <summary>
    /// Generates the NULL/NOT NULL, IDENTITY, and DEFAULT clause for this column
    /// </summary>
    public override string Declaration()
    {
        var declaration = !IsPrimaryKey && AllowNulls ? "NULL" : "NOT NULL";
        if (IsAutoNumber)
        {
            declaration += " IDENTITY";
        }

        if (DefaultExpression.IsNotEmpty())
        {
            declaration += " DEFAULT " + DefaultExpression;
        }

        return $"{declaration} {ColumnChecks.Select(x => x.FullDeclaration()).Join(" ")}".TrimEnd();
    }

    protected override bool EqualsColumn(TableColumnBase<ColumnCheck> other)
    {
        return string.Equals(QuotedName, ((TableColumn)other).QuotedName) &&
               string.Equals(SqlServerProvider.Instance.ConvertSynonyms(RawType()),
                   SqlServerProvider.Instance.ConvertSynonyms(other.RawType()));
    }

    public override string AlterColumnTypeSql(Core.DbObjectName tableIdentifier, TableColumnBase<ColumnCheck> changeActual)
    {
        return $"alter table {tableIdentifier} alter column {((TableColumn)changeActual).ToDeclaration()};";
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
        return $"alter table {tableIdentifier} add {ToDeclaration()};";
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
}

/// <summary>
/// Abstract base class for SQL Server column check constraints
/// </summary>
public abstract class ColumnCheck : ColumnCheckBase
{
}

/// <summary>
/// SERIAL column constraint for SQL Server (for compatibility)
/// </summary>
public class SerialValue : ColumnCheck
{
    public override string Declaration()
    {
        return "SERIAL";
    }
}
