using JasperFx.Core;
using JasperFx.Core.Reflection;

namespace Weasel.Core.Tables;

/// <summary>
/// Abstract base class for table columns that defines common properties and behavior
/// shared between PostgreSQL and SQL Server implementations.
/// </summary>
/// <typeparam name="TColumnCheck">The type of column check constraint</typeparam>
public abstract class TableColumnBase<TColumnCheck> : INamed
    where TColumnCheck : ColumnCheckBase
{
    protected TableColumnBase(string name, string type)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentOutOfRangeException(nameof(name));
        }

        if (string.IsNullOrEmpty(type))
        {
            throw new ArgumentOutOfRangeException(nameof(type));
        }

        Name = name.ToLower().Trim().Replace(' ', '_');
        Type = type.ToLower();
    }

    /// <summary>
    /// Collection of check constraints applied to this column
    /// </summary>
    public IList<TColumnCheck> ColumnChecks { get; } = new List<TColumnCheck>();

    /// <summary>
    /// Indicates whether the column allows NULL values. Default is true.
    /// </summary>
    public bool AllowNulls { get; set; } = true;

    /// <summary>
    /// The DEFAULT expression for this column
    /// </summary>
    public string? DefaultExpression { get; set; }

    /// <summary>
    /// The SQL data type of the column
    /// </summary>
    public string Type { get; set; }

    /// <summary>
    /// Indicates whether this column is part of the primary key
    /// </summary>
    public bool IsPrimaryKey { get; set; }

    /// <summary>
    /// The normalized column name
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// The quoted column name (database-specific quoting)
    /// </summary>
    public abstract string QuotedName { get; }

    /// <summary>
    /// Extracts the raw type name without any parameters (e.g., "varchar" from "varchar(255)")
    /// </summary>
    public string RawType()
    {
        return Type.Split('(')[0].Trim();
    }

    /// <summary>
    /// Generates the NULL/NOT NULL and DEFAULT clause for this column
    /// </summary>
    public virtual string Declaration()
    {
        var declaration = !IsPrimaryKey && AllowNulls ? "NULL" : "NOT NULL";
        if (DefaultExpression.IsNotEmpty())
        {
            declaration += " DEFAULT " + DefaultExpression;
        }

        return $"{declaration} {ColumnChecks.Select(x => x.FullDeclaration()).Join(" ")}".TrimEnd();
    }

    /// <summary>
    /// Compares this column to another for equality based on name and type
    /// </summary>
    protected abstract bool EqualsColumn(TableColumnBase<TColumnCheck> other);

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

        if (!obj.GetType().CanBeCastTo(GetType()))
        {
            return false;
        }

        return EqualsColumn((TableColumnBase<TColumnCheck>)obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            return (Name.GetHashCode() * 397) ^ Type.GetHashCode();
        }
    }

    /// <summary>
    /// Generates the complete column declaration (name, type, constraints)
    /// </summary>
    public string ToDeclaration()
    {
        var declaration = Declaration();

        return declaration.IsEmpty()
            ? $"{QuotedName} {Type}"
            : $"{QuotedName} {Type} {declaration}";
    }

    public override string ToString()
    {
        return ToDeclaration();
    }

    /// <summary>
    /// Generates the SQL to alter this column's type
    /// </summary>
    public abstract string AlterColumnTypeSql(DbObjectName tableIdentifier, TableColumnBase<TColumnCheck> changeActual);

    /// <summary>
    /// Generates the SQL to drop this column from a table
    /// </summary>
    public virtual string DropColumnSql(DbObjectName tableIdentifier)
    {
        return $"alter table {tableIdentifier} drop column {QuotedName};";
    }

    /// <summary>
    /// Indicates whether this column can be added to an existing table
    /// (true if nullable or has a default value)
    /// </summary>
    public virtual bool CanAdd()
    {
        return AllowNulls || DefaultExpression.IsNotEmpty();
    }

    /// <summary>
    /// Generates the SQL to add this column to an existing table
    /// </summary>
    public abstract string AddColumnSql(DbObjectName tableIdentifier);

    /// <summary>
    /// Indicates whether this column can be altered to match another column
    /// </summary>
    public virtual bool CanAlter(TableColumnBase<TColumnCheck> actual)
    {
        // TODO -- need this to be more systematic
        return true;
    }
}
