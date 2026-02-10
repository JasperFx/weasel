using JasperFx.Core;
using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.Sqlite.Tables;

public class TableColumn: ITableColumn
{
    public TableColumn(string name, string type)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentOutOfRangeException(nameof(name));
        }

        if (string.IsNullOrEmpty(type))
        {
            throw new ArgumentOutOfRangeException(nameof(type));
        }

        // SQLite is case-insensitive, normalize names to lowercase
        Name = name.ToLowerInvariant().Trim().Replace(' ', '_');
        // Normalize type using provider
        Type = SqliteProvider.Instance.ConvertSynonyms(type);
    }

    public IList<ColumnCheck> ColumnChecks { get; } = new List<ColumnCheck>();

    public bool AllowNulls { get; set; } = true;

    public string? DefaultExpression { get; set; }

    public string Type { get; set; }
    public Table Parent { get; internal set; } = null!;

    public bool IsPrimaryKey { get; internal set; }

    /// <summary>
    /// When true, uses AUTOINCREMENT keyword with INTEGER PRIMARY KEY.
    /// Note: INTEGER PRIMARY KEY is an alias for the rowid, AUTOINCREMENT prevents reuse.
    /// </summary>
    public bool IsAutoNumber { get; set; }

    /// <summary>
    /// Expression for a generated column (GENERATED ALWAYS AS ...)
    /// </summary>
    public string? GeneratedExpression { get; set; }

    /// <summary>
    /// For generated columns: STORED (materialized) or VIRTUAL (computed on read)
    /// </summary>
    public GeneratedColumnType? GeneratedType { get; set; }

    public string Name { get; }
    public string QuotedName => SchemaUtils.QuoteName(Name);

    public string RawType()
    {
        return Type.Split('(')[0].Trim();
    }

    public string Declaration()
    {
        var parts = new List<string>();

        // NULL/NOT NULL constraint
        if (!IsPrimaryKey && !AllowNulls)
        {
            parts.Add("NOT NULL");
        }

        // PRIMARY KEY with optional AUTOINCREMENT
        if (IsPrimaryKey)
        {
            if (IsAutoNumber)
            {
                parts.Add("PRIMARY KEY AUTOINCREMENT");
            }
            else
            {
                parts.Add("PRIMARY KEY");
            }
        }

        // DEFAULT expression
        if (DefaultExpression.IsNotEmpty())
        {
            parts.Add($"DEFAULT {DefaultExpression}");
        }

        // GENERATED column
        if (GeneratedExpression.IsNotEmpty() && GeneratedType.HasValue)
        {
            var genType = GeneratedType.Value == GeneratedColumnType.Stored ? "STORED" : "VIRTUAL";
            parts.Add($"GENERATED ALWAYS AS ({GeneratedExpression}) {genType}");
        }

        // Column checks
        if (ColumnChecks.Any())
        {
            parts.Add(ColumnChecks.Select(x => x.FullDeclaration()).Join(" "));
        }

        return parts.Join(" ").TrimEnd();
    }

    protected bool Equals(TableColumn other)
    {
        return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase) &&
               string.Equals(SqliteProvider.Instance.ConvertSynonyms(RawType()),
                   SqliteProvider.Instance.ConvertSynonyms(other.RawType()), StringComparison.OrdinalIgnoreCase);
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

        if (!obj.GetType().CanBeCastTo<TableColumn>())
        {
            return false;
        }

        return Equals((TableColumn)obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            // SQLite is case-insensitive
            return (Name.ToLowerInvariant().GetHashCode() * 397) ^ Type.ToUpperInvariant().GetHashCode();
        }
    }

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
    /// SQLite has limited ALTER TABLE support. Most column modifications require table recreation.
    /// </summary>
    public virtual string AlterColumnTypeSql(Table table, TableColumn changeActual)
    {
        // SQLite 3.35+ supports ALTER TABLE ... ALTER COLUMN but it's limited
        // For now, indicate that table recreation is needed
        throw new NotSupportedException(
            $"SQLite does not support altering column type directly. Table '{table.Identifier}' must be recreated to change column '{QuotedName}'.");
    }

    /// <summary>
    /// SQLite 3.35+ supports DROP COLUMN
    /// </summary>
    public string DropColumnSql(Table table)
    {
        return $"ALTER TABLE {table.Identifier} DROP COLUMN {QuotedName};";
    }

    public virtual bool CanAdd()
    {
        // SQLite allows adding columns if they are nullable or have a default value
        return AllowNulls || DefaultExpression.IsNotEmpty();
    }

    /// <summary>
    /// SQLite supports ADD COLUMN (oldest ALTER TABLE command)
    /// </summary>
    public virtual string AddColumnSql(Table parent)
    {
        return $"ALTER TABLE {parent.Identifier} ADD COLUMN {ToDeclaration()};";
    }

    public virtual bool CanAlter(TableColumn actual)
    {
        // SQLite has very limited ALTER COLUMN support
        // Most changes require table recreation
        return false;
    }

    /// <summary>
    /// SQLite 3.25+ supports ALTER TABLE RENAME COLUMN
    /// </summary>
    public string RenameColumnSql(Table table, string oldName)
    {
        return $"ALTER TABLE {table.Identifier} RENAME COLUMN {SchemaUtils.QuoteName(oldName)} TO {QuotedName};";
    }

    /// <summary>
    /// Checks if this column has the same type and constraints as another column (ignoring name).
    /// Used for rename detection heuristics.
    /// </summary>
    public bool IsStructuralMatch(TableColumn other)
    {
        return string.Equals(
                   SqliteProvider.Instance.ConvertSynonyms(RawType()),
                   SqliteProvider.Instance.ConvertSynonyms(other.RawType()),
                   StringComparison.OrdinalIgnoreCase) &&
               AllowNulls == other.AllowNulls &&
               IsPrimaryKey == other.IsPrimaryKey &&
               IsAutoNumber == other.IsAutoNumber;
    }
}

public enum GeneratedColumnType
{
    /// <summary>
    /// VIRTUAL: Computed on read (not stored)
    /// </summary>
    Virtual,

    /// <summary>
    /// STORED: Materialized in the table
    /// </summary>
    Stored
}

public abstract class ColumnCheck
{
    /// <summary>
    ///     The database name for the check. This can be null
    /// </summary>
    public string? Name { get; set; }

    public abstract string Declaration();

    public string FullDeclaration()
    {
        if (Name.IsEmpty())
        {
            return Declaration();
        }

        return $"CONSTRAINT {Name} {Declaration()}";
    }
}

/// <summary>
/// Auto-increment column using INTEGER PRIMARY KEY AUTOINCREMENT
/// </summary>
public class AutoIncrementValue: ColumnCheck
{
    public override string Declaration()
    {
        return "PRIMARY KEY AUTOINCREMENT";
    }
}
