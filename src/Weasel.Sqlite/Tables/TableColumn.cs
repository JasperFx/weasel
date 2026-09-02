using JasperFx.Core;
using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.Sqlite.Tables;

public class TableColumn: ITableColumn
{
    /// <param name="preserveCase">
    ///     Keep the caller's casing instead of folding to lowercase. SQLite identifiers are
    ///     case-insensitive so the folding is only cosmetic, but it was the one provider with no
    ///     way to turn it off, which meant a table reproducing a schema another tool created came
    ///     back reporting the wrong casing (weasel#458).
    /// </param>
    public TableColumn(string name, string type, bool preserveCase = false)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentOutOfRangeException(nameof(name));
        }

        if (string.IsNullOrEmpty(type))
        {
            throw new ArgumentOutOfRangeException(nameof(type));
        }

        // Undelimited first: a caller who wrote "order date" means that column, and the catalog
        // reports it back bare -- so that is the column that gets created, rather than order_date
        // (weasel#458).
        var unquoted = SchemaUtils.Unquote(name.Trim());

        Name = preserveCase ? unquoted : unquoted.ToLowerInvariant();
        Type = type.Trim();
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

    string? ITableColumn.ComputedExpression
    {
        get => GeneratedExpression;
        set
        {
            GeneratedExpression = value;
            GeneratedType ??= GeneratedColumnType.Virtual;
        }
    }

    bool ITableColumn.ComputedColumnIsStored
    {
        get => GeneratedType == GeneratedColumnType.Stored;
        set => GeneratedType = value ? GeneratedColumnType.Stored : GeneratedColumnType.Virtual;
    }

    /// <summary>
    /// For generated columns: STORED (materialized) or VIRTUAL (computed on read)
    /// </summary>
    public GeneratedColumnType? GeneratedType { get; set; }

    public string Name { get; }
    public string QuotedName => SchemaUtils.QuoteName(Name);

    /// <summary>
    ///     The type as it is written into DDL. A STRICT table accepts only INT, INTEGER, REAL,
    ///     TEXT, BLOB and ANY, so the declared type is mapped onto one of those there; everywhere
    ///     else SQLite stores the declared text verbatim and it is emitted as written.
    /// </summary>
    public string DdlType => Parent is { StrictTypes: true }
        ? SqliteProvider.Instance.ToStrictType(Type)
        : Type;

    /// <summary>
    ///     What a comparison reduces this column's type to. This has to agree with
    ///     <see cref="DdlType" /> on a STRICT table: the database holds the type that was emitted,
    ///     so comparing the model's declared type against it through a different normalization
    ///     reports drift on a column that already matches, and the resulting migration never
    ///     converges.
    /// </summary>
    private string ComparisonType => Parent is { StrictTypes: true }
        ? DdlType
        : SqliteProvider.Instance.ConvertSynonyms(RawType());

    public string RawType()
    {
        return Type.Split('(')[0].Trim();
    }

    public string Declaration() => Declaration(emitInlinePrimaryKey: true);

    /// <summary>
    /// Generate the column declaration. Pass <paramref name="emitInlinePrimaryKey"/> = false when
    /// the table is responsible for emitting a table-level <c>PRIMARY KEY (...)</c> constraint
    /// (e.g. composite primary keys on SQLite, where two inline <c>PRIMARY KEY</c> columns are
    /// rejected with <c>'table ... has more than one primary key'</c>).
    /// </summary>
    public string Declaration(bool emitInlinePrimaryKey)
    {
        var parts = new List<string>();

        // SQLite does not imply NOT NULL for a primary key: outside a WITHOUT ROWID table, only
        // INTEGER PRIMARY KEY is safe, and only because it is a rowid alias and a NULL is replaced
        // by the next rowid rather than stored. A REAL or TEXT primary key stores the NULL, so
        // suppressing NOT NULL here let a rebuilt table accept keys the original rejected.
        if (!AllowNulls)
        {
            parts.Add("NOT NULL");
        }

        // PRIMARY KEY with optional AUTOINCREMENT
        if (IsPrimaryKey && emitInlinePrimaryKey)
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
               string.Equals(ComparisonType, other.ComparisonType, StringComparison.OrdinalIgnoreCase);
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
            return (Name.ToLowerInvariant().GetHashCode() * 397) ^
                   ComparisonType.ToUpperInvariant().GetHashCode();
        }
    }

    public string ToDeclaration() => ToDeclaration(emitInlinePrimaryKey: true);

    public string ToDeclaration(bool emitInlinePrimaryKey)
    {
        var declaration = Declaration(emitInlinePrimaryKey);

        return declaration.IsEmpty()
            ? $"{QuotedName} {DdlType}"
            : $"{QuotedName} {DdlType} {declaration}";
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
