using JasperFx.Core;
using JasperFx.Core.Reflection;

namespace Weasel.SqlServer.Tables;

public interface INamed
{
    string Name { get; }
}

public class TableColumn: INamed
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

        Name = name.ToLower().Trim().Replace(' ', '_');
        Type = type.ToLower();
    }


    public IList<ColumnCheck> ColumnChecks { get; } = new List<ColumnCheck>();

    public bool AllowNulls { get; set; } = true;

    public string? DefaultExpression { get; set; }


    public string Type { get; set; }
    public Table Parent { get; internal set; } = null!;

    public bool IsPrimaryKey { get; internal set; }
    public bool IsAutoNumber { get; set; }

    public string Name { get; }
    public string QuotedName => SchemaUtils.QuoteName(Name);

    public string RawType()
    {
        return Type.Split('(')[0].Trim();
    }

    public string Declaration()
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

    protected bool Equals(TableColumn other)
    {
        return string.Equals(Name, other.Name) &&
               string.Equals(SqlServerProvider.Instance.ConvertSynonyms(RawType()),
                   SqlServerProvider.Instance.ConvertSynonyms(other.RawType()));
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
            return (Name.GetHashCode() * 397) ^ Type.GetHashCode();
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


    public virtual string AlterColumnTypeSql(Table table, TableColumn changeActual)
    {
        return $"alter table {table.Identifier} modify column {Name.PadRight(Name.Length)} type {Type};";
    }

    public string DropColumnSql(Table table)
    {
        return $"alter table {table.Identifier} drop column {Name};";
    }


    public virtual bool CanAdd()
    {
        return AllowNulls || DefaultExpression.IsNotEmpty();
    }

    public virtual string AddColumnSql(Table parent)
    {
        return $"alter table {parent.Identifier} add {ToDeclaration()};";
    }


    public virtual bool CanAlter(TableColumn actual)
    {
        // TODO -- need this to be more systematic
        return true;
    }
}

public abstract class ColumnCheck
{
    /// <summary>
    ///     The database name for the check. This can be null
    /// </summary>
    public string? Name { get; set; } // TODO -- validate good name

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

public class SerialValue: ColumnCheck
{
    public override string Declaration()
    {
        return "SERIAL";
    }
}

/*

public class GeneratedAlwaysAsStored : ColumnCheck
{
    // GENERATED ALWAYS AS ( generation_expr ) STORED
}

public class GeneratedAsIdentity : ColumnCheck
{
    // GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( sequence_options ) ]
}

*/
