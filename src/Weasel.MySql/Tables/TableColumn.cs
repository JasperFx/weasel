using JasperFx.Core;
using Weasel.Core;

namespace Weasel.MySql.Tables;

public class TableColumn: ITableColumn
{
    public TableColumn(string name, string type)
    {
        Name = name;
        Type = type;
    }

    public string Name { get; }
    public string Type { get; set; }
    public bool AllowNulls { get; set; } = true;
    public bool IsPrimaryKey { get; set; }
    public bool IsAutoNumber { get; set; }
    public string? DefaultExpression { get; set; }
    public Table? Parent { get; set; }

    public string QuotedName => $"`{Name}`";

    public string RawType()
    {
        var type = Type.ToUpperInvariant();

        // Remove size specifications for comparison
        var parenIndex = type.IndexOf('(');
        if (parenIndex > 0)
        {
            type = type.Substring(0, parenIndex);
        }

        return type;
    }

    public string ToDeclaration()
    {
        return $"{QuotedName} {Type} {Declaration()}".TrimEnd();
    }

    public string Declaration()
    {
        var parts = new List<string>();

        if (!AllowNulls || IsPrimaryKey)
        {
            parts.Add("NOT NULL");
        }
        else
        {
            parts.Add("NULL");
        }

        if (IsAutoNumber)
        {
            parts.Add("AUTO_INCREMENT");
        }

        if (DefaultExpression.IsNotEmpty())
        {
            parts.Add($"DEFAULT {DefaultExpression}");
        }

        return parts.Join(" ");
    }

    public bool IsEquivalentTo(TableColumn other)
    {
        if (!Name.Equals(other.Name, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (RawType() != other.RawType())
        {
            return false;
        }

        // Compare nullability only for non-primary key columns
        if (!IsPrimaryKey && !other.IsPrimaryKey)
        {
            if (AllowNulls != other.AllowNulls)
            {
                return false;
            }
        }

        return true;
    }

    public override string ToString()
    {
        return $"{Name}: {Type}";
    }

    public override bool Equals(object? obj)
    {
        if (obj is TableColumn other)
        {
            return IsEquivalentTo(other);
        }

        return false;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Name.ToUpperInvariant(), RawType());
    }
}
