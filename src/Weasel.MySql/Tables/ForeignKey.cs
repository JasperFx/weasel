using System.Text;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.MySql.Tables;

public class ForeignKey: INamed
{
    private readonly List<string> _columnNames = new();
    private readonly List<string> _linkedNames = new();

    public ForeignKey(string name)
    {
        Name = name;
    }

    public string Name { get; set; }
    public DbObjectName? LinkedTable { get; set; }
    public CascadeAction OnDelete { get; set; } = CascadeAction.NoAction;
    public CascadeAction OnUpdate { get; set; } = CascadeAction.NoAction;

    public string[] ColumnNames
    {
        get => _columnNames.ToArray();
        set
        {
            _columnNames.Clear();
            _columnNames.AddRange(value);
        }
    }

    public string[] LinkedNames
    {
        get => _linkedNames.ToArray();
        set
        {
            _linkedNames.Clear();
            _linkedNames.AddRange(value);
        }
    }

    public void LinkColumns(string columnName, string linkedName)
    {
        _columnNames.Add(columnName);
        _linkedNames.Add(linkedName);
    }

    public string ToDDL(Table parent)
    {
        if (LinkedTable == null)
        {
            throw new InvalidOperationException("LinkedTable must be set before generating DDL");
        }

        var builder = new StringBuilder();
        builder.Append($"ALTER TABLE {parent.Identifier.QualifiedName} ADD CONSTRAINT `{Name}` ");
        builder.Append($"FOREIGN KEY ({_columnNames.Select(c => $"`{c}`").Join(", ")}) ");
        builder.Append($"REFERENCES {LinkedTable.QualifiedName} ({_linkedNames.Select(c => $"`{c}`").Join(", ")})");

        if (OnDelete != CascadeAction.NoAction)
        {
            builder.Append($" ON DELETE {GetCascadeActionSql(OnDelete)}");
        }

        if (OnUpdate != CascadeAction.NoAction)
        {
            builder.Append($" ON UPDATE {GetCascadeActionSql(OnUpdate)}");
        }

        builder.Append(";");

        return builder.ToString();
    }

    private static string GetCascadeActionSql(CascadeAction action)
    {
        return action switch
        {
            CascadeAction.Cascade => "CASCADE",
            CascadeAction.SetNull => "SET NULL",
            CascadeAction.SetDefault => "SET DEFAULT",
            CascadeAction.Restrict => "RESTRICT",
            _ => "NO ACTION"
        };
    }

    public void ReadReferentialActions(string onDelete, string onUpdate)
    {
        OnDelete = MySqlProvider.ReadAction(onDelete);
        OnUpdate = MySqlProvider.ReadAction(onUpdate);
    }

    public bool IsEquivalentTo(ForeignKey other)
    {
        if (!Name.Equals(other.Name, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (LinkedTable?.QualifiedName != other.LinkedTable?.QualifiedName)
        {
            return false;
        }

        if (!_columnNames.SequenceEqual(other._columnNames, StringComparer.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!_linkedNames.SequenceEqual(other._linkedNames, StringComparer.OrdinalIgnoreCase))
        {
            return false;
        }

        if (OnDelete != other.OnDelete || OnUpdate != other.OnUpdate)
        {
            return false;
        }

        return true;
    }
}
