using System.Text;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.MySql.Tables;

public class ForeignKey: ForeignKeyBase
{
    private readonly List<string> _columnNames = new();
    private readonly List<string> _linkedNames = new();

    public ForeignKey(string name) : base(name)
    {
    }

    public override string[] ColumnNames
    {
        get => _columnNames.ToArray();
        set
        {
            _columnNames.Clear();
            _columnNames.AddRange(value);
        }
    }

    public override string[] LinkedNames
    {
        get => _linkedNames.ToArray();
        set
        {
            _linkedNames.Clear();
            _linkedNames.AddRange(value);
        }
    }

#pragma warning disable CS0618 // Type or member is obsolete
    /// <summary>
    /// The cascade action to take when a referenced row is deleted
    /// </summary>
    public CascadeAction OnDelete
    {
        get => ToLocalCascadeAction(DeleteAction);
        set => DeleteAction = ToCoreAction(value);
    }

    /// <summary>
    /// The cascade action to take when a referenced row is updated
    /// </summary>
    public CascadeAction OnUpdate
    {
        get => ToLocalCascadeAction(UpdateAction);
        set => UpdateAction = ToCoreAction(value);
    }

    private static Core.CascadeAction ToCoreAction(CascadeAction action)
    {
        return action switch
        {
            CascadeAction.NoAction => Core.CascadeAction.NoAction,
            CascadeAction.Restrict => Core.CascadeAction.Restrict,
            CascadeAction.Cascade => Core.CascadeAction.Cascade,
            CascadeAction.SetNull => Core.CascadeAction.SetNull,
            CascadeAction.SetDefault => Core.CascadeAction.SetDefault,
            _ => Core.CascadeAction.NoAction
        };
    }

    private static CascadeAction ToLocalCascadeAction(Core.CascadeAction action)
    {
        return action switch
        {
            Core.CascadeAction.NoAction => CascadeAction.NoAction,
            Core.CascadeAction.Restrict => CascadeAction.Restrict,
            Core.CascadeAction.Cascade => CascadeAction.Cascade,
            Core.CascadeAction.SetNull => CascadeAction.SetNull,
            Core.CascadeAction.SetDefault => CascadeAction.SetDefault,
            _ => CascadeAction.NoAction
        };
    }
#pragma warning restore CS0618 // Type or member is obsolete

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
