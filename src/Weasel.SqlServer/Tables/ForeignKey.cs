using JasperFx.Core;
using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.SqlServer.Tables;

public class MisconfiguredForeignKeyException: Exception
{
    public MisconfiguredForeignKeyException(string? message): base(message)
    {
    }
}

public class ForeignKey: INamed
{
    private string[] _columnNames = null!;
    private string[] _linkedNames = null!;

    public ForeignKey(string name)
    {
        Name = name;
    }

    public string[] ColumnNames
    {
        get => _columnNames;
        set => _columnNames = value.OrderBy(x => x).ToArray();
    }

    public string[] LinkedNames
    {
        get => _linkedNames;
        set => _linkedNames = value.OrderBy(x => x).ToArray();
    }

    public DbObjectName LinkedTable { get; set; } = null!;

    public CascadeAction OnDelete { get; set; } = CascadeAction.NoAction;
    public CascadeAction OnUpdate { get; set; } = CascadeAction.NoAction;

    public string Name { get; set; }

    protected bool Equals(ForeignKey other)
    {
        return Name == other.Name && ColumnNames.SequenceEqual(other.ColumnNames) &&
               LinkedNames.SequenceEqual(other.LinkedNames) && Equals(LinkedTable, other.LinkedTable) &&
               OnDelete == other.OnDelete && OnUpdate == other.OnUpdate;
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

        if (!obj.GetType().CanBeCastTo<ForeignKey>())
        {
            return false;
        }

        return Equals((ForeignKey)obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = Name != null ? Name.GetHashCode() : 0;
            hashCode = (hashCode * 397) ^ (ColumnNames != null ? ColumnNames.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (LinkedNames != null ? LinkedNames.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (LinkedTable != null ? LinkedTable.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (int)OnDelete;
            hashCode = (hashCode * 397) ^ (int)OnUpdate;
            return hashCode;
        }
    }

    /// <summary>
    ///     Read the DDL definition from the server
    /// </summary>
    /// <param name="definition"></param>
    /// <exception cref="NotImplementedException"></exception>
    public void Parse(string definition)
    {
        var open1 = definition.IndexOf('(');
        var closed1 = definition.IndexOf(')');

        ColumnNames = definition.Substring(open1 + 1, closed1 - open1 - 1).ToDelimitedArray(',');

        var open2 = definition.IndexOf('(', closed1);
        var closed2 = definition.IndexOf(')', open2);

        LinkedNames = definition.Substring(open2 + 1, closed2 - open2 - 1).ToDelimitedArray(',');


        var references = "REFERENCES";
        var tableStart = definition.IndexOf(references) + references.Length;

        var tableName = definition.Substring(tableStart, open2 - tableStart).Trim();
        LinkedTable = DbObjectName.Parse(SqlServerProvider.Instance, tableName);

        if (definition.ContainsIgnoreCase("ON DELETE CASCADE"))
        {
            OnDelete = CascadeAction.Cascade;
        }
        else if (definition.ContainsIgnoreCase("ON DELETE SET NULL"))
        {
            OnDelete = CascadeAction.SetNull;
        }
        else if (definition.ContainsIgnoreCase("ON DELETE SET DEFAULT"))
        {
            OnDelete = CascadeAction.SetDefault;
        }

        if (definition.ContainsIgnoreCase("ON UPDATE CASCADE"))
        {
            OnUpdate = CascadeAction.Cascade;
        }
        else if (definition.ContainsIgnoreCase("ON UPDATE SET NULL"))
        {
            OnUpdate = CascadeAction.SetNull;
        }
        else if (definition.ContainsIgnoreCase("ON UPDATE SET DEFAULT"))
        {
            OnUpdate = CascadeAction.SetDefault;
        }
    }

    public string ToDDL(Table parent)
    {
        var writer = new StringWriter();
        WriteAddStatement(parent, writer);

        return writer.ToString();
    }

    public void WriteAddStatement(Table parent, TextWriter writer)
    {
        writer.WriteLine($"ALTER TABLE {parent.Identifier}");
        writer.WriteLine($"ADD CONSTRAINT {Name} FOREIGN KEY({ColumnNames.Join(", ")})");
        writer.Write($" REFERENCES {LinkedTable}({LinkedNames.Join(", ")})");
        writer.WriteCascadeAction("ON DELETE", OnDelete);
        writer.WriteCascadeAction("ON UPDATE", OnUpdate);
        writer.Write(";");
        writer.WriteLine();
    }

    public void WriteDropStatement(Table parent, TextWriter writer)
    {
        writer.WriteLine($"ALTER TABLE {parent.Identifier} DROP CONSTRAINT IF EXISTS {Name};");
    }

    public void LinkColumns(string columnName, string referencedName)
    {
        if (ColumnNames == null)
        {
            ColumnNames = new[] { columnName };
            LinkedNames = new[] { referencedName };
        }
        else
        {
            ColumnNames = ColumnNames.Append(columnName).ToArray();
            LinkedNames = LinkedNames.Append(referencedName).ToArray();
        }
    }

    public void ReadReferentialActions(string onDelete, string onUpdate)
    {
        OnDelete = SqlServerProvider.ReadAction(onDelete);
        OnUpdate = SqlServerProvider.ReadAction(onUpdate);
    }
}
