using JasperFx.Core;
using Weasel.Core;
using Weasel.Core.Tables;
using CoreCascadeAction = Weasel.Core.Tables.CascadeAction;

namespace Weasel.SqlServer.Tables;

/// <summary>
/// SQL Server-specific foreign key implementation
/// </summary>
public class ForeignKey : ForeignKeyBase, INamed
{
    private string[] _columnNames = null!;
    private string[] _linkedNames = null!;

    public ForeignKey(string name) : base(name)
    {
    }

    /// <summary>
    /// The column names in the source table (auto-sorted alphabetically)
    /// </summary>
    public override string[] ColumnNames
    {
        get => _columnNames;
        set => _columnNames = value.OrderBy(x => x).ToArray();
    }

    /// <summary>
    /// The column names in the referenced table (auto-sorted alphabetically)
    /// </summary>
    public override string[] LinkedNames
    {
        get => _linkedNames;
        set => _linkedNames = value.OrderBy(x => x).ToArray();
    }

    /// <summary>
    /// The referential action to take on DELETE (using SQL Server-specific CascadeAction)
    /// </summary>
    public new CascadeAction OnDelete
    {
        get => (CascadeAction)base.OnDelete;
        set => base.OnDelete = (CoreCascadeAction)value;
    }

    /// <summary>
    /// The referential action to take on UPDATE (using SQL Server-specific CascadeAction)
    /// </summary>
    public new CascadeAction OnUpdate
    {
        get => (CascadeAction)base.OnUpdate;
        set => base.OnUpdate = (CoreCascadeAction)value;
    }

    /// <summary>
    /// Read the DDL definition from the server
    /// </summary>
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
        return ToDDL(parent.Identifier);
    }

    public override string ToDDL(DbObjectName parentIdentifier)
    {
        var writer = new StringWriter();
        WriteAddStatement(parentIdentifier, writer);

        return writer.ToString();
    }

    public void WriteAddStatement(Table parent, TextWriter writer)
    {
        WriteAddStatement(parent.Identifier, writer);
    }

    public override void WriteAddStatement(DbObjectName parentIdentifier, TextWriter writer)
    {
        writer.WriteLine($"ALTER TABLE {parentIdentifier}");
        writer.WriteLine($"ADD CONSTRAINT {Name} FOREIGN KEY({ColumnNames.Join(", ")})");
        writer.Write($" REFERENCES {LinkedTable}({LinkedNames.Join(", ")})");
        writer.WriteCascadeAction("ON DELETE", OnDelete);
        writer.WriteCascadeAction("ON UPDATE", OnUpdate);
        writer.Write(";");
        writer.WriteLine();
    }

    public void WriteDropStatement(Table parent, TextWriter writer)
    {
        WriteDropStatement(parent.Identifier, writer);
    }

    /// <summary>
    /// Links a column to a referenced column
    /// </summary>
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

    /// <summary>
    /// Reads referential actions from SQL Server metadata strings
    /// </summary>
    public void ReadReferentialActions(string onDelete, string onUpdate)
    {
        OnDelete = SqlServerProvider.ReadAction(onDelete);
        OnUpdate = SqlServerProvider.ReadAction(onUpdate);
    }
}
