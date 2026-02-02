using JasperFx.Core;
using Weasel.Core;
using Weasel.Core.Tables;
using CoreCascadeAction = Weasel.Core.Tables.CascadeAction;

namespace Weasel.Postgresql.Tables;

/// <summary>
/// Exception thrown when a foreign key has invalid column mappings
/// </summary>
public class InvalidForeignKeyException : Exception
{
    public InvalidForeignKeyException(string? message) : base(message)
    {
    }
}

/// <summary>
/// PostgreSQL-specific foreign key implementation
/// </summary>
public class ForeignKey : ForeignKeyBase, INamed
{
    public ForeignKey(string name) : base(name)
    {
    }

    /// <summary>
    /// The referential action to take on DELETE (using PostgreSQL-specific CascadeAction)
    /// </summary>
    public new CascadeAction OnDelete
    {
        get => (CascadeAction)base.OnDelete;
        set => base.OnDelete = (CoreCascadeAction)value;
    }

    /// <summary>
    /// The referential action to take on UPDATE (using PostgreSQL-specific CascadeAction)
    /// </summary>
    public new CascadeAction OnUpdate
    {
        get => (CascadeAction)base.OnUpdate;
        set => base.OnUpdate = (CoreCascadeAction)value;
    }

    /// <summary>
    /// Read the DDL definition from the server
    /// </summary>
    public void Parse(string definition, string schema = "public")
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
        if (!tableName.Contains('.'))
        {
            tableName = $"{schema}.{tableName}";
        }

        LinkedTable = DbObjectName.Parse(PostgresqlProvider.Instance, tableName);

        if (definition.ContainsIgnoreCase("ON DELETE CASCADE"))
        {
            OnDelete = CascadeAction.Cascade;
        }
        else if (definition.ContainsIgnoreCase("ON DELETE RESTRICT"))
        {
            OnDelete = CascadeAction.Restrict;
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
        else if (definition.ContainsIgnoreCase("ON UPDATE RESTRICT"))
        {
            OnUpdate = CascadeAction.Restrict;
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
        writer.Write($"REFERENCES {LinkedTable}({LinkedNames.Join(", ")})");
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
    /// Tries to correct column mappings for multi-column primary key references
    /// </summary>
    public void TryToCorrectForLink(Table parentTable, Table linkedTable)
    {
        // Depends on "id" always being first in Marten world
        // This is important, don't lose the ordering that marten does to put tenant_id first
        if (LinkedNames.Length != linkedTable.PrimaryKeyColumns.Count)
        {
            LinkedNames = LinkedNames.Union(linkedTable.PrimaryKeyColumns).ToArray();
        }

        if (ColumnNames.Length != LinkedNames.Length)
        {
            // Leave the first column alone!
            for (int i = 1; i < LinkedNames.Length; i++)
            {
                var columnName = LinkedNames[i];
                var matching = parentTable.ColumnFor(columnName);
                if (matching != null)
                {
                    ColumnNames = ColumnNames.Concat([columnName]).ToArray();
                }
                else
                {
                    throw new InvalidForeignKeyException(
                        $"Cannot make a foreign key relationship from {parentTable.Identifier}({ColumnNames.Join(", ")}) to {linkedTable.Identifier}({LinkedNames.Join(", ")}) ");
                }
            }
        }
    }
}
