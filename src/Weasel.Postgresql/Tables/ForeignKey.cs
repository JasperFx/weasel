using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Postgresql.Tables;

public class ForeignKey: ForeignKeyBase
{
    private string[] _columnNames = null!;
    private string[] _linkedNames = null!;

    public ForeignKey(string name) : base(name)
    {
    }

    public override string[] ColumnNames
    {
        get => _columnNames;
        set => _columnNames = value;
    }

    public override string[] LinkedNames
    {
        get => _linkedNames;
        set => _linkedNames = value;
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

    /// <inheritdoc />
    protected override DbObjectName ParseLinkedTable(string tableName)
        => DbObjectName.Parse(PostgresqlProvider.Instance, tableName);

    /// <summary>
    ///     PostgreSQL-specific convenience overload that defaults <paramref name="schema" />
    ///     to <c>"public"</c> when the catalog row hands back an unqualified table name.
    ///     Calls into <see cref="ForeignKeyBase.Parse" /> for the shared body.
    /// </summary>
    public new void Parse(string definition, string schema = "public")
        => base.Parse(definition, schema);

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
        writer.Write($"REFERENCES {LinkedTable}({LinkedNames.Join(", ")})");
        writer.WriteCascadeAction("ON DELETE", OnDelete);
        writer.WriteCascadeAction("ON UPDATE", OnUpdate);
        writer.Write(";");
        writer.WriteLine();
    }

    public void WriteDropStatement(Table parent, TextWriter writer)
    {
        writer.WriteLine($"ALTER TABLE {parent.Identifier} DROP CONSTRAINT IF EXISTS {Name};");
    }

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

public class InvalidForeignKeyException: Exception
{
    public InvalidForeignKeyException(string? message) : base(message)
    {
    }
}
