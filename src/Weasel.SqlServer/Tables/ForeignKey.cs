using JasperFx.Core;
using Weasel.Core;

namespace Weasel.SqlServer.Tables;

public class ForeignKey: ForeignKeyBase
{
    private string[] _columnNames = null!;
    private string[] _linkedNames = null!;

    public ForeignKey(string name) : base(SchemaUtils.Unbracket(name))
    {
    }

    // Declaration order is preserved on both sides. These setters used to sort each side
    // independently, which made a composite FK whose two sides don't sort into the same relative
    // order pair the wrong columns together -- ColumnNames[i] must stay matched to LinkedNames[i].
    // The sort was compensating for the catalog query having no ORDER BY; that query now orders by
    // constraint_column_id instead. See Table.FetchExisting.cs.
    public override string[] ColumnNames
    {
        get => _columnNames;
        set => _columnNames = value.Select(SchemaUtils.Unbracket).ToArray();
    }

    public override string[] LinkedNames
    {
        get => _linkedNames;
        set => _linkedNames = value.Select(SchemaUtils.Unbracket).ToArray();
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
            Core.CascadeAction.Restrict => CascadeAction.NoAction, // SQL Server doesn't support Restrict, map to NoAction
            Core.CascadeAction.Cascade => CascadeAction.Cascade,
            Core.CascadeAction.SetNull => CascadeAction.SetNull,
            Core.CascadeAction.SetDefault => CascadeAction.SetDefault,
            _ => CascadeAction.NoAction
        };
    }
#pragma warning restore CS0618 // Type or member is obsolete

    /// <summary>
    ///     SQL Server has no ON DELETE RESTRICT — it is written and reported as
    ///     NO ACTION, so the two must compare as equal during delta detection.
    /// </summary>
    protected override Core.CascadeAction NormalizeCascadeAction(Core.CascadeAction action)
        => action == Core.CascadeAction.Restrict ? Core.CascadeAction.NoAction : action;

    /// <inheritdoc />
    protected override DbObjectName ParseLinkedTable(string tableName)
        => DbObjectName.Parse(SqlServerProvider.Instance, tableName);

    public string ToDDL(Table parent)
    {
        var writer = new StringWriter();
        WriteAddStatement(parent, writer);

        return writer.ToString();
    }

    public void WriteAddStatement(Table parent, TextWriter writer)
    {
        writer.WriteLine($"ALTER TABLE {parent.Identifier}");
        writer.WriteLine(
            $"ADD CONSTRAINT {SchemaUtils.QuoteName(Name)} FOREIGN KEY({ColumnNames.Select(SchemaUtils.QuoteName).Join(", ")})");
        writer.Write($" REFERENCES {LinkedTable}({LinkedNames.Select(SchemaUtils.QuoteName).Join(", ")})");
        writer.WriteCascadeAction("ON DELETE", OnDelete);
        writer.WriteCascadeAction("ON UPDATE", OnUpdate);
        writer.Write(";");
        writer.WriteLine();
    }

    public void WriteDropStatement(Table parent, TextWriter writer)
    {
        writer.WriteLine($"ALTER TABLE {parent.Identifier} DROP CONSTRAINT IF EXISTS {SchemaUtils.QuoteName(Name)};");
    }
}
