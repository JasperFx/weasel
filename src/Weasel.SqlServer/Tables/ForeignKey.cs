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

    public bool? IsDisabled { get; set; }

    public bool? IsNotForReplication { get; set; }

    public bool? IsNotTrusted { get; set; }

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
    /// <summary>
    ///     A foreign key pairs its columns positionally, but the pairing -- not the order the pairs
    ///     are written in -- is what defines the constraint. Now that both sides keep declaration
    ///     order, comparing the two lists positionally would report drift on a key the caller merely
    ///     wrote in another order, and "fix" it by dropping and recreating an identical constraint.
    /// </summary>
    private bool SamePairs(ForeignKey other)
    {
        if (ColumnNames.Length != other.ColumnNames.Length ||
            LinkedNames.Length != other.LinkedNames.Length ||
            ColumnNames.Length != LinkedNames.Length)
        {
            return false;
        }

        // ColumnComparer is virtual; hardcoding a comparer would silently change case sensitivity
        // and ignore a subclass that overrides it.
        IEnumerable<string> pairs(ForeignKey fk)
            => fk.ColumnNames.Zip(fk.LinkedNames, (c, l) => $"{c}\u0000{l}")
                .OrderBy(x => x, ColumnComparer);

        return pairs(this).SequenceEqual(pairs(other), ColumnComparer);
    }

    public override bool Equals(object? obj)
    {
        if (obj is not ForeignKey other)
        {
            return base.Equals(obj);
        }

        return (base.Equals(obj) || (NameComparer.Equals(Name, other.Name)
                                     && Equals(LinkedTable, other.LinkedTable)
                                     && NormalizeCascadeAction(DeleteAction) ==
                                     NormalizeCascadeAction(other.DeleteAction)
                                     && NormalizeCascadeAction(UpdateAction) ==
                                     NormalizeCascadeAction(other.UpdateAction)
                                     && SamePairs(other)))
               && SameState(other);
    }

    private bool SameState(ForeignKey other)
        => pinnedMatch(IsDisabled, other.IsDisabled)
           && pinnedMatch(IsNotForReplication, other.IsNotForReplication)
           && pinnedMatch(IsNotTrusted, other.IsNotTrusted);

    internal void AssertReachableState()
    {
        if (IsNotTrusted != false) return;

        if (IsNotForReplication == true || IsDisabled == true)
        {
            throw new InvalidOperationException(
                $"Foreign key {Name} pins IsNotTrusted = false together with "
                + (IsNotForReplication == true ? "IsNotForReplication = true" : "IsDisabled = true")
                + ", which SQL Server cannot hold: a NOT FOR REPLICATION or disabled foreign key is always untrusted. "
                + "Leave IsNotTrusted unset or pin it to true.");
        }
    }

    private static bool pinnedMatch(bool? left, bool? right)
        => left is null || right is null || left == right;

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

    /// <summary>
    ///     Write the constraint, guarded by an existence check so a rendered migration script can be
    ///     run more than once.
    /// </summary>
    /// <remarks>
    ///     A constraint is a schema-scoped object, so the guard names the parent table's schema and
    ///     not the linked table's. Unlike the index rendering, nothing compares this text, so the
    ///     guard can live here and every emission site, <see cref="ToDDL" /> included, inherits it.
    /// </remarks>
    public void WriteAddStatement(Table parent, TextWriter writer)
        => writeAddStatement(parent.Identifier, writer, onlyWhenColumnTypesMatch: false);

    /// <summary>
    ///     Write the constraint so that it is only added when every column pair it links has the same type
    ///     on both sides when the statement runs, and is skipped otherwise.
    /// </summary>
    /// <remarks>
    ///     For a foreign key a migration took down to change a key column's type (weasel#870). SQL Server
    ///     refuses a foreign key between columns of different types or lengths (Msg 1778, Msg 1753), and
    ///     when the two tables are changed one after the other, by two deltas of one migration in either
    ///     order or by two migrations, the key can only go back once both sides are done. The first side
    ///     to finish leaves it off, and the second puts it back. If the second side is never changed, the
    ///     next delta of the table holding the key reports it missing.
    /// </remarks>
    public void WriteAddStatementWhenColumnTypesMatch(DbObjectName parent, TextWriter writer)
        => writeAddStatement(parent, writer, onlyWhenColumnTypesMatch: true);

    private void writeAddStatement(DbObjectName parent, TextWriter writer, bool onlyWhenColumnTypesMatch)
    {
        AssertReachableState();

        var constraint = $"N'{SchemaUtils.EscapeLiteral(parent.Schema)}.{SchemaUtils.EscapeLiteral(Name)}'";
        writer.WriteLine($"IF OBJECT_ID({constraint}, N'F') IS NULL");
        if (onlyWhenColumnTypesMatch)
        {
            writeColumnTypesMatch(parent, writer);
        }

        writer.WriteLine(IsNotTrusted == true
            ? $"ALTER TABLE {parent} WITH NOCHECK"
            : $"ALTER TABLE {parent}");
        writer.WriteLine(
            $"ADD CONSTRAINT {SchemaUtils.QuoteName(Name)} FOREIGN KEY({ColumnNames.Select(SchemaUtils.QuoteName).Join(", ")})");
        writer.Write($" REFERENCES {LinkedTable}({LinkedNames.Select(SchemaUtils.QuoteName).Join(", ")})");
        writer.WriteCascadeAction("ON DELETE", OnDelete);
        writer.WriteCascadeAction("ON UPDATE", OnUpdate);
        if (IsNotForReplication == true) writer.Write(" NOT FOR REPLICATION");
        writer.Write(";");
        writer.WriteLine();

        if (IsDisabled == true)
        {
            if (onlyWhenColumnTypesMatch)
            {
                writer.WriteLine($"IF OBJECT_ID({constraint}, N'F') IS NOT NULL");
            }

            writer.WriteLine($"ALTER TABLE {parent} NOCHECK CONSTRAINT {SchemaUtils.QuoteName(Name)};");
        }
    }

    private void writeColumnTypesMatch(DbObjectName parent, TextWriter writer)
    {
        var pairs = ColumnNames.Zip(LinkedNames, (column, linked) =>
            $"(N'{SchemaUtils.EscapeLiteral(SchemaUtils.Unbracket(column))}', N'{SchemaUtils.EscapeLiteral(SchemaUtils.Unbracket(linked))}')");

        writer.WriteLine("    AND NOT EXISTS (");
        writer.WriteLine($"        SELECT 1 FROM (VALUES {pairs.Join(", ")}) AS pair (c, l)");
        writer.WriteLine(
            $"        LEFT JOIN sys.columns rc ON rc.object_id = OBJECT_ID(N'{SchemaUtils.EscapeLiteral(parent.ToString())}') AND rc.name = pair.c");
        writer.WriteLine(
            $"        LEFT JOIN sys.columns lc ON lc.object_id = OBJECT_ID(N'{SchemaUtils.EscapeLiteral(LinkedTable!.ToString())}') AND lc.name = pair.l");
        writer.WriteLine("        WHERE rc.column_id IS NULL OR lc.column_id IS NULL OR rc.user_type_id <> lc.user_type_id");
        writer.WriteLine("            OR rc.max_length <> lc.max_length OR rc.precision <> lc.precision OR rc.scale <> lc.scale)");
    }

    public void WriteDropStatement(Table parent, TextWriter writer)
    {
        writer.WriteLine($"ALTER TABLE {parent.Identifier} DROP CONSTRAINT IF EXISTS {SchemaUtils.QuoteName(Name)};");
    }
}
