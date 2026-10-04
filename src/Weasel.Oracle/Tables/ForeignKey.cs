using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Oracle.Tables;

public class ForeignKey: ForeignKeyBase
{
    private string[] _columnNames = null!;
    private string[] _linkedNames = null!;

    public ForeignKey(string name) : base(SchemaUtils.Unquote(name))
    {
    }

    public override string[] ColumnNames
    {
        get => _columnNames;
        set => _columnNames = value.OrderBy(x => x).ToArray();
    }

    public override string[] LinkedNames
    {
        get => _linkedNames;
        set => _linkedNames = value.OrderBy(x => x).ToArray();
    }

    /// <inheritdoc />
    protected override StringComparer NameComparer => StringComparer.OrdinalIgnoreCase;

    /// <inheritdoc />
    protected override StringComparer ColumnComparer => StringComparer.OrdinalIgnoreCase;

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
            Core.CascadeAction.Restrict => CascadeAction.NoAction, // Oracle doesn't support Restrict, map to NoAction
            Core.CascadeAction.Cascade => CascadeAction.Cascade,
            Core.CascadeAction.SetNull => CascadeAction.SetNull,
            Core.CascadeAction.SetDefault => CascadeAction.SetDefault,
            _ => CascadeAction.NoAction
        };
    }
#pragma warning restore CS0618 // Type or member is obsolete

    /// <inheritdoc />
    protected override DbObjectName ParseLinkedTable(string tableName)
        => DbObjectName.Parse(OracleProvider.Instance, tableName);

    public string ToDDL(Table parent)
    {
        var writer = new StringWriter();
        WriteAddStatement(parent, writer);

        return writer.ToString();
    }

    public void WriteAddStatement(Table parent, TextWriter writer)
    {
        // Case-preserved identifiers must be quoted or Oracle folds them to
        // uppercase; the conventional (folded) path stays unquoted as before
        var quote = parent.PreserveIdentifierCase
            ? (Func<string, string>)(x => $"\"{x}\"")
            : x => x;

        writer.WriteLine($"ALTER TABLE {parent.Identifier}");
        writer.WriteLine($"ADD CONSTRAINT {quote(Name)} FOREIGN KEY({ColumnNames.Select(quote).Join(", ")})");
        writer.Write($" REFERENCES {LinkedTable}({LinkedNames.Select(quote).Join(", ")})");
        writer.WriteCascadeAction("ON DELETE", OnDelete);
        writer.WriteLine();
    }

    /// <summary>
    ///     The same <c>ADD CONSTRAINT</c>, wrapped so that running it a second time does nothing
    ///     instead of failing.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         weasel#681. The <c>CREATE TABLE</c> above this in a rendered creation script is
    ///         already wrapped in an <c>all_tables</c> existence check, so a second run skips it and
    ///         reaches this statement — which had no guard, and failed the script with
    ///         <c>ORA-02275: such a referential constraint already exists in the table</c>. This uses
    ///         the same shape as the table's own guard in <see cref="Table.WriteCreateStatement" />,
    ///         against <c>all_constraints</c>.
    ///     </para>
    ///     <para>
    ///         As with the table guard, the statement becomes the text of a PL/SQL string literal, so
    ///         every quote inside it has to be doubled — a cascade clause or a quoted identifier is
    ///         where one turns up. And the name is matched the way Oracle stores it: folded to upper
    ///         case unless the table preserves identifier case, in which case
    ///         <see cref="WriteAddStatement" /> quoted it and the catalog holds it as written.
    ///     </para>
    ///     <para>
    ///         <see cref="WriteAddStatement" /> stays unguarded for the migration path, which only
    ///         ever adds a key it has determined is missing. No rendered migration changes.
    ///     </para>
    /// </remarks>
    public void WriteGuardedAddStatement(Table parent, TextWriter writer)
    {
        var inner = new StringWriter();
        WriteAddStatement(parent, inner);

        var constraintName = parent.PreserveIdentifierCase ? Name : Name.ToUpperInvariant();

        writer.WriteLine("DECLARE");
        writer.WriteLine("    v_count NUMBER;");
        writer.WriteLine("BEGIN");
        writer.WriteLine(
            $"    SELECT COUNT(*) INTO v_count FROM all_constraints WHERE constraint_name = '{SchemaUtils.EscapeLiteral(constraintName)}' AND owner = '{SchemaUtils.EscapeLiteral(parent.Identifier.Schema.ToUpperInvariant())}';");
        writer.WriteLine("    IF v_count = 0 THEN");
        writer.WriteLine($"        EXECUTE IMMEDIATE '{SchemaUtils.EscapeLiteral(inner.ToString().Trim())}';");
        writer.WriteLine("    END IF;");
        writer.WriteLine("END;");
    }

    public void WriteDropStatement(Table parent, TextWriter writer)
    {
        writer.WriteLine($"ALTER TABLE {parent.Identifier} DROP CONSTRAINT {Name}");
    }
}
