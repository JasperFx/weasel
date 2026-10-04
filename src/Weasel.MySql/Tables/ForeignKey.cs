using System.Text;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.MySql.Tables;

public class ForeignKey: ForeignKeyBase
{
    private readonly List<string> _columnNames = new();
    private readonly List<string> _linkedNames = new();

    public ForeignKey(string name) : base(SchemaUtils.Unquote(name))
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
    /// <remarks>
    ///     MySQL's catalog returns FK metadata as separate columns rather than a
    ///     pre-formatted DDL string, so <c>Parse</c> is never called in practice —
    ///     but the contract is here in case a caller does use it.
    /// </remarks>
    protected override DbObjectName ParseLinkedTable(string tableName)
        => DbObjectName.Parse(MySqlProvider.Instance, tableName);

    /// <summary>
    ///     The foreign key as a table-level constraint for inside <c>CREATE TABLE</c>, rather than as
    ///     a following <c>ALTER TABLE</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         weasel#686. Declared inline, the key is covered by the table's own
    ///         <c>CREATE TABLE IF NOT EXISTS</c>, which is what makes a rendered creation script
    ///         re-runnable. A trailing <c>ALTER TABLE … ADD CONSTRAINT</c> has no guard available on
    ///         MySQL — there is no <c>IF NOT EXISTS</c> for a constraint and no anonymous block to
    ///         catch the duplicate with, the way PostgreSQL and Oracle have (weasel#681) — so a
    ///         second run of the script failed with <c>1826, Duplicate foreign key constraint
    ///         name</c>.
    ///     </para>
    ///     <para>
    ///         InnoDB resolves an inline foreign key at <c>CREATE TABLE</c> time, so the referenced
    ///         table has to exist already. That is why this became available only once weasel#677
    ///         made the script order its tables by dependency; before that it would have swapped one
    ///         failure for another.
    ///     </para>
    ///     <para>
    ///         <see cref="ToDDL" /> is unchanged and still the migration path's statement: a delta
    ///         adds a key to a table that already exists, where an <c>ALTER</c> is the only option.
    ///     </para>
    /// </remarks>
    public void WriteInlineDefinition(TextWriter writer)
    {
        if (LinkedTable == null)
        {
            throw new InvalidOperationException("LinkedTable must be set before generating DDL");
        }

        writer.Write(
            $"CONSTRAINT {SchemaUtils.QuoteName(Name)} FOREIGN KEY ({_columnNames.Select(SchemaUtils.QuoteName).Join(", ")})");
        writer.Write(
            $" REFERENCES {LinkedTable.QualifiedName} ({_linkedNames.Select(SchemaUtils.QuoteName).Join(", ")})");

        if (OnDelete != CascadeAction.NoAction)
        {
            writer.Write($" ON DELETE {GetCascadeActionSql(OnDelete)}");
        }

        if (OnUpdate != CascadeAction.NoAction)
        {
            writer.Write($" ON UPDATE {GetCascadeActionSql(OnUpdate)}");
        }
    }

    /// <summary>
    ///     The inline declaration as a string. See <see cref="WriteInlineDefinition" />.
    /// </summary>
    public string ToInlineDefinition()
    {
        var writer = new StringWriter();
        WriteInlineDefinition(writer);

        return writer.ToString();
    }

    public string ToDDL(Table parent)
    {
        if (LinkedTable == null)
        {
            throw new InvalidOperationException("LinkedTable must be set before generating DDL");
        }

        var builder = new StringBuilder();
        builder.Append($"ALTER TABLE {parent.Identifier.QualifiedName} ADD CONSTRAINT {SchemaUtils.QuoteName(Name)} ");
        builder.Append($"FOREIGN KEY ({_columnNames.Select(c => $"{SchemaUtils.QuoteName(c)}").Join(", ")}) ");
        builder.Append($"REFERENCES {LinkedTable.QualifiedName} ({_linkedNames.Select(c => $"{SchemaUtils.QuoteName(c)}").Join(", ")})");

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

    /// <summary>
    ///     Pre-existing helper kept for backward compatibility. Equivalent to
    ///     <see cref="object.Equals(object)" /> on this type since 9.0; new callers
    ///     should prefer plain <c>Equals</c>.
    /// </summary>
    public bool IsEquivalentTo(ForeignKey other) => EqualsCore(other);
}
