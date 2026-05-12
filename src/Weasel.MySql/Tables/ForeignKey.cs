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

    /// <summary>
    ///     Pre-existing helper kept for backward compatibility. Equivalent to
    ///     <see cref="object.Equals(object)" /> on this type since 9.0; new callers
    ///     should prefer plain <c>Equals</c>.
    /// </summary>
    public bool IsEquivalentTo(ForeignKey other) => EqualsCore(other);
}
