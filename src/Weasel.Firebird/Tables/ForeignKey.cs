using System.Text;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Firebird.Tables;

public class ForeignKey: ForeignKeyBase
{
    private readonly List<string> _columnNames = new();
    private readonly List<string> _linkedNames = new();

    public ForeignKey(string name): base(SchemaUtils.Unquote(name))
    {
    }

    /// <summary>
    ///     The constrained columns, in key order. Kept in the order given, because a composite key pairs
    ///     each one with the referenced column at the same position.
    /// </summary>
    public override string[] ColumnNames
    {
        get => _columnNames.ToArray();
        set
        {
            _columnNames.Clear();
            _columnNames.AddRange(value.Select(SchemaUtils.Unquote));
        }
    }

    public override string[] LinkedNames
    {
        get => _linkedNames.ToArray();
        set
        {
            _linkedNames.Clear();
            _linkedNames.AddRange(value.Select(SchemaUtils.Unquote));
        }
    }

    /// <inheritdoc />
    protected override StringComparer NameComparer => StringComparer.OrdinalIgnoreCase;

    /// <inheritdoc />
    protected override StringComparer ColumnComparer => StringComparer.OrdinalIgnoreCase;

    /// <summary>
    ///     Firebird records a key declared without an action as <c>RESTRICT</c> and one declared
    ///     <c>NO ACTION</c> as that; the two behave alike, and <c>RESTRICT</c> cannot be written back,
    ///     so they are one action here.
    /// </summary>
    protected override CascadeAction NormalizeCascadeAction(CascadeAction action)
        => action == CascadeAction.Restrict ? CascadeAction.NoAction : action;

    /// <inheritdoc />
    protected override CascadeAction ReadAction(string description) => FirebirdProvider.ReadAction(description);

    /// <inheritdoc />
    protected override DbObjectName ParseLinkedTable(string tableName)
        => FirebirdObjectName.From(FirebirdProvider.Instance.Parse(tableName));

    /// <summary>
    ///     This key's <c>ALTER TABLE … ADD CONSTRAINT</c> statement, unterminated and unguarded.
    /// </summary>
    public string ToDDL(Table parent)
    {
        if (LinkedTable == null)
        {
            throw new InvalidOperationException($"Foreign key {Name} needs a LinkedTable before it can be written");
        }

        FirebirdObjectName.AssertDefaultSchema(LinkedTable.Schema, $"a foreign key to {LinkedTable.Name}");

        var preserveCase = parent.PreserveIdentifierCase;
        string quote(string name) => SchemaUtils.QuoteName(name, preserveCase);

        var builder = new StringBuilder();
        builder.Append($"ALTER TABLE {parent.QuotedName} ADD CONSTRAINT {quote(Name)} ");
        builder.Append($"FOREIGN KEY ({_columnNames.Select(quote).Join(", ")}) ");
        builder.Append($"REFERENCES {quote(LinkedTable.Name)} ({_linkedNames.Select(quote).Join(", ")})");
        builder.AppendCascadeAction("ON DELETE", DeleteAction);
        builder.AppendCascadeAction("ON UPDATE", UpdateAction);

        return builder.ToString();
    }

    internal string CatalogName(Table parent) => SchemaUtils.CatalogName(Name, parent.PreserveIdentifierCase);

    /// <summary>
    ///     Write the key, guarded on its name: constraint and index names share one namespace in a
    ///     Firebird database, so the probe is the constraint catalog.
    /// </summary>
    public void WriteAddStatement(Table parent, TextWriter writer)
    {
        FirebirdScript.WriteGuarded(writer, CatalogProbes.Constraint(CatalogName(parent)), ToDDL(parent));
    }

    public void WriteDropStatement(Table parent, TextWriter writer)
    {
        FirebirdScript.WriteGuardedWhenExists(writer, CatalogProbes.Constraint(CatalogName(parent)),
            $"ALTER TABLE {parent.QuotedName} DROP CONSTRAINT {SchemaUtils.QuoteName(Name, parent.PreserveIdentifierCase)}");
    }

    public bool IsEquivalentTo(ForeignKey other) => EqualsCore(other);
}
