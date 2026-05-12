using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Sqlite.Tables;

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

    /// <summary>
    ///     SQLite uses <see cref="Core.CascadeAction" /> directly — there's no
    ///     provider-local enum shim like PostgreSQL / SQL Server / Oracle / MySQL.
    ///     These aliases exist so call sites that still spell the cascade as
    ///     <c>OnDelete</c> / <c>OnUpdate</c> continue to compile.
    /// </summary>
    public CascadeAction OnDelete
    {
        get => DeleteAction;
        set => DeleteAction = value;
    }

    public CascadeAction OnUpdate
    {
        get => UpdateAction;
        set => UpdateAction = value;
    }

    /// <inheritdoc />
    protected override DbObjectName ParseLinkedTable(string tableName)
        => DbObjectName.Parse(SqliteProvider.Instance, tableName);

    public string ToDDL(Table parent)
    {
        var writer = new StringWriter();
        WriteInlineDefinition(writer);
        return writer.ToString();
    }

    /// <summary>
    /// SQLite foreign keys are typically defined inline with CREATE TABLE
    /// This returns the FOREIGN KEY constraint for use in CREATE TABLE statement
    /// </summary>
    public void WriteInlineDefinition(TextWriter writer)
    {
        writer.Write($"CONSTRAINT {SchemaUtils.QuoteName(Name)} FOREIGN KEY ({ColumnNames.Select(SchemaUtils.QuoteName).Join(", ")})");
        // SQLite FOREIGN KEY REFERENCES only supports table name, not schema-qualified names
        // The referenced table must be in the same database as the current table
        writer.Write($" REFERENCES {SchemaUtils.QuoteName(LinkedTable.Name)} ({LinkedNames.Select(SchemaUtils.QuoteName).Join(", ")})");

        if (DeleteAction != CascadeAction.NoAction)
        {
            writer.WriteCascadeAction("ON DELETE", DeleteAction);
        }

        if (UpdateAction != CascadeAction.NoAction)
        {
            writer.WriteCascadeAction("ON UPDATE", UpdateAction);
        }
    }

    /// <summary>
    /// SQLite does not support ALTER TABLE ADD CONSTRAINT for foreign keys.
    /// Foreign keys must be defined at table creation time.
    /// </summary>
    public void WriteAddStatement(Table parent, TextWriter writer)
    {
        throw new NotSupportedException(
            "SQLite does not support ALTER TABLE ADD CONSTRAINT for foreign keys. " +
            "Foreign keys must be defined when the table is created. " +
            $"Table '{parent.Identifier}' must be recreated to add foreign key '{Name}'.");
    }

    /// <summary>
    /// SQLite does not support ALTER TABLE DROP CONSTRAINT.
    /// The table must be recreated without the foreign key.
    /// </summary>
    public void WriteDropStatement(Table parent, TextWriter writer)
    {
        throw new NotSupportedException(
            "SQLite does not support ALTER TABLE DROP CONSTRAINT for foreign keys. " +
            $"Table '{parent.Identifier}' must be recreated to remove foreign key '{Name}'.");
    }
}
