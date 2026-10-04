using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Postgresql.Tables;

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

    /// <summary>
    ///     Foreign key names and columns are compared case-insensitively:
    ///     a case-preserved expected FK (e.g. EF Core's "FK_Posts_Blogs_BlogId")
    ///     must match the same constraint read back from the catalog regardless
    ///     of whether it was created quoted or folded to lowercase.
    /// </summary>
    protected override StringComparer NameComparer => StringComparer.OrdinalIgnoreCase;

    /// <inheritdoc cref="NameComparer" />
    protected override StringComparer ColumnComparer => StringComparer.OrdinalIgnoreCase;

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
        var columns = ColumnNames.Select(x => SchemaUtils.QuoteName(x)).Join(", ");
        var linkedColumns = LinkedNames.Select(x => SchemaUtils.QuoteName(x)).Join(", ");

        writer.WriteLine($"ALTER TABLE {parent.Identifier}");
        writer.WriteLine($"ADD CONSTRAINT {SchemaUtils.QuoteName(Name)} FOREIGN KEY({columns})");
        writer.Write($"REFERENCES {LinkedTable}({linkedColumns})");
        writer.WriteCascadeAction("ON DELETE", OnDelete);
        writer.WriteCascadeAction("ON UPDATE", OnUpdate);
        writer.Write(";");
        writer.WriteLine();
    }

    /// <summary>
    ///     The same <c>ADD CONSTRAINT</c>, wrapped so that running it a second time does nothing
    ///     instead of failing.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         weasel#681. A rendered creation script creates its tables with
    ///         <c>CREATE TABLE IF NOT EXISTS</c>, so a second run sails past them and reaches this
    ///         statement — which had no guard of its own, and failed the whole script with
    ///         <c>42710: constraint "…" for relation "…" already exists</c>. A model with no foreign
    ///         keys re-ran fine, which is why it went unnoticed. SQL Server already guarded its
    ///         equivalent with <c>IF OBJECT_ID(…, N'F') IS NULL</c> and Firebird with a catalog probe,
    ///         so PostgreSQL was the outlier.
    ///     </para>
    ///     <para>
    ///         The guard catches <c>duplicate_object</c> rather than testing <c>pg_constraint</c>
    ///         first, for the same reason the schema-creation guard in <c>PostgresqlMigrator</c>
    ///         keeps its <c>EXCEPTION</c> block: no existence check is concurrent-safe, since two
    ///         sessions can both pass it and then race on the catalog. Catching the error is the
    ///         check, and it needs no second statement.
    ///     </para>
    ///     <para>
    ///         Like the <c>IF NOT EXISTS</c> around the table itself, this is "create if absent" and
    ///         not "reconcile": a constraint of this name that differs from the model is left alone
    ///         rather than replaced. Reconciling is the migration path's job, and
    ///         <see cref="WriteAddStatement" /> is deliberately left unguarded for it — a delta only
    ///         ever adds a key it has determined is missing, and leaving that output untouched means
    ///         no rendered migration changes.
    ///     </para>
    /// </remarks>
    public void WriteGuardedAddStatement(Table parent, TextWriter writer)
    {
        var inner = new StringWriter();
        WriteAddStatement(parent, inner);

        writer.WriteLine("DO $do$");
        writer.WriteLine("BEGIN");
        writer.Write(inner.ToString());
        writer.WriteLine("EXCEPTION");
        writer.WriteLine("    WHEN duplicate_object THEN NULL;");
        writer.WriteLine("END");
        writer.WriteLine("$do$;");
    }

    public void WriteDropStatement(Table parent, TextWriter writer)
    {
        writer.WriteLine($"ALTER TABLE {parent.Identifier} DROP CONSTRAINT IF EXISTS {SchemaUtils.QuoteName(Name)};");
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
