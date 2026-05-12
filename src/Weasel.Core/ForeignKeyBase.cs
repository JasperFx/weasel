using JasperFx.Core;

namespace Weasel.Core;

/// <summary>
///     Cross-provider base for foreign-key constraint definitions. Owns the columns,
///     the referenced table, the cascade actions, and the boilerplate that every
///     provider was reimplementing nearly verbatim:
///     <list type="bullet">
///         <item>
///             <see cref="Parse" /> — parse a catalog-supplied
///             <c>"FOREIGN KEY (..) REFERENCES schema.tbl(..) [ON DELETE …] [ON UPDATE …]"</c>
///             string. Subclasses only supply the provider-specific
///             <see cref="ParseLinkedTable" /> hook for wrapping the table name in the
///             right <see cref="DbObjectName" /> subclass.
///         </item>
///         <item>
///             <see cref="LinkColumns" /> — append a (dependent, principal) column pair.
///         </item>
///         <item>
///             <see cref="ReadReferentialActions" /> — translate catalog action text
///             into <see cref="CascadeAction" /> via a virtual
///             <see cref="ReadAction" /> hook (subclasses route to the provider's
///             static <c>ReadAction</c>).
///         </item>
///         <item>
///             <see cref="Equals(object)" /> / <see cref="GetHashCode" /> — structural
///             equality across columns, linked table and cascades, parameterised by
///             the virtual <see cref="NameComparer" /> / <see cref="ColumnComparer" />
///             so case-sensitive providers (PostgreSQL, SQL Server) and case-insensitive
///             providers (Oracle, SQLite, MySQL) reuse the same code.
///         </item>
///     </list>
///     Provider-specific <see cref="WriteAddStatement" /> / <see cref="WriteDropStatement" />
///     are still per-subclass because they take a provider-specific <c>Table</c>; lifting
///     those is blocked on <c>TableBase</c> (#270 step 9).
/// </summary>
public abstract class ForeignKeyBase : INamed
{
    protected ForeignKeyBase(string name)
    {
        Name = name;
    }

    /// <summary>
    /// The name of the foreign key constraint
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// The column names in the child/dependent table that make up the foreign key
    /// </summary>
    public abstract string[] ColumnNames { get; set; }

    /// <summary>
    /// The column names in the parent/principal table that are referenced
    /// </summary>
    public abstract string[] LinkedNames { get; set; }

    /// <summary>
    /// The referenced/principal table
    /// </summary>
    public DbObjectName? LinkedTable { get; set; }

    /// <summary>
    /// The cascade action to take when a referenced row is deleted
    /// </summary>
    public CascadeAction DeleteAction { get; set; } = CascadeAction.NoAction;

    /// <summary>
    /// The cascade action to take when a referenced row is updated
    /// </summary>
    public CascadeAction UpdateAction { get; set; } = CascadeAction.NoAction;

    /// <summary>
    ///     How <see cref="Name" /> is compared for equality / hashing. PostgreSQL and
    ///     SQL Server use <see cref="StringComparer.Ordinal" />; Oracle, SQLite, MySQL
    ///     fold case (catalog metadata is case-folded on those providers).
    /// </summary>
    protected virtual StringComparer NameComparer => StringComparer.Ordinal;

    /// <summary>
    ///     How <see cref="ColumnNames" /> / <see cref="LinkedNames" /> entries are
    ///     compared. Defaults to ordinal; the case-insensitive providers override.
    /// </summary>
    protected virtual StringComparer ColumnComparer => StringComparer.Ordinal;

    /// <summary>
    ///     Parse a foreign-key DDL fragment of the shape
    ///     <c>"FOREIGN KEY (col1, col2) REFERENCES schema.tbl(col1, col2) [ON DELETE …] [ON UPDATE …]"</c>
    ///     into this instance. The format is the lowest common denominator of every
    ///     provider's catalog query, so the body lives here.
    ///     Subclasses supply <see cref="ParseLinkedTable" /> for the provider-specific
    ///     <see cref="DbObjectName" /> wrapping (PostgreSQL's <c>PostgresqlObjectName</c>,
    ///     SQL Server's <c>SqlServerObjectName</c>, etc.).
    /// </summary>
    /// <param name="definition">The DDL fragment from the catalog query.</param>
    /// <param name="defaultSchema">
    ///     When the parsed table name is unqualified, prefix this schema. PostgreSQL
    ///     callers pass <c>"public"</c>; other providers pass <c>null</c> (and rely on
    ///     the catalog already qualifying the name).
    /// </param>
    public virtual void Parse(string definition, string? defaultSchema = null)
    {
        var open1 = definition.IndexOf('(');
        var closed1 = definition.IndexOf(')');

        ColumnNames = definition.Substring(open1 + 1, closed1 - open1 - 1).ToDelimitedArray(',');

        var open2 = definition.IndexOf('(', closed1);
        var closed2 = definition.IndexOf(')', open2);

        LinkedNames = definition.Substring(open2 + 1, closed2 - open2 - 1).ToDelimitedArray(',');

        const string references = "REFERENCES";
        var tableStart = definition.IndexOf(references, StringComparison.OrdinalIgnoreCase) + references.Length;

        var tableName = definition.Substring(tableStart, open2 - tableStart).Trim();
        if (defaultSchema != null && !tableName.Contains('.'))
        {
            tableName = $"{defaultSchema}.{tableName}";
        }
        LinkedTable = ParseLinkedTable(tableName);

        DeleteAction = ParseCascadeClause(definition, "ON DELETE");
        UpdateAction = ParseCascadeClause(definition, "ON UPDATE");
    }

    /// <summary>
    ///     Wrap the parsed table name in the provider-specific
    ///     <see cref="DbObjectName" /> subclass. Called from <see cref="Parse" />.
    /// </summary>
    protected abstract DbObjectName ParseLinkedTable(string tableName);

    /// <summary>
    ///     Scan a DDL fragment for an <c>"ON DELETE x"</c> or <c>"ON UPDATE x"</c>
    ///     clause and return the corresponding <see cref="CascadeAction" />. Providers
    ///     that don't support a particular action (e.g. SQL Server has no RESTRICT)
    ///     simply never see that text in their catalog output, so the broader check
    ///     here is safe.
    /// </summary>
    protected static CascadeAction ParseCascadeClause(string definition, string prefix)
    {
        if (definition.ContainsIgnoreCase($"{prefix} CASCADE"))
        {
            return CascadeAction.Cascade;
        }
        if (definition.ContainsIgnoreCase($"{prefix} RESTRICT"))
        {
            return CascadeAction.Restrict;
        }
        if (definition.ContainsIgnoreCase($"{prefix} SET NULL"))
        {
            return CascadeAction.SetNull;
        }
        if (definition.ContainsIgnoreCase($"{prefix} SET DEFAULT"))
        {
            return CascadeAction.SetDefault;
        }
        return CascadeAction.NoAction;
    }

    /// <summary>
    ///     Append a (dependent column, referenced column) pair to
    ///     <see cref="ColumnNames" /> and <see cref="LinkedNames" />. Used by table
    ///     fluent APIs and by catalog-introspection codepaths that build the FK row
    ///     by row.
    /// </summary>
    public virtual void LinkColumns(string columnName, string referencedName)
    {
        if (ColumnNames == null)
        {
            ColumnNames = new[] { columnName };
            LinkedNames = new[] { referencedName };
        }
        else
        {
            ColumnNames = ColumnNames.Append(columnName).ToArray();
            LinkedNames = LinkedNames.Append(referencedName).ToArray();
        }
    }

    /// <summary>
    ///     Translate provider-supplied catalog cascade-action text into
    ///     <see cref="CascadeAction" />. The default handles every spelling used
    ///     across PostgreSQL / SQL Server / Oracle / MySQL / SQLite catalog rows
    ///     (CASCADE / RESTRICT / NO_ACTION / NO ACTION / SET_NULL / SET NULL /
    ///     SET_DEFAULT / SET DEFAULT). Providers that don't support a particular
    ///     action simply never see that text in their catalog, so the broader
    ///     mapping here is safe. Override only if a provider emits a non-standard
    ///     spelling.
    /// </summary>
    protected virtual CascadeAction ReadAction(string description)
    {
        return description.ToUpperInvariant().Trim() switch
        {
            "CASCADE" => CascadeAction.Cascade,
            "RESTRICT" => CascadeAction.Restrict,
            "NO ACTION" or "NO_ACTION" => CascadeAction.NoAction,
            "SET NULL" or "SET_NULL" => CascadeAction.SetNull,
            "SET DEFAULT" or "SET_DEFAULT" => CascadeAction.SetDefault,
            _ => CascadeAction.NoAction
        };
    }

    /// <summary>
    ///     Populate <see cref="DeleteAction" /> and <see cref="UpdateAction" /> from
    ///     catalog text. Either side may be <c>null</c> (e.g. Oracle never has an
    ///     ON UPDATE clause, so its row's <c>onUpdate</c> is null).
    /// </summary>
    public void ReadReferentialActions(string? onDelete, string? onUpdate = null)
    {
        if (onDelete != null)
        {
            DeleteAction = ReadAction(onDelete);
        }

        if (onUpdate != null)
        {
            UpdateAction = ReadAction(onUpdate);
        }
    }

    /// <summary>
    ///     Structural equality across foreign keys from the same provider: name, both
    ///     column lists, the referenced table, and both cascade actions. Uses the
    ///     virtual <see cref="NameComparer" /> / <see cref="ColumnComparer" /> so
    ///     providers that fold case (Oracle, SQLite, MySQL) get case-insensitive
    ///     matching for free.
    ///     <para>
    ///     Two foreign keys are eligible for structural comparison when they share
    ///     the same <em>provider root</em> — the immediate subclass of
    ///     <see cref="ForeignKeyBase" /> (e.g. <c>Weasel.Postgresql.Tables.ForeignKey</c>).
    ///     This matches the pre-9.0 per-provider <c>Equals</c> behaviour that allowed
    ///     consumer-supplied marker subclasses (Marten ships one) to compare equal to
    ///     plain provider foreign keys, while still rejecting cross-provider comparison.
    ///     </para>
    /// </summary>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj))
        {
            return false;
        }
        if (ReferenceEquals(this, obj))
        {
            return true;
        }
        if (obj is not ForeignKeyBase other)
        {
            return false;
        }
        if (!GetProviderRootType(GetType()).IsAssignableFrom(other.GetType()))
        {
            return false;
        }
        return EqualsCore(other);
    }

    /// <summary>
    ///     Walks the inheritance chain from <paramref name="t" /> up to the first type
    ///     whose immediate base is <see cref="ForeignKeyBase" /> — i.e. the provider's
    ///     concrete <c>ForeignKey</c> class. Used by <see cref="Equals(object)" /> to
    ///     allow consumer-defined subclasses of a provider's <c>ForeignKey</c> to compare
    ///     equal to plain provider foreign keys.
    /// </summary>
    private static Type GetProviderRootType(Type t)
    {
        while (t.BaseType != typeof(ForeignKeyBase))
        {
            t = t.BaseType!;
        }
        return t;
    }

    /// <summary>
    ///     Field-by-field comparison reused by <see cref="Equals(object)" /> and
    ///     available to subclasses that need to compose it (e.g. an existing
    ///     <c>IsEquivalentTo</c> wrapper).
    /// </summary>
    protected bool EqualsCore(ForeignKeyBase other)
    {
        return NameComparer.Equals(Name, other.Name)
               && ColumnNames.SequenceEqual(other.ColumnNames, ColumnComparer)
               && LinkedNames.SequenceEqual(other.LinkedNames, ColumnComparer)
               && Equals(LinkedTable, other.LinkedTable)
               && DeleteAction == other.DeleteAction
               && UpdateAction == other.UpdateAction;
    }

    /// <summary>
    ///     Hash is built from <see cref="Name" />, <see cref="LinkedTable" /> and both
    ///     cascade actions. Array contents are intentionally omitted: previous per-
    ///     provider implementations hashed the array reference, which gave different
    ///     hashes for structurally-equal foreign keys — a real bug. Omitting the
    ///     arrays gives more hash collisions but keeps <c>HashSet</c> / <c>Dictionary</c>
    ///     correct.
    /// </summary>
    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = Name != null ? NameComparer.GetHashCode(Name) : 0;
            hashCode = (hashCode * 397) ^ (LinkedTable != null ? LinkedTable.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (int)DeleteAction;
            hashCode = (hashCode * 397) ^ (int)UpdateAction;
            return hashCode;
        }
    }
}
