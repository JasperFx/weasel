namespace Weasel.Core;

/// <summary>
///     Shared base for provider-specific <c>Sequence</c> classes (PostgreSQL, SQL Server,
///     Oracle, MySQL). Owns the cross-provider properties — identifier, optional starting
///     value, optional owner column — and the standard <see cref="ISchemaObject" />
///     boilerplate. Concrete subclasses only implement the provider-specific DDL.
/// </summary>
public abstract class SequenceBase: SchemaObjectBase
{
    protected SequenceBase(DbObjectName identifier) : base(identifier)
    {
    }

    protected SequenceBase(DbObjectName identifier, long startWith) : base(identifier)
    {
        StartWith = startWith;
    }

    /// <summary>
    ///     Optional starting value for the sequence. When null, the provider's default is used
    ///     (typically 1).
    /// </summary>
    public long? StartWith { get; set; }

    /// <summary>
    ///     Optional table that "owns" this sequence (PostgreSQL's
    ///     <c>ALTER SEQUENCE … OWNED BY tbl.col</c>). On providers that do not support sequence
    ///     ownership semantics this is typically ignored.
    /// </summary>
    public DbObjectName? Owner { get; set; }

    /// <summary>
    ///     Name of the column on <see cref="Owner" /> that the sequence is bound to. Only
    ///     meaningful when <see cref="Owner" /> is set.
    /// </summary>
    public string OwnerColumn { get; set; } = null!;
}
