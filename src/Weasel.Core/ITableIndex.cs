namespace Weasel.Core;

/// <summary>
///     Provider-neutral view of an index definition on a table. This is the
///     lowest common denominator supported by every provider's concrete
///     IndexDefinition type; richer, provider-specific options (index methods,
///     included columns, sort order, fill factor, ...) remain on the concrete
///     types.
/// </summary>
public interface ITableIndex : INamed
{
    /// <summary>Whether this is a UNIQUE index</summary>
    bool IsUnique { get; set; }

    /// <summary>
    ///     Optional WHERE predicate for a partial / filtered index. Written in
    ///     the provider's SQL dialect. Not every provider supports filtered
    ///     indexes — setting it on one that doesn't will surface at DDL time.
    /// </summary>
    string? Predicate { get; set; }

    /// <summary>
    ///     Optional non-key (INCLUDE) columns for a covering index. Supported by
    ///     PostgreSQL 11+ and SQL Server; providers without covering-index
    ///     support throw <see cref="NotSupportedException" /> from the setter.
    /// </summary>
    string[]? IncludeColumns { get; set; }
}
