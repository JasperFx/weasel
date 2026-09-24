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
    /// <summary>
    ///     The key columns of the index, in order. May be null or empty for
    ///     expression-based indexes on providers that support them (PostgreSQL,
    ///     SQLite) — those indexes carry the expression on the concrete type.
    /// </summary>
    string[]? Columns { get; set; }

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

    /// <summary>
    ///     Optional index access method, e.g. PostgreSQL's gin/gist/hash/brin
    ///     (CREATE INDEX ... USING method) or MySQL's btree/hash. Null means the
    ///     provider default (btree). Providers without pluggable index methods
    ///     throw <see cref="NotSupportedException" /> when a non-null method is
    ///     assigned.
    /// </summary>
    string? Method { get; set; }

    /// <summary>
    ///     True when this index carries options that the members above cannot express — an
    ///     operator class or mask, a sort or nulls order, a collation, a tablespace, storage
    ///     parameters, and so on. A consumer that can only read the neutral surface has to treat
    ///     such an index as opaque and fall back on <see cref="ToDDL" />, because reconstructing
    ///     it from the neutral properties alone would silently produce a <em>different</em> index
    ///     that still applies without error (weasel#615).
    /// </summary>
    /// <remarks>
    ///     How an index is <em>built</em> is not an option in this sense. PostgreSQL's
    ///     <c>IsConcurrent</c> is deliberately excluded: it says nothing about the shape of the
    ///     resulting index, it is a property of the moment rather than of the definition, and a
    ///     consumer that cannot build concurrently in the first place is not losing anything by
    ///     ignoring it.
    /// </remarks>
    bool HasProviderSpecificOptions { get; }

    /// <summary>
    ///     This index's own <c>CREATE INDEX</c> DDL against the given table, for a consumer that
    ///     cannot reproduce it from the neutral properties. Provider-neutral counterpart of each
    ///     provider's <c>ToDDL(Table)</c>.
    /// </summary>
    /// <remarks>
    ///     Always a single statement that is safe to embed in a migration script: never the
    ///     multi-statement or concurrent form, and never carrying Weasel's own marker comments.
    ///     <paramref name="parent" /> must be the provider's own table type -- the same table this
    ///     index belongs to.
    /// </remarks>
    string ToDDL(ITable parent);
}
