#nullable enable
using Weasel.Core.SqlGeneration;

namespace Weasel.Storage;

/// <summary>
///     What differs from store to store about a hierarchy sub-class's storage, supplied to
///     <see cref="ClosedShapeSubClassDocumentStorage{TDoc,TRoot,TId}" /> (weasel#811). Everything else a
///     sub-class storage does is delegated to its root's storage, which owns the table, the descriptor
///     and the hierarchical selectors.
/// </summary>
/// <remarks>
///     <para>
///         <b>Queries are narrowed in SQL; loads by id are not.</b> A load names one row, and the id is
///         unique across the hierarchy, so a sub-class storage loads through its root and tests the type
///         of what comes back. Adding the discriminator to the load would make "that id is a sibling
///         sub-class" indistinguishable from "no such id". The members here only shape queries and
///         truncation.
///     </para>
///     <para>
///         Members with a default body are optional, and any member added later will carry one too.
///     </para>
/// </remarks>
/// <typeparam name="TDoc">The sub-class.</typeparam>
/// <typeparam name="TRoot">The hierarchy's root document type.</typeparam>
/// <typeparam name="TId">The hierarchy's id type.</typeparam>
public interface IClosedShapeSubClassRules<TDoc, TRoot, TId>
    where TDoc : notnull, TRoot
    where TRoot : notnull
    where TId : notnull
{
    /// <summary>
    ///     The predicate that narrows the root's table to <typeparamref name="TDoc" /> and every
    ///     sub-class registered below it, e.g. <c>d.doc_type in ('cat', 'lion')</c>. Read once, when the
    ///     storage is built.
    /// </summary>
    ISqlFragment DocumentTypeFilter { get; }

    /// <summary>
    ///     The filters a query against <typeparamref name="TDoc" /> carries. Default: the root's
    ///     filters (soft-delete, tenant) and <see cref="DocumentTypeFilter" />, joined with <c>and</c>.
    /// </summary>
    /// <remarks>
    ///     A store whose query layer adds the discriminator once per statement itself returns
    ///     <c>root.FilterDocuments(query, session)</c> here, so the filter is not written twice.
    /// </remarks>
    /// <param name="root">The hierarchy root's storage.</param>
    /// <param name="query">The caller's predicate.</param>
    /// <param name="session">The querying session.</param>
    ISqlFragment FilterDocuments(IDocumentStorage<TRoot, TId> root, ISqlFragment query, IStorageSession session)
        => ClosedShapeAndFragment.Of(root.FilterDocuments(query, session), DocumentTypeFilter);

    /// <summary>
    ///     The filters every query against <typeparamref name="TDoc" /> carries by default. Default: the
    ///     root's default filter and <see cref="DocumentTypeFilter" />, or the type filter alone when the
    ///     root has none. Called once, when the storage is built.
    /// </summary>
    /// <param name="root">The hierarchy root's storage.</param>
    ISqlFragment? DefaultWhereFragment(IDocumentStorage<TRoot, TId> root)
        => root.DefaultWhereFragment() is { } rootWhere
            ? ClosedShapeAndFragment.Of(rootWhere, DocumentTypeFilter)
            : DocumentTypeFilter;

    /// <summary>
    ///     The statement that removes this sub-class's rows, e.g.
    ///     <c>delete from docs where doc_type = 'cat'</c>; or null to truncate the root's whole table,
    ///     which is what a projection rebuild replaying into a hierarchy needs. Default: null.
    /// </summary>
    /// <remarks>
    ///     The default is the wider of the two because Weasel cannot write the narrow one: the
    ///     discriminator column and its literal quoting belong to the store and its dialect. A store
    ///     whose callers expect a sub-class truncate to leave sibling rows alone must answer this.
    /// </remarks>
    string? TruncateSql => null;
}
