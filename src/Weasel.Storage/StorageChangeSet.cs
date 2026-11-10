#nullable enable
using JasperFx.Events.Documents;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
///     An immutable <see cref="IDocumentChangeSet" /> derived from the storage operations a unit of work
///     committed: which documents it inserted, which it updated, and which it deleted.
/// </summary>
/// <remarks>
///     <para>
///         <b>Materialized once, in the constructor.</b> The contract promises snapshots, because a
///         listener may keep the change set past the commit boundary and the list it was built from may
///         be reset or appended to afterwards. Partitioning in one pass also means a listener that reads
///         all three lists does not walk the operations three times.
///     </para>
///     <para>
///         <b>Deletions are recognized by type before role.</b> A by-id deletion
///         (<see cref="IDeletion" />) carries its identity; a soft delete is an <c>UPDATE</c> whose role is
///         still <see cref="OperationRole.Deletion" />, so testing the role first would report every by-id
///         deletion without its id. An operation whose role is <see cref="OperationRole.Deletion" /> but
///         which is not an <see cref="IDeletion" /> — a predicate delete — is reported with a
///         <see langword="null" /> id, which is better than leaving it invisible to a listener.
///     </para>
///     <para>
///         A store whose own operations surface the document or the id some other way passes
///         <c>documentOf</c> / <c>identityOf</c>; they are consulted only for an operation that is not an
///         <see cref="IDocumentStorageOperation" /> or an <see cref="IDeletion" /> respectively.
///     </para>
///     <para>
///         Events are not part of this type. A store's own change set, which adds its streams and events,
///         can wrap or derive from it.
///     </para>
/// </remarks>
public class StorageChangeSet: IDocumentChangeSet
{
    private readonly IReadOnlyList<object> _inserted;
    private readonly IReadOnlyList<object> _updated;
    private readonly IReadOnlyList<IDocumentDeletion> _deleted;

    /// <param name="operations">The operations the unit of work committed, in order.</param>
    public StorageChangeSet(IEnumerable<IStorageOperation> operations): this(operations, null, null)
    {
    }

    /// <param name="operations">The operations the unit of work committed, in order.</param>
    /// <param name="documentOf">
    ///     The document an insert/update/upsert operation wrote, for an operation that is not an
    ///     <see cref="IDocumentStorageOperation" />. Returning <see langword="null" /> leaves it out.
    /// </param>
    /// <param name="identityOf">
    ///     The id a deletion removed, for an operation that is not an <see cref="IDeletion" />.
    /// </param>
    public StorageChangeSet(IEnumerable<IStorageOperation> operations,
        Func<IStorageOperation, object?>? documentOf,
        Func<IStorageOperation, object?>? identityOf)
    {
        ArgumentNullException.ThrowIfNull(operations);

        List<object>? inserted = null;
        List<object>? updated = null;
        List<IDocumentDeletion>? deleted = null;

        foreach (var operation in operations)
        {
            if (operation is IDeletion deletion)
            {
                (deleted ??= []).Add(new StorageDocumentDeletion(operation.DocumentType, deletion.Id));
                continue;
            }

            switch (operation.Role())
            {
                case OperationRole.Insert:
                    if (documentFor(operation, documentOf) is { } insertedDocument)
                    {
                        (inserted ??= []).Add(insertedDocument);
                    }

                    break;

                case OperationRole.Update or OperationRole.Upsert:
                    if (documentFor(operation, documentOf) is { } updatedDocument)
                    {
                        (updated ??= []).Add(updatedDocument);
                    }

                    break;

                case OperationRole.Deletion:
                    (deleted ??= []).Add(new StorageDocumentDeletion(operation.DocumentType,
                        identityOf?.Invoke(operation)));
                    break;
            }
        }

        // Empty rather than null, per IDocumentChangeSet, and a shared empty array so the common
        // "this commit deleted nothing" costs no allocation.
        _inserted = inserted ?? (IReadOnlyList<object>)Array.Empty<object>();
        _updated = updated ?? (IReadOnlyList<object>)Array.Empty<object>();
        _deleted = deleted ?? (IReadOnlyList<IDocumentDeletion>)Array.Empty<IDocumentDeletion>();
    }

    /// <inheritdoc />
    public IReadOnlyList<object> Inserted => _inserted;

    /// <inheritdoc />
    public IReadOnlyList<object> Updated => _updated;

    /// <inheritdoc />
    public IReadOnlyList<IDocumentDeletion> Deleted => _deleted;

    private static object? documentFor(IStorageOperation operation, Func<IStorageOperation, object?>? documentOf)
        => operation is IDocumentStorageOperation storage ? storage.Document : documentOf?.Invoke(operation);
}

/// <summary>
///     The <see cref="IDocumentDeletion" /> a <see cref="StorageChangeSet" /> reports: the deleted
///     document's type and, when the deletion named one, its id.
/// </summary>
/// <param name="DocumentType">The deleted document's type.</param>
/// <param name="Id">The deleted document's id, or <see langword="null" /> for a predicate delete.</param>
public sealed record StorageDocumentDeletion(Type DocumentType, object? Id): IDocumentDeletion;
