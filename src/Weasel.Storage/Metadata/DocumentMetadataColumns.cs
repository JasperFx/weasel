#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace Weasel.Storage.Metadata;

/// <summary>
/// weasel#745: the full set of metadata columns a document type can carry, store-neutral. It is the
/// union of what Marten (<c>DocumentMetadataCollection</c>), Polecat (<c>DocumentMetadataConfig</c>)
/// and Fisher (<c>DocumentMetadata</c>) each kept: Marten's numeric <see cref="Revision"/> and
/// <see cref="DocumentType"/> columns as well as the columns all three share.
/// </summary>
/// <remarks>
/// <para>
/// Every column starts <b>disabled</b>, under a plain default name. The stores do not agree on
/// either. Polecat's tables always carry <c>created_at</c>, <c>last_modified</c> and
/// <c>dotnet_type</c>, while Marten can turn its informational columns off. Marten prefixes its own
/// columns with <c>mt_</c>. So the owning store enables what its table carries and renames what it
/// names differently, once, when it creates the set. A neutral default that matched one store would
/// be silently wrong for the other two.
/// </para>
/// <para>
/// The user-facing DSL over this set is <see cref="MetadataConfig{T}"/>. The plan that turns it into
/// binder arrays is <see cref="DocumentBinderPlan{TDoc}"/>.
/// </para>
/// </remarks>
public sealed class DocumentMetadataColumns
{
    /// <summary>The Guid optimistic-concurrency version. Default name <c>guid_version</c>.</summary>
    public DocumentMetadataColumn Version { get; } = new("guid_version");

    /// <summary>The monotonic numeric revision. Default name <c>revision</c>.</summary>
    public DocumentMetadataColumn Revision { get; } = new("revision");

    /// <summary>Timestamp of the last write. Default name <c>last_modified</c>.</summary>
    public DocumentMetadataColumn LastModified { get; } = new("last_modified");

    /// <summary>Timestamp of the first write. Default name <c>created_at</c>.</summary>
    public DocumentMetadataColumn CreatedAt { get; } = new("created_at");

    /// <summary>The stored tenant id. Default name <c>tenant_id</c>.</summary>
    public DocumentMetadataColumn TenantId { get; } = new("tenant_id");

    /// <summary>The soft-delete flag. Default name <c>is_deleted</c>.</summary>
    public DocumentMetadataColumn IsSoftDeleted { get; } = new("is_deleted");

    /// <summary>The soft-delete timestamp. Default name <c>deleted_at</c>.</summary>
    public DocumentMetadataColumn SoftDeletedAt { get; } = new("deleted_at");

    /// <summary>The hierarchy discriminator (a sub-class alias). Default name <c>doc_type</c>.</summary>
    public DocumentMetadataColumn DocumentType { get; } = new("doc_type");

    /// <summary>The full name of the persisted .NET type. Default name <c>dotnet_type</c>.</summary>
    public DocumentMetadataColumn DotNetType { get; } = new("dotnet_type");

    /// <summary>The session's correlation id. Default name <c>correlation_id</c>.</summary>
    public DocumentMetadataColumn CorrelationId { get; } = new("correlation_id");

    /// <summary>The session's causation id. Default name <c>causation_id</c>.</summary>
    public DocumentMetadataColumn CausationId { get; } = new("causation_id");

    /// <summary>The session's user or process name. Default name <c>last_modified_by</c>.</summary>
    public DocumentMetadataColumn LastModifiedBy { get; } = new("last_modified_by");

    /// <summary>The session's custom headers. Default name <c>headers</c>.</summary>
    public DocumentMetadataColumn Headers { get; } = new("headers");

    /// <summary>
    /// Every column, in a fixed order. This is <b>not</b> a SELECT order: that is whatever order the
    /// store adds binders to its <see cref="DocumentBinderPlan{TDoc}"/> in.
    /// </summary>
    public IEnumerable<DocumentMetadataColumn> AllColumns()
    {
        yield return Version;
        yield return Revision;
        yield return LastModified;
        yield return CreatedAt;
        yield return TenantId;
        yield return IsSoftDeleted;
        yield return SoftDeletedAt;
        yield return DocumentType;
        yield return DotNetType;
        yield return CorrelationId;
        yield return CausationId;
        yield return LastModifiedBy;
        yield return Headers;
    }

    /// <summary>
    /// The four columns whose values come off the session rather than the document: correlation id,
    /// causation id, last-modified-by and headers.
    /// </summary>
    public IEnumerable<DocumentMetadataColumn> SessionColumns()
    {
        yield return CorrelationId;
        yield return CausationId;
        yield return LastModifiedBy;
        yield return Headers;
    }

    /// <summary>
    /// The enabled columns, in <see cref="AllColumns"/> order.
    /// </summary>
    public IEnumerable<DocumentMetadataColumn> EnabledColumns() => AllColumns().Where(x => x.Enabled);

    /// <summary>
    /// Layer another set over this one — for example the fluent DSL's configuration over what the
    /// metadata attributes produced. Column by column, as <see cref="DocumentMetadataColumn.MergeFrom"/>:
    /// enables what the other enabled and takes the member mappings it set. Names are not copied.
    /// </summary>
    public void MergeFrom(DocumentMetadataColumns other)
    {
        ArgumentNullException.ThrowIfNull(other);

        using var mine = AllColumns().GetEnumerator();
        using var theirs = other.AllColumns().GetEnumerator();
        while (mine.MoveNext() && theirs.MoveNext())
        {
            mine.Current.MergeFrom(theirs.Current);
        }
    }

    /// <summary>
    /// Refuse the one combination no store can build: a Guid version and a numeric revision on the
    /// same document. Marten stores both in the same physical column, and every store's operations
    /// pick exactly one concurrency mode.
    /// </summary>
    public void AssertValid(Type documentType)
    {
        if (Version.Enabled && Revision.Enabled)
        {
            throw new InvalidOperationException(
                $"Document type {documentType.FullName} enables both a Guid version ('{Version.Name}') and a numeric revision ('{Revision.Name}'). Use one or the other.");
        }
    }
}
