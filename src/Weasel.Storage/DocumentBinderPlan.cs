#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Weasel.Storage.Metadata;

namespace Weasel.Storage;

/// <summary>
/// weasel#745: the ordered plan of a document type's metadata binders. It holds what each store's
/// descriptor builder used to assemble by hand: the write and read binder arrays, the
/// version/revision and doc-type read indexes, and the narrower QueryOnly read set.
/// </summary>
/// <remarks>
/// <para>
/// <b>Binder order sets the SELECT ordinals.</b> A selector reads metadata column <c>i</c> at
/// ordinal <c>FirstMetadataColumn + i</c>, where <c>i</c> is the binder's position in
/// <see cref="ReadBinders"/> (or <see cref="QueryOnlyReadBinders"/>). So the SELECT's column list
/// must be built from the same array, in the same order. <see cref="SelectColumns"/> and
/// <see cref="QueryOnlySelectColumns"/> build it here, from the plan itself, so a store cannot
/// append to one side without the other.
/// </para>
/// <para>
/// The plan does <b>not</b> choose an order. Marten, Polecat and Fisher order their columns
/// differently (Marten reads <c>tenant_id</c> before <c>last_modified</c>, Fisher reads it last),
/// and each store's existing tables and queries depend on its own order. The plan records the order
/// the store adds binders in and derives everything else from that.
/// </para>
/// <para>
/// The write side follows the same rule: <see cref="WriteBinders"/> is the INSERT column order, and
/// <see cref="ClientSideWriteBinders"/> is the order of the <c>?</c> parameter slots.
/// </para>
/// </remarks>
public sealed class DocumentBinderPlan<TDoc>
    where TDoc : notnull
{
    private readonly List<IDocumentMetadataBinder<TDoc>> _write = new();
    private readonly List<IDocumentMetadataBinder<TDoc>> _read = new();

    private IDocumentMetadataBinder<TDoc>? _docTypeBinder;
    private IVersionMetadataBinder<TDoc>? _versionBinder;
    private IRevisionMetadataBinder<TDoc>? _revisionBinder;
    private bool _versionReadByQueryOnly = true;

    private Arrays? _arrays;

    // ---- composing the plan ----

    /// <summary>
    /// Add a binder that writes its column and is never read back: <c>dotnet_type</c>, a duplicated
    /// field, or a column whose member mapping is absent.
    /// </summary>
    public DocumentBinderPlan<TDoc> Write(IDocumentMetadataBinder<TDoc> binder)
    {
        addWrite(binder);
        return this;
    }

    /// <summary>
    /// Add a binder that is read back and never written by the binder loop. Examples are
    /// <c>created_at</c>, which a column DEFAULT fills and a write would move forward, and
    /// <c>tenant_id</c>, which the operations bind inline ahead of the binder loop.
    /// </summary>
    public DocumentBinderPlan<TDoc> Read(IDocumentMetadataBinder<TDoc> binder)
    {
        addRead(binder);
        return this;
    }

    /// <summary>
    /// Add a binder that is written and read back.
    /// </summary>
    public DocumentBinderPlan<TDoc> WriteAndRead(IDocumentMetadataBinder<TDoc> binder)
    {
        addWrite(binder);
        addRead(binder);
        return this;
    }

    /// <summary>
    /// Add a binder that is written, and read back only when <paramref name="column"/> maps onto a
    /// document member. An unmapped binder has nothing to write the value to, so it would cost a
    /// SELECT column to do nothing. This is the rule every store applies to its ordinary metadata
    /// columns.
    /// </summary>
    public DocumentBinderPlan<TDoc> WriteAndReadIfMapped(IDocumentMetadataBinder<TDoc> binder,
        DocumentMetadataColumn column)
    {
        ArgumentNullException.ThrowIfNull(column);

        addWrite(binder);
        if (column.Member is not null)
        {
            addRead(binder);
        }

        return this;
    }

    /// <summary>
    /// Add a read-only binder, and only when <paramref name="column"/> maps onto a document member.
    /// </summary>
    public DocumentBinderPlan<TDoc> ReadIfMapped(IDocumentMetadataBinder<TDoc> binder, DocumentMetadataColumn column)
    {
        ArgumentNullException.ThrowIfNull(column);
        ArgumentNullException.ThrowIfNull(binder);

        if (column.Member is not null)
        {
            addRead(binder);
        }

        return this;
    }

    /// <summary>
    /// When <paramref name="column"/> is enabled, build its binder from the column's name and member,
    /// write it, and read it back if it is mapped. A disabled column adds nothing.
    /// </summary>
    public DocumentBinderPlan<TDoc> AddIfEnabled(DocumentMetadataColumn column,
        Func<string, MemberInfo?, IDocumentMetadataBinder<TDoc>> create)
    {
        ArgumentNullException.ThrowIfNull(column);
        ArgumentNullException.ThrowIfNull(create);

        if (!column.Enabled)
        {
            return this;
        }

        return WriteAndReadIfMapped(create(column.Name, column.Member), column);
    }

    /// <summary>
    /// Add the hierarchy discriminator. It is always read, because the hierarchical selectors
    /// dispatch deserialization on it, and its position becomes <see cref="DocTypeReadIndex"/>.
    /// </summary>
    /// <param name="binder">The discriminator binder.</param>
    /// <param name="write">Whether the binder loop writes it. Every store writes it today.</param>
    public DocumentBinderPlan<TDoc> DocumentType(IDocumentMetadataBinder<TDoc> binder, bool write = true)
    {
        ArgumentNullException.ThrowIfNull(binder);
        if (_docTypeBinder is not null)
        {
            throw new InvalidOperationException(
                $"{typeof(TDoc).FullName} already has a document-type binder ('{_docTypeBinder.ColumnName}').");
        }

        if (write)
        {
            addWrite(binder);
        }

        addRead(binder);
        _docTypeBinder = binder;
        return this;
    }

    /// <summary>
    /// Add the Guid version binder. It is always written.
    /// </summary>
    /// <param name="binder">The version binder.</param>
    /// <param name="read">
    /// Whether the full (non-QueryOnly) read set carries it. Marten reads it when it is mapped or
    /// optimistic concurrency is on; Polecat and Fisher only add it under optimistic concurrency
    /// and then always read it.
    /// </param>
    /// <param name="readByQueryOnly">
    /// Whether the QueryOnly read set keeps it too. QueryOnly never writes, so it has no use for a
    /// version it cannot act on unless the version is mapped onto a member. The stores do not agree
    /// on how to tell, which is why the caller says rather than the plan inferring it from a member.
    /// Ignored when <paramref name="read"/> is false.
    /// </param>
    public DocumentBinderPlan<TDoc> Version(IVersionMetadataBinder<TDoc> binder, bool read, bool readByQueryOnly)
    {
        ArgumentNullException.ThrowIfNull(binder);
        assertNoConcurrencyBinderYet(binder);

        addWrite(binder);
        if (read)
        {
            addRead(binder);
            _versionReadByQueryOnly = readByQueryOnly;
        }

        _versionBinder = binder;
        return this;
    }

    /// <summary>
    /// Add the numeric revision binder. It is always written. The parameters mean what they mean
    /// for <see cref="Version"/>. Fisher keeps an unmapped revision in the QueryOnly read set, and
    /// Marten and Polecat drop it.
    /// </summary>
    public DocumentBinderPlan<TDoc> Revision(IRevisionMetadataBinder<TDoc> binder, bool read, bool readByQueryOnly)
    {
        ArgumentNullException.ThrowIfNull(binder);
        assertNoConcurrencyBinderYet(binder);

        addWrite(binder);
        if (read)
        {
            addRead(binder);
            _versionReadByQueryOnly = readByQueryOnly;
        }

        _revisionBinder = binder;
        return this;
    }

    // ---- what the plan produces ----

    /// <summary>
    /// Every write binder, client-side and server-side, in INSERT column order.
    /// </summary>
    public IDocumentMetadataBinder<TDoc>[] WriteBinders => arrays().Write;

    /// <summary>
    /// The write binders that fill a <c>?</c> slot, in parameter order. The server-side ones have
    /// their SQL baked into the statement instead.
    /// </summary>
    public IDocumentMetadataBinder<TDoc>[] ClientSideWriteBinders => arrays().ClientSide;

    /// <summary>
    /// The read binders, in SELECT order. Binder <c>i</c> reads ordinal
    /// <c>FirstMetadataColumn + i</c>.
    /// </summary>
    public IDocumentMetadataBinder<TDoc>[] ReadBinders => arrays().Read;

    /// <summary>
    /// The QueryOnly read set: <see cref="ReadBinders"/> without the version or revision binder when
    /// that was added with <c>readByQueryOnly: false</c>. It is the <b>same array instance</b> as
    /// <see cref="ReadBinders"/> when nothing is dropped.
    /// </summary>
    public IDocumentMetadataBinder<TDoc>[] QueryOnlyReadBinders => arrays().QueryOnlyRead;

    /// <summary>The Guid version binder, if one was added.</summary>
    public IVersionMetadataBinder<TDoc>? VersionBinder => _versionBinder;

    /// <summary>The numeric revision binder, if one was added.</summary>
    public IRevisionMetadataBinder<TDoc>? RevisionBinder => _revisionBinder;

    /// <summary>
    /// The version or revision binder's index in <see cref="ReadBinders"/>, or <c>-1</c> when it is
    /// not read.
    /// </summary>
    public int VersionReadIndex => arrays().VersionReadIndex;

    /// <summary>
    /// The document-type binder's index in <see cref="ReadBinders"/>, or <c>-1</c> when the mapping
    /// is not hierarchical. The hierarchical QueryOnly selector uses the same index against
    /// <see cref="QueryOnlyReadBinders"/>, so the plan refuses any order in which dropping the
    /// version would move it. See <see cref="QueryOnlyReadBinders"/>.
    /// </summary>
    public int DocTypeReadIndex => arrays().DocTypeReadIndex;

    /// <summary>
    /// The SELECT column list for the full read set: <paramref name="leadingColumns"/> (for example
    /// <c>id</c> and <c>data</c>), then each read binder's column in order.
    /// <c>FirstMetadataColumn</c> is <c>leadingColumns.Length</c>.
    /// </summary>
    public string[] SelectColumns(params string[] leadingColumns) => columns(leadingColumns, ReadBinders);

    /// <summary>
    /// The SELECT column list for the QueryOnly read set, built the same way as
    /// <see cref="SelectColumns"/>.
    /// </summary>
    public string[] QueryOnlySelectColumns(params string[] leadingColumns) =>
        columns(leadingColumns, QueryOnlyReadBinders);

    /// <summary>
    /// The reader ordinal <paramref name="binder"/> is read at, given the number of columns ahead of
    /// the metadata columns. Returns <c>-1</c> when the binder is not in that read set.
    /// </summary>
    public int OrdinalOf(IDocumentMetadataBinder<TDoc> binder, int leadingColumnCount, bool queryOnly = false)
    {
        ArgumentNullException.ThrowIfNull(binder);
        if (leadingColumnCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(leadingColumnCount));
        }

        var index = Array.IndexOf(queryOnly ? QueryOnlyReadBinders : ReadBinders, binder);
        return index < 0 ? -1 : leadingColumnCount + index;
    }

    // ---- internals ----

    private void addWrite(IDocumentMetadataBinder<TDoc> binder)
    {
        ArgumentNullException.ThrowIfNull(binder);
        assertNotAlreadyIn(_write, binder, "write");
        _write.Add(binder);
        _arrays = null;
    }

    private void addRead(IDocumentMetadataBinder<TDoc> binder)
    {
        ArgumentNullException.ThrowIfNull(binder);
        assertNotAlreadyIn(_read, binder, "read");
        _read.Add(binder);
        _arrays = null;
    }

    private static void assertNotAlreadyIn(List<IDocumentMetadataBinder<TDoc>> list,
        IDocumentMetadataBinder<TDoc> binder, string side)
    {
        // Same instance or same column name: either way the statement would carry the column twice,
        // and on the read side every ordinal after it would be one off.
        var existing = list.FirstOrDefault(x =>
            ReferenceEquals(x, binder) ||
            string.Equals(x.ColumnName, binder.ColumnName, StringComparison.OrdinalIgnoreCase));

        if (existing is not null)
        {
            throw new InvalidOperationException(
                $"{typeof(TDoc).FullName}: the {side} set already has a binder for column '{existing.ColumnName}'.");
        }
    }

    private void assertNoConcurrencyBinderYet(IDocumentMetadataBinder<TDoc> binder)
    {
        var existing = (IDocumentMetadataBinder<TDoc>?)_versionBinder ?? _revisionBinder;
        if (existing is not null)
        {
            throw new InvalidOperationException(
                $"{typeof(TDoc).FullName} already has a concurrency binder ('{existing.ColumnName}'); cannot add '{binder.ColumnName}'. A document uses a Guid version or a numeric revision, not both.");
        }
    }

    private static string[] columns(string[] leadingColumns, IDocumentMetadataBinder<TDoc>[] binders)
    {
        ArgumentNullException.ThrowIfNull(leadingColumns);

        var result = new string[leadingColumns.Length + binders.Length];
        leadingColumns.CopyTo(result, 0);
        for (var i = 0; i < binders.Length; i++)
        {
            result[leadingColumns.Length + i] = binders[i].ColumnName;
        }

        return result;
    }

    private Arrays arrays() => _arrays ??= build();

    private Arrays build()
    {
        var write = _write.ToArray();
        var read = _read.ToArray();
        var clientSide = write.Where(x => !x.IsServerSide).ToArray();

        var concurrency = (IDocumentMetadataBinder<TDoc>?)_versionBinder ?? _revisionBinder;
        var versionReadIndex = concurrency is null ? -1 : Array.IndexOf(read, concurrency);
        var docTypeReadIndex = _docTypeBinder is null ? -1 : Array.IndexOf(read, _docTypeBinder);

        var queryOnlyRead = read;
        if (versionReadIndex >= 0 && !_versionReadByQueryOnly)
        {
            // DocTypeReadIndex is shared by both read sets, so the doc type must sit ahead of the
            // version for dropping the version to leave it where it was.
            if (docTypeReadIndex > versionReadIndex)
            {
                throw new InvalidOperationException(
                    $"{typeof(TDoc).FullName}: the document-type binder ('{_docTypeBinder!.ColumnName}') is read after the version binder ('{concurrency!.ColumnName}'), which the QueryOnly read set drops. Add the document type first, so its read index is the same in both sets.");
            }

            queryOnlyRead = read.Where((_, i) => i != versionReadIndex).ToArray();
        }

        return new Arrays(write, clientSide, read, queryOnlyRead, versionReadIndex, docTypeReadIndex);
    }

    private sealed record Arrays(
        IDocumentMetadataBinder<TDoc>[] Write,
        IDocumentMetadataBinder<TDoc>[] ClientSide,
        IDocumentMetadataBinder<TDoc>[] Read,
        IDocumentMetadataBinder<TDoc>[] QueryOnlyRead,
        int VersionReadIndex,
        int DocTypeReadIndex);
}
