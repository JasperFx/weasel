#nullable enable
using System;
using JasperFx;

namespace Weasel.Storage;

/// <summary>
/// The single message source for a failed numeric-revision guard. The numeric closed-shape
/// operations know the document uses numeric revisions -- that is what makes them the numeric
/// operations -- so the "you probably wanted UpdateRevision()" hint always applies, no matter
/// how the document opted in. <see cref="ConcurrencyException.ToMessage"/> can only add that
/// hint for <c>IRevisioned</c> / <c>ILongVersioned</c> documents, which would make the hint
/// depend on which of several equivalent configuration styles a user picked.
/// </summary>
public static class NumericRevisionConcurrency
{
    /// <summary>
    /// The concurrency failure message for a document mapped with numeric revisions. Keeps the
    /// "Optimistic concurrency check failed for {type} #{id}" opening that
    /// <see cref="ConcurrencyException.ToMessage"/> uses, then names the remedy.
    /// </summary>
    public static string ToMessage(Type documentType, object id)
    {
        return
            $"Optimistic concurrency check failed for {documentType.FullName} #{id}. This document uses numeric revisions: the store uses the document's current revision value as the expected revision on Store(). If you are re-storing a document that still carries the revision it was loaded with, call IDocumentSession.UpdateRevision(document, expectedRevision) with the next revision instead, or reset the revision to 0 to let the store assign it.";
    }

    /// <summary>
    /// Build the <see cref="ConcurrencyException"/> for a failed numeric-revision guard, with
    /// <see cref="ConcurrencyException.DocType"/> and <see cref="ConcurrencyException.Id"/>
    /// still filled in.
    /// </summary>
    public static ConcurrencyException ExceptionFor(Type documentType, object id)
    {
        return new ConcurrencyException(ToMessage(documentType, id), documentType, id);
    }
}
