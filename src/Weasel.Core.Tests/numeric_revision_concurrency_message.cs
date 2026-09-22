using System;
using JasperFx;
using Shouldly;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     weasel#595. The numeric closed-shape operations are the single throw site for a failed
///     revision guard across Marten, Polecat and Fisher, and they already know the document uses
///     numeric revisions. The "call UpdateRevision() instead" hint therefore has to be on the
///     message unconditionally -- <see cref="ConcurrencyException.ToMessage" /> can only add it
///     for <c>IRevisioned</c> / <c>ILongVersioned</c> documents, so a document that opts into
///     revisions through configuration instead of an interface used to get no hint at all.
/// </summary>
public class numeric_revision_concurrency_message
{
    private class RevisionedDoc: IRevisioned
    {
        public int Version { get; set; }
    }

    private class ConfiguredDoc
    {
        public int Revision { get; set; }
    }

    [Fact]
    public void carries_the_update_revision_hint_for_a_document_with_no_revision_interface()
    {
        var message = NumericRevisionConcurrency.ToMessage(typeof(ConfiguredDoc), 5);

        message.ShouldContain("UpdateRevision");
        message.ShouldContain("reset the revision to 0");

        // ... which is exactly what the two-argument ConcurrencyException constructor cannot do
        // for this document, and why the numeric operations no longer use it.
        ConcurrencyException.ToMessage(typeof(ConfiguredDoc), 5)
            .ShouldNotContain("UpdateRevision");
    }

    [Fact]
    public void carries_the_same_hint_for_an_IRevisioned_document()
    {
        NumericRevisionConcurrency.ToMessage(typeof(RevisionedDoc), 5)
            .ShouldBe(NumericRevisionConcurrency.ToMessage(typeof(ConfiguredDoc), 5)
                .Replace(typeof(ConfiguredDoc).FullName!, typeof(RevisionedDoc).FullName!));
    }

    [Fact]
    public void keeps_the_opening_that_ConcurrencyException_uses()
    {
        NumericRevisionConcurrency.ToMessage(typeof(ConfiguredDoc), 5)
            .ShouldStartWith($"Optimistic concurrency check failed for {typeof(ConfiguredDoc).FullName} #5");
    }

    [Fact]
    public void the_exception_still_carries_the_doc_type_and_id()
    {
        var ex = NumericRevisionConcurrency.ExceptionFor(typeof(ConfiguredDoc), 5);

        ex.DocType.ShouldBe(typeof(ConfiguredDoc).FullName);
        ex.Id.ShouldBe(5);
        ex.Message.ShouldBe(NumericRevisionConcurrency.ToMessage(typeof(ConfiguredDoc), 5));
    }
}
