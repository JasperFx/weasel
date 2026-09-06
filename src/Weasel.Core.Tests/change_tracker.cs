using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using Shouldly;
using Weasel.Core.Sequences;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
/// <see cref="ChangeTracker{T}" />, and in particular the <c>JsonNode.DeepEquals</c> fallback that
/// weasel#577 moved behind <see cref="IStorageSession.UseSemanticJsonChangeDetection" />.
/// </summary>
/// <remarks>
///     <para>
///     The tracker's contract is "did this document change since it was loaded", and its hazard is
///     asymmetric: a false negative silently drops the user's edit, while a false positive only
///     writes a row that did not need writing. So every genuine-change case here runs under
///     <b>both</b> settings of the flag — the point of the change is that turning the fallback off
///     cannot lose a write, and the point of keeping the fallback available is that turning it on
///     cannot swallow one.
///     </para>
///     <para>
///     Weasel.Storage has no test project of its own; this lives here for the same reason the
///     flat-table and event-loader suites do. Pure in-memory serialization — no database.
///     </para>
/// </remarks>
public class change_tracker
{
    private readonly StubStorageSession theSession = new();

    private static TrackedDoc doc() => new();

    // ------------------------------------------------------------------------------------------
    // The unchanged path
    // ------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void an_untouched_document_is_not_dirty(bool semantic)
    {
        theSession.SemanticFallback = semantic;
        var tracker = new ChangeTracker<TrackedDoc>(theSession, doc());

        tracker.DetectChanges(theSession, out var operation).ShouldBeFalse();
        operation.ShouldBeNull();
    }

    /// <summary>
    /// A session that says nothing about the flag gets the fast path. Read through a session that
    /// does not implement the member at all, so this exercises the default interface
    /// implementation rather than a stub's field initializer.
    /// </summary>
    [Fact]
    public void the_semantic_fallback_is_off_unless_a_session_opts_in()
    {
        IStorageSession silent = new SilentStorageSession();

        silent.UseSemanticJsonChangeDetection.ShouldBeFalse();
    }

    // ------------------------------------------------------------------------------------------
    // Genuine changes, under both settings of the flag. These are the cases the DeepEquals path
    // was covering, and none of them may go quiet in either configuration.
    // ------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(false, "scalar")]
    [InlineData(false, "number")]
    [InlineData(false, "nested")]
    [InlineData(false, "list-element")]
    [InlineData(false, "list-append")]
    [InlineData(false, "list-removal")]
    [InlineData(false, "list-reorder")]
    [InlineData(false, "dictionary-value")]
    [InlineData(false, "dictionary-key-added")]
    [InlineData(false, "dictionary-key-removed")]
    [InlineData(false, "dictionary-value-changed-in-place-with-a-reorder")]
    [InlineData(false, "null-out-a-member")]
    [InlineData(true, "scalar")]
    [InlineData(true, "number")]
    [InlineData(true, "nested")]
    [InlineData(true, "list-element")]
    [InlineData(true, "list-append")]
    [InlineData(true, "list-removal")]
    [InlineData(true, "list-reorder")]
    [InlineData(true, "dictionary-value")]
    [InlineData(true, "dictionary-key-added")]
    [InlineData(true, "dictionary-key-removed")]
    [InlineData(true, "dictionary-value-changed-in-place-with-a-reorder")]
    [InlineData(true, "null-out-a-member")]
    public void a_genuine_change_is_detected(bool semantic, string change)
    {
        theSession.SemanticFallback = semantic;
        var document = doc();
        var tracker = new ChangeTracker<TrackedDoc>(theSession, document);

        applyChange(document, change);

        tracker.DetectChanges(theSession, out var operation).ShouldBeTrue();
        operation.ShouldBeSameAs(theSession.TheUpsert);
    }

    private static void applyChange(TrackedDoc document, string change)
    {
        switch (change)
        {
            case "scalar":
                document.Name = "changed";
                break;
            case "number":
                document.Count += 1;
                break;
            case "nested":
                document.Child.Value = "changed";
                break;
            case "list-element":
                document.Tags[1] = "changed";
                break;
            case "list-append":
                document.Tags.Add("d");
                break;
            case "list-removal":
                document.Tags.RemoveAt(0);
                break;
            case "list-reorder":
                // A JSON array's order *is* meaningful, so unlike a dictionary this must read as a
                // change even with the fallback on.
                document.Tags.Reverse();
                break;
            case "dictionary-value":
                document.Attributes["one"] = "changed";
                break;
            case "dictionary-key-added":
                document.Attributes["four"] = "4";
                break;
            case "dictionary-key-removed":
                document.Attributes.Remove("two");
                break;
            case "dictionary-value-changed-in-place-with-a-reorder":
                // Removing an entry and re-adding it moves it to the end, so here a reorder and a
                // real change arrive together and the fallback has to look past the one to see the
                // other.
                document.Attributes.Remove("one");
                document.Attributes["one"] = "a different value";
                break;
            case "null-out-a-member":
                document.Name = null;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(change), change, "Unknown change");
        }
    }

    // ------------------------------------------------------------------------------------------
    // The reordered-but-equal case: the one thing the flag actually changes
    // ------------------------------------------------------------------------------------------

    /// <summary>
    /// Re-inserting a dictionary's entries in a different order changes the JSON text without
    /// changing what the JSON means. It needs no custom converter and no second serializer
    /// instance, which is why the fallback is kept as an option rather than deleted outright.
    /// </summary>
    private static void reorderTheDictionary(TrackedDoc document)
    {
        var pairs = new List<KeyValuePair<string, string>>(document.Attributes);
        pairs.Reverse();
        document.Attributes.Clear();
        foreach (var pair in pairs)
        {
            document.Attributes[pair.Key] = pair.Value;
        }
    }

    /// <summary>
    /// Guards the premise the rest of this section rests on. If the serializer ever started
    /// emitting dictionary entries in a canonical order, the flag would have nothing left to do.
    /// </summary>
    [Fact]
    public void reordering_a_dictionary_really_does_change_the_json_text()
    {
        var document = doc();
        var before = theSession.Serializer.ToCleanJson(document);

        reorderTheDictionary(document);

        theSession.Serializer.ToCleanJson(document).ShouldNotBe(before);
    }

    [Fact]
    public void a_reordered_dictionary_reports_a_change_by_default()
    {
        var document = doc();
        var tracker = new ChangeTracker<TrackedDoc>(theSession, document);

        reorderTheDictionary(document);

        // The accepted cost of the default: a redundant write of semantically identical JSON.
        tracker.DetectChanges(theSession, out _).ShouldBeTrue();
    }

    [Fact]
    public void a_reordered_dictionary_is_not_a_change_when_the_session_opts_in()
    {
        theSession.SemanticFallback = true;
        var document = doc();
        var tracker = new ChangeTracker<TrackedDoc>(theSession, document);

        reorderTheDictionary(document);

        tracker.DetectChanges(theSession, out var operation).ShouldBeFalse();
        operation.ShouldBeNull();
    }

    // ------------------------------------------------------------------------------------------
    // Reset
    // ------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void reset_rebaselines_so_the_next_pass_sees_nothing(bool semantic)
    {
        theSession.SemanticFallback = semantic;
        var document = doc();
        var tracker = new ChangeTracker<TrackedDoc>(theSession, document);

        document.Name = "changed";
        tracker.DetectChanges(theSession, out _).ShouldBeTrue();

        tracker.Reset(theSession);

        tracker.DetectChanges(theSession, out _).ShouldBeFalse();
    }

    /// <summary>
    /// Why <see cref="ChangeTracker{T}.Reset" /> re-serializes instead of reusing the JSON
    /// <c>DetectChanges</c> already produced: the document is mutated in between. The write
    /// binders project the session's correlation / causation / last-modified-by / headers onto it
    /// before the body is serialized, and the concurrency operations write the new version or
    /// revision back onto it from postprocess — both after <c>DetectChanges</c> has run and before
    /// <c>Reset</c> does. A baseline captured at detection time would be stale, and the next
    /// <c>DetectChanges</c> would report a change nobody made.
    /// </summary>
    [Fact]
    public void reset_picks_up_a_mutation_made_after_detect_changes()
    {
        var document = doc();
        var tracker = new ChangeTracker<TrackedDoc>(theSession, document);

        document.Name = "changed";
        tracker.DetectChanges(theSession, out _).ShouldBeTrue();

        // Stands in for the version write-back and the ApplyToDocument write binders.
        document.Version = Guid.NewGuid();
        document.LastModifiedBy = "the session's user";

        tracker.Reset(theSession);

        tracker.DetectChanges(theSession, out _).ShouldBeFalse();
    }

    /// <summary>
    /// A tracker constructed now baselines on <c>ToCleanJson</c> of the document as it stands, so a
    /// reset tracker answering identically to a fresh one — on an unchanged document and then on a
    /// changed one — is the byte-identity claim, exercised rather than asserted against a private
    /// field.
    /// </summary>
    [Fact]
    public void reset_leaves_a_baseline_a_fresh_tracker_would_agree_with()
    {
        var document = doc();
        var tracker = new ChangeTracker<TrackedDoc>(theSession, document);

        document.Name = "changed";
        document.Attributes["four"] = "4";
        tracker.DetectChanges(theSession, out _).ShouldBeTrue();
        tracker.Reset(theSession);

        var fresh = new ChangeTracker<TrackedDoc>(theSession, document);

        tracker.DetectChanges(theSession, out _).ShouldBeFalse();
        fresh.DetectChanges(theSession, out _).ShouldBeFalse();

        document.Count = 99;

        tracker.DetectChanges(theSession, out _).ShouldBeTrue();
        fresh.DetectChanges(theSession, out _).ShouldBeTrue();
    }

    // ------------------------------------------------------------------------------------------
    // The concurrency routing sitting next to the code being changed
    // ------------------------------------------------------------------------------------------

    [Fact]
    public void a_versioned_document_routes_through_overwrite_when_concurrency_is_disabled()
    {
        theSession.Concurrency = ConcurrencyChecks.Disabled;
        theSession.TheStorage.UseOptimisticConcurrency.Returns(true);

        var document = doc();
        var tracker = new ChangeTracker<TrackedDoc>(theSession, document);
        document.Name = "changed";

        tracker.DetectChanges(theSession, out var operation).ShouldBeTrue();
        operation.ShouldBeSameAs(theSession.TheOverwrite);
    }

    [Fact]
    public void an_unversioned_document_still_upserts_when_concurrency_is_disabled()
    {
        theSession.Concurrency = ConcurrencyChecks.Disabled;

        var document = doc();
        var tracker = new ChangeTracker<TrackedDoc>(theSession, document);
        document.Name = "changed";

        tracker.DetectChanges(theSession, out var operation).ShouldBeTrue();
        operation.ShouldBeSameAs(theSession.TheUpsert);
    }

    // ------------------------------------------------------------------------------------------
    // Test doubles
    // ------------------------------------------------------------------------------------------

    public class TrackedDoc
    {
        public Guid Id { get; set; } = new("aaaaaaaa-0000-4000-8000-000000000001");
        public string? Name { get; set; } = "original";
        public int Count { get; set; } = 3;
        public Guid Version { get; set; }
        public string? LastModifiedBy { get; set; }
        public NestedValue Child { get; set; } = new();
        public List<string> Tags { get; set; } = ["a", "b", "c"];

        public Dictionary<string, string> Attributes { get; set; } = new()
        {
            ["one"] = "1", ["two"] = "2", ["three"] = "3"
        };
    }

    public class NestedValue
    {
        public string Value { get; set; } = "nested";
    }

    /// <summary>
    /// Implements <see cref="IStorageSession" /> without saying anything about
    /// <see cref="IStorageSession.UseSemanticJsonChangeDetection" />, so reading that member goes
    /// to the interface's default implementation.
    /// </summary>
    private sealed class SilentStorageSession: StubStorageSessionBase;

    /// <summary>
    /// Re-implements the interface so the fallback can be switched per test.
    /// </summary>
    private sealed class StubStorageSession: StubStorageSessionBase, IStorageSession
    {
        public bool SemanticFallback { get; set; }

        bool IStorageSession.UseSemanticJsonChangeDetection => SemanticFallback;
    }

    /// <summary>
    /// The least <see cref="IStorageSession" /> <see cref="ChangeTracker{T}" /> touches: a real
    /// serializer and a provider graph whose dirty-tracking storage hands back marker operations.
    /// Everything else throws, so a test cannot quietly start exercising something else.
    /// </summary>
    private abstract class StubStorageSessionBase: IStorageSession
    {
        // Weasel.Storage.IStorageOperation is spelled out here and below on purpose. This file
        // sits in namespace Weasel.Core.Tests, so the bare name IStorageOperation binds to
        // Weasel.Core.IStorageOperation from the enclosing namespace, which beats both the
        // `using Weasel.Storage` and any using-alias. Shortening it compiles and then fails at
        // run time with a substitute that cannot return the operation the tracker asked for.
        protected StubStorageSessionBase()
        {
            Weasel.Storage.IStorageOperation upsert = new MarkerOperation("upsert");
            Weasel.Storage.IStorageOperation overwrite = new MarkerOperation("overwrite");
            var storage = Substitute.For<IDocumentStorage<TrackedDoc>>();

            storage.Upsert(Arg.Any<TrackedDoc>(), Arg.Any<IStorageSession>(), Arg.Any<string>())
                .Returns(upsert);
            storage.Overwrite(Arg.Any<TrackedDoc>(), Arg.Any<IStorageSession>(), Arg.Any<string>())
                .Returns(overwrite);

            TheUpsert = upsert;
            TheOverwrite = overwrite;
            TheStorage = storage;
            Database = new StubDatabase(new StubProviderGraph(storage));
        }

        public IDocumentStorage<TrackedDoc> TheStorage { get; }
        public Weasel.Storage.IStorageOperation TheUpsert { get; }
        public Weasel.Storage.IStorageOperation TheOverwrite { get; }

        public IStorageSerializer Serializer { get; } =
            StorageSerializerAdapter.For(new SystemTextJsonSerializer());

        public IStorageDatabase Database { get; }

        public ConcurrencyChecks Concurrency { get; set; } = ConcurrencyChecks.Enabled;

        public IList<IChangeTracker> ChangeTrackers { get; } = new List<IChangeTracker>();
        public Dictionary<Type, object> ItemMap { get; } = new();

        public string TenantId => "*DEFAULT*";
        public string? CausationId { get; set; }
        public string? CorrelationId { get; set; }
        public string? CurrentUserName { get; set; }
        public Dictionary<string, object>? Headers => null;
        public bool CausationIdEnabled => false;
        public bool CorrelationIdEnabled => false;
        public bool HeadersEnabled => false;
        public bool UserNameEnabled => false;

        public IVersionTracker Versions => throw new NotSupportedException();
        public IDocumentStorage StorageFor(Type documentType) => throw new NotSupportedException();
        public IDocumentStorage<T> StorageFor<T>() where T : notnull => throw new NotSupportedException();
        public void MarkAsAddedForStorage(object id, object document) => throw new NotSupportedException();
        public void MarkAsDocumentLoaded(object id, object document) => throw new NotSupportedException();
        public string NextTempTableName() => throw new NotSupportedException();

        public Task<DbDataReader> ExecuteReaderAsync(DbCommand command, CancellationToken token = default) =>
            throw new NotSupportedException();
    }

    /// <summary>
    /// A distinguishable stand-in for the operation the tracker hands back, so a test can say
    /// which of the two exits it took.
    /// </summary>
    private sealed class MarkerOperation(string name): Weasel.Storage.IStorageOperation
    {
        public override string ToString() => name;

        public Type DocumentType => typeof(TrackedDoc);
        public OperationRole Role() => OperationRole.Upsert;

        public void ConfigureCommand(ICommandBuilder builder, IStorageSession session) =>
            throw new NotSupportedException();

        public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) =>
            throw new NotSupportedException();
    }

    private sealed class StubDatabase(IProviderGraph providers): IStorageDatabase
    {
        public IProviderGraph Providers { get; } = providers;

        public DbConnection CreateStorageConnection() => throw new NotSupportedException();
        public Task RunSqlAsync(string sql, CancellationToken ct = default) => throw new NotSupportedException();
        public ISequence SequenceFor(Type documentType) => throw new NotSupportedException();
    }

    private sealed class StubProviderGraph(IDocumentStorage<TrackedDoc> storage): IProviderGraph
    {
        private readonly DocumentProvider<TrackedDoc> _provider = new(storage, storage, storage, storage);

        public DocumentProvider<T> StorageFor<T>() where T : notnull =>
            _provider as DocumentProvider<T>
            ?? throw new NotSupportedException($"Only {typeof(TrackedDoc).Name} is registered.");

        public void Append<T>(DocumentProvider<T> provider) where T : notnull => throw new NotSupportedException();
    }
}
