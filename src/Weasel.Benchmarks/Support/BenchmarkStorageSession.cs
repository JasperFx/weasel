using System.Data.Common;
using JasperFx.Events.Aggregation;
using JasperFx.MultiTenancy;
using Weasel.Core;
using Weasel.Core.Sequences;
using Weasel.Core.SqlGeneration;
using Weasel.Storage;
using IStorageOperation = Weasel.Storage.IStorageOperation;

namespace Weasel.Benchmarks.Support;

/// <summary>
///     The least <see cref="IStorageSession" /> a <see cref="ChangeTracker{T}" /> needs: a real
///     serializer (the thing being measured) and a provider graph whose dirty-tracking storage
///     hands back a trivial operation. Everything the change-tracking path never touches throws,
///     so the benchmark cannot quietly start measuring something else.
/// </summary>
internal sealed class BenchmarkStorageSession: IStorageSession
{
    public BenchmarkStorageSession(IStorageSerializer serializer, IProviderGraph providers)
    {
        Serializer = serializer;
        Database = new StubDatabase(providers);
    }

    public IStorageSerializer Serializer { get; }
    public IStorageDatabase Database { get; }
    public ConcurrencyChecks Concurrency { get; set; } = ConcurrencyChecks.Enabled;

    /// <summary>
    ///     Settable so the same benchmark document can be measured with the semantic fallback both
    ///     off (the default since weasel#577) and on.
    /// </summary>
    public bool UseSemanticJsonChangeDetection { get; set; }

    public IList<IChangeTracker> ChangeTrackers { get; } = new List<IChangeTracker>();
    public Dictionary<Type, object> ItemMap { get; } = new();

    public string TenantId => StorageConstants.DefaultTenantId;
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

    private sealed class StubDatabase: IStorageDatabase
    {
        public StubDatabase(IProviderGraph providers) => Providers = providers;

        public IProviderGraph Providers { get; }

        public DbConnection CreateStorageConnection() => throw new NotSupportedException();
        public Task RunSqlAsync(string sql, CancellationToken ct = default) => throw new NotSupportedException();

        public ISequence SequenceFor(Type documentType) => throw new NotSupportedException();
    }
}

internal static class StorageConstants
{
    public const string DefaultTenantId = "*DEFAULT*";
}

/// <summary>
///     A provider graph holding exactly one document type's storage.
/// </summary>
internal sealed class BenchmarkProviderGraph<TDoc>: IProviderGraph where TDoc : notnull
{
    private readonly DocumentProvider<TDoc> _provider;

    public BenchmarkProviderGraph(IDocumentStorage<TDoc> storage)
    {
        _provider = new DocumentProvider<TDoc>(storage, storage, storage, storage);
    }

    public DocumentProvider<T> StorageFor<T>() where T : notnull
    {
        return _provider as DocumentProvider<T>
               ?? throw new NotSupportedException($"Only {typeof(TDoc).Name} is registered.");
    }

    public void Append<T>(DocumentProvider<T> provider) where T : notnull => throw new NotSupportedException();
}

/// <summary>
///     Implements only the three members <see cref="ChangeTracker{T}.DetectChanges" /> reaches on
///     the changed path — the two concurrency flags and <c>Upsert</c> — and returns an operation
///     that does nothing, so the benchmark reports the cost of detecting the change rather than the
///     cost of a store's real upsert construction.
/// </summary>
internal sealed class NoopDocumentStorage<T>: IDocumentStorage<T> where T : notnull
{
    private static readonly IStorageOperation _operation = new NoopOperation();

    public bool UseOptimisticConcurrency => false;
    public bool UseNumericRevisions => false;

    public IStorageOperation Upsert(T document, IStorageSession session, string tenantId) => _operation;
    public IStorageOperation Overwrite(T document, IStorageSession session, string tenantId) => _operation;

    // ---- everything below is unreachable from the change-tracking path ----

    public Type SourceType => typeof(T);
    public Type IdType => typeof(Guid);
    public Type DocumentType => typeof(T);
    public IOperationFragment DeleteFragment => throw new NotSupportedException();
    public IOperationFragment HardDeleteFragment => throw new NotSupportedException();
    public IReadOnlyList<IDuplicatedField> DuplicatedFields => throw new NotSupportedException();
    public DbObjectName TableName => throw new NotSupportedException();
    public TenancyStyle TenancyStyle => throw new NotSupportedException();
    public string FromObject => throw new NotSupportedException();
    public Type SelectedType => throw new NotSupportedException();

    public string[] SelectFields() => throw new NotSupportedException();
    public ISelector BuildSelector(IStorageSession session) => throw new NotSupportedException();
    public void Apply(ICommandBuilder builder) => throw new NotSupportedException();
    public bool Contains(Type type) => throw new NotSupportedException();

    public Task TruncateDocumentStorageAsync(IStorageDatabase database, CancellationToken ct = default) =>
        throw new NotSupportedException();

    public ISqlFragment FilterDocuments(ISqlFragment query, IStorageSession session) =>
        throw new NotSupportedException();

    public ISqlFragment? DefaultWhereFragment() => throw new NotSupportedException();
    public object RawIdentityValue(object id) => throw new NotSupportedException();
    public object IdentityFor(T document) => throw new NotSupportedException();
    public Guid? VersionFor(T document, IStorageSession session) => throw new NotSupportedException();
    public void Store(IStorageSession session, T document) => throw new NotSupportedException();
    public void Store(IStorageSession session, T document, Guid? version) => throw new NotSupportedException();
    public void Store(IStorageSession session, T document, long revision) => throw new NotSupportedException();
    public void Eject(IStorageSession session, T document) => throw new NotSupportedException();

    public IStorageOperation Update(T document, IStorageSession session, string tenantId) =>
        throw new NotSupportedException();

    public IStorageOperation Insert(T document, IStorageSession session, string tenantId) =>
        throw new NotSupportedException();

    public IStorageOperation OverwriteProjected(T document, string tenantId) => throw new NotSupportedException();
    public IStorageOperation UpsertProjected(T document, string tenantId) => throw new NotSupportedException();
    public IStorageOperation InsertProjected(T document, string tenantId) => throw new NotSupportedException();
    public IStorageOperation UpdateProjected(T document, string tenantId) => throw new NotSupportedException();
    public IDeletion DeleteForDocument(T document, string tenantId) => throw new NotSupportedException();
    public void EjectById(IStorageSession session, object id) => throw new NotSupportedException();
    public void RemoveDirtyTracker(IStorageSession session, object id) => throw new NotSupportedException();
    public IDeletion HardDeleteForDocument(T document, string tenantId) => throw new NotSupportedException();
    public void SetIdentityFromString(T document, string identityString) => throw new NotSupportedException();
    public void SetIdentityFromGuid(T document, Guid identityGuid) => throw new NotSupportedException();

    private sealed class NoopOperation: IStorageOperation
    {
        public Type DocumentType => typeof(T);
        public OperationRole Role() => OperationRole.Upsert;

        public void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
        {
        }

        public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) =>
            Task.CompletedTask;
    }
}
