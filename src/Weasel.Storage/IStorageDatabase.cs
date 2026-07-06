#nullable enable
using System.Data.Common;
using Weasel.Core.Sequences;

namespace Weasel.Storage;

/// <summary>
///     Dialect-neutral database accessor the closed-shape storage runtime targets: the
///     <see cref="IProviderGraph"/> for document-provider lookup, the
///     <see cref="ISequenceSource"/> Hi-Lo/sequence seam used by the Weasel.Core.Identity
///     <c>AssignIfMissing</c> strategies, and a neutral connection/SQL surface for the
///     session-free storage paths.
/// </summary>
public interface IStorageDatabase: ISequenceSource
{
    IProviderGraph Providers { get; }

    /// <summary>
    ///     Open a new database-neutral connection. The projection-safe closed-shape read path
    ///     uses this to run its <see cref="DbCommand"/> off the session. (Distinct name from any
    ///     dialect-typed <c>CreateConnection</c> a store's database exposes, to avoid a
    ///     same-signature return-type clash.)
    /// </summary>
    DbConnection CreateStorageConnection();

    /// <summary>
    ///     Execute a single SQL statement against a fresh connection. Used by the closed-shape
    ///     <c>TruncateDocumentStorageAsync</c> path.
    /// </summary>
    Task RunSqlAsync(string sql, CancellationToken ct = default);
}
