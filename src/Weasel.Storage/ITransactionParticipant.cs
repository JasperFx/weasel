#nullable enable
using System.Data.Common;

namespace Weasel.Storage;

/// <summary>
///     Something else writing on the owning store's connection, inside the store's transaction,
///     committed (or rolled back) with it. Allows external components — an EF Core
///     <c>DbContext</c>, Dapper, hand-written ADO.NET — to flush their pending work into the
///     same transaction the store uses for its own batch operations, so "my rows and the
///     store's, or neither" holds atomically.
/// </summary>
/// <typeparam name="TConnection">The owning store's ADO.NET connection type.</typeparam>
/// <typeparam name="TTransaction">The owning store's ADO.NET transaction type.</typeparam>
/// <remarks>
///     <para>
///         Each Critter Stack store closes this over its own provider types and exposes the
///         closed shape as its own <c>ITransactionParticipant</c>, so participant code is
///         written against the provider the store actually uses. The interface is
///         contravariant in both type parameters, so a participant written against the base
///         <see cref="DbConnection" /> / <see cref="DbTransaction" /> pair satisfies any
///         store's closed shape.
///     </para>
///     <para>
///         <b>The connection is the point.</b> A participant must write on the connection it
///         is handed, not open one of its own to the same database — a parallel connection is
///         outside the transaction at best, and on an embedded store (one writer per database
///         file) it is a self-deadlock that presents as a hang rather than an error. That is
///         why the connection is a parameter rather than something the participant is
///         expected to find.
///     </para>
/// </remarks>
public interface ITransactionParticipant<in TConnection, in TTransaction>
    where TConnection : DbConnection
    where TTransaction : DbTransaction
{
    /// <summary>
    ///     Write, on the supplied connection and inside the supplied transaction. Called after
    ///     the store's own operations have executed but before the transaction is committed,
    ///     so nothing written here is visible to anyone else until the store commits, and
    ///     throwing here rolls back the store's work along with the participant's.
    /// </summary>
    /// <remarks>
    ///     <b>This may be called more than once for one unit of work</b>, and a participant
    ///     has to survive it. A store whose commit runs inside a resilience pipeline (e.g. a
    ///     retried <c>SQLITE_BUSY</c> on Fisher) re-executes the whole write delegate, so
    ///     whatever this writes must still be pending on the second attempt. The failed
    ///     attempt's transaction rolled back, so re-writing is correct; <em>not</em>
    ///     re-writing is the silent failure, because the store's own work commits either way.
    ///     See <see cref="AfterCommitAsync" /> for the other half of that.
    /// </remarks>
    Task BeforeCommitAsync(TConnection connection, TTransaction transaction, CancellationToken token);

    /// <summary>
    ///     Reconcile whatever <see cref="BeforeCommitAsync" /> left pending, now that the
    ///     write is durable. Does nothing unless a participant overrides it.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>This is not a post-commit side-effect hook</b> — a store's session listener
    ///         seam is still the place for those, with the "everyone can see this now"
    ///         semantics an application wants. This exists for the narrower job the retry rule
    ///         above creates: a participant that has to keep its writes replayable across
    ///         attempts needs one place to stop keeping them, and only the store knows when
    ///         the commit happened.
    ///     </para>
    ///     <para>
    ///         Runs <b>outside</b> any resilience pipeline the store's commit uses, so it
    ///         fires once for a transaction that committed rather than once per attempt. It
    ///         does not fire for a session enlisted in a transaction the caller owns — there
    ///         the commit is the caller's and the store is never told it happened, so a
    ///         participant has nothing to reconcile until the caller commits.
    ///     </para>
    /// </remarks>
    Task AfterCommitAsync(CancellationToken token) => Task.CompletedTask;
}
