#nullable enable
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Weasel.Storage;

/// <summary>
///     A store's per-command observation of <see cref="OperationBatchExecutor" />: the before, success and
///     failure lines its session logger writes around each command, and the store-wide exception
///     transforms asked after the operations' own.
/// </summary>
/// <remarks>
///     <para>
///         "Command" means one round trip: the whole batch for
///         <see cref="OperationBatchExecutor.ExecuteAsync(IReadOnlyList{IStorageOperation}, Func{CancellationToken, Task{DbDataReader}}, OperationBatchOptions, CancellationToken)" />
///         and its <see cref="DbBatch" /> and <see cref="DbCommand" /> overloads, one operation for
///         <see cref="OperationBatchExecutor.ExecuteEachAsync(IReadOnlyList{IStorageOperation}, Func{IStorageOperation, DbCommand}, Func{DbCommand, CancellationToken, Task{DbDataReader}}, OperationBatchOptions, CancellationToken)" />.
///         For each, the executor calls, in order:
///     </para>
///     <list type="number">
///         <item><see cref="BeforeExecute" />, before the reader is opened;</item>
///         <item>
///             then exactly one of <see cref="OnSuccess" /> — once every result set has been walked and
///             drained, before anything postprocessing <em>collected</em> (a concurrency miss) is thrown —
///             or <see cref="OnFailure" /> with the provider's own exception, <em>before</em> any transform
///             replaces it, so the log line still says what the database refused;
///         </item>
///         <item>
///             after <see cref="OnFailure" />, <see cref="TryTransform" />, but only when no operation's
///             <see cref="JasperFx.Core.Exceptions.IExceptionTransform" /> claimed the exception.
///         </item>
///     </list>
///     <para>
///         No listener costs nothing: <see cref="OperationBatchOptions.Listener" /> left
///         <see langword="null" /> allocates no <see cref="OperationBatchCommand" /> and makes no call. A
///         store that decides once per batch whether it is logging passes a listener only when it is.
///     </para>
///     <para>
///         Every member has a default that does nothing, so an implementation overrides only what it
///         needs. A listener must not throw: an exception from one replaces the one being reported.
///     </para>
/// </remarks>
public interface IOperationBatchListener
{
    /// <summary>Called immediately before the command's reader is opened.</summary>
    void BeforeExecute(OperationBatchCommand command)
    {
    }

    /// <summary>
    ///     Called once the command's reader has been walked and drained without a provider exception.
    ///     Exceptions postprocessing collected are thrown after this, not reported through
    ///     <see cref="OnFailure" />: the command itself succeeded.
    /// </summary>
    void OnSuccess(OperationBatchCommand command)
    {
    }

    /// <summary>
    ///     Called with the exception that opening, walking or draining the reader threw, before any
    ///     transform is applied to it.
    /// </summary>
    void OnFailure(OperationBatchCommand command, Exception exception)
    {
    }

    /// <summary>
    ///     The store's own exception translation, asked after every operation's
    ///     <see cref="JasperFx.Core.Exceptions.IExceptionTransform" /> declined <paramref name="exception" />.
    ///     Return <see langword="true" /> with a replacement to throw it instead of the original.
    /// </summary>
    /// <returns><see langword="false" /> by default: the original is rethrown with its stack trace.</returns>
    bool TryTransform(OperationBatchCommand command, Exception exception,
        [NotNullWhen(true)] out Exception? transformed)
    {
        transformed = null;
        return false;
    }
}

/// <summary>
///     The command an <see cref="IOperationBatchListener" /> is told about: the operations it carries and,
///     when the store handed it to the executor, the provider object it was compiled to.
/// </summary>
public sealed class OperationBatchCommand
{
    /// <summary>Describe one command to a listener.</summary>
    /// <param name="operations">The operations the command carries, in the order they were configured.</param>
    /// <param name="batch">The batch the operations were compiled into, if the store passed one.</param>
    /// <param name="command">The command the operations were compiled into, if the store passed one.</param>
    public OperationBatchCommand(IReadOnlyList<IStorageOperation> operations, DbBatch? batch = null,
        DbCommand? command = null)
    {
        ArgumentNullException.ThrowIfNull(operations);

        Operations = operations;
        Batch = batch;
        Command = command;
    }

    /// <summary>The operations this command carries, in configuration order.</summary>
    public IReadOnlyList<IStorageOperation> Operations { get; }

    /// <summary>
    ///     The <see cref="DbBatch" /> being executed, when the store called a <see cref="DbBatch" />
    ///     overload; <see langword="null" /> otherwise. Cast it to the provider's own type to log it.
    /// </summary>
    public DbBatch? Batch { get; }

    /// <summary>
    ///     The <see cref="DbCommand" /> being executed, when the store called a <see cref="DbCommand" />
    ///     overload (or prepared one per operation); <see langword="null" /> otherwise.
    /// </summary>
    public DbCommand? Command { get; }
}
