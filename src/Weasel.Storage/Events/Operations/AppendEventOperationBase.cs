#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Exceptions;
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Dialect-neutral base for the closed-shape per-event append operation — one
/// <see cref="IStorageOperation"/> per event, no result set. Relocated from
/// Marten's <c>Marten.Events.Operations.AppendEventOperationBase</c> as part of
/// the marten#4821 event-storage move (event E3); Marten keeps its own
/// like-named base behind for the legacy codegen write path ("Leave public for
/// codegen!").
/// </summary>
/// <remarks>
/// <para>
/// Implements the neutral <see cref="Weasel.Storage.IStorageOperation"/> — a
/// store's own dialect-typed operation contract derives from it and bridges the
/// dialect-typed <c>ConfigureCommand</c> down to this neutral slot with a
/// default interface method, so a moved op that authors the neutral
/// <see cref="ConfigureCommand"/> directly is a first-class citizen of the
/// execution pipeline.
/// </para>
/// <para>
/// Like <see cref="InsertStreamOperationBase"/>, this implements the neutral
/// <see cref="IExceptionTransform"/> and delegates to an optional
/// <c>TransformAppendEventException</c> closure supplied by the descriptor
/// (weasel#596). The unique violation on the events table's
/// <c>(stream_id, version)</c> key is how a lost optimistic-concurrency race
/// surfaces on the rich append path; without a per-operation hook it falls
/// through to the store's *global* transform chain, which sees only the driver
/// exception and has to reconstruct the stream id and aggregate type by regex
/// over provider-specific (and frequently redacted) error detail. With the hook
/// the dialect has the <see cref="StreamAction"/> in hand. When no closure is
/// installed the raw provider exception flows through unchanged.
/// </para>
/// </remarks>
public abstract class AppendEventOperationBase: IStorageOperation, IExceptionTransform, NoDataReturnedCall
{
    private readonly Func<Exception, StreamAction, Exception?>? _transformAppendEventException;

    protected AppendEventOperationBase(StreamAction stream, IEvent e)
        : this(stream, e, null)
    {
    }

    protected AppendEventOperationBase(
        StreamAction stream,
        IEvent e,
        Func<Exception, StreamAction, Exception?>? transformAppendEventException)
    {
        Stream = stream;
        Event = e;
        _transformAppendEventException = transformAppendEventException;

        if (e.Version == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(e), "Version cannot be 0");
        }
    }

    public StreamAction Stream { get; }
    public IEvent Event { get; }

    public abstract void ConfigureCommand(ICommandBuilder builder, IStorageSession session);

    public Type DocumentType => typeof(IEvent);

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
        return Task.CompletedTask;
    }

    public OperationRole Role()
    {
        return OperationRole.Events;
    }

    public override string ToString()
    {
        return $"Insert Event to Stream {Stream.Key ?? Stream.Id.ToString()}, Version {Event.Version}";
    }

    public bool TryTransform(Exception original, out Exception? transformed)
    {
        if (_transformAppendEventException is { } transform)
        {
            var result = transform(original, Stream);
            if (result is not null)
            {
                transformed = result;
                return true;
            }
        }

        transformed = null;
        return false;
    }
}
