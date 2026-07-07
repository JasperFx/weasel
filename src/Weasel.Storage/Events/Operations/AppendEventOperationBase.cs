#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
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
/// Implements the neutral <see cref="Weasel.Storage.IStorageOperation"/> — a
/// store's own dialect-typed operation contract derives from it and bridges the
/// dialect-typed <c>ConfigureCommand</c> down to this neutral slot with a
/// default interface method, so a moved op that authors the neutral
/// <see cref="ConfigureCommand"/> directly is a first-class citizen of the
/// execution pipeline.
/// </remarks>
public abstract class AppendEventOperationBase: IStorageOperation, NoDataReturnedCall
{
    protected AppendEventOperationBase(StreamAction stream, IEvent e)
    {
        Stream = stream;
        Event = e;

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
}
