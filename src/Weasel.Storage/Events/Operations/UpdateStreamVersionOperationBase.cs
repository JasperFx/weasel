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
/// Dialect-neutral base for the closed-shape <c>mt_streams</c>
/// update-version operation (expected-version guard). Relocated from Marten's
/// <c>Marten.Events.Operations.UpdateStreamVersion</c> as part of the
/// marten#4821 event-storage move (event E3).
/// </summary>
/// <remarks>
/// The command shape lives on a dialect-installed descriptor closure
/// (<c>ConfigureUpdateStreamVersionCommand</c>). SQL shape:
/// <c>update {schema}.mt_streams set version = $1 where id = $2 and version = $3 [and tenant_id = $4] returning version</c>.
/// <see cref="PostprocessAsync"/> raises
/// <see cref="EventStreamUnexpectedMaxEventIdException"/> when no row was
/// updated (expected-version mismatch) — the concurrency exception lives in
/// JasperFx.Events, so this Postprocess semantic ports neutrally.
/// </remarks>
public abstract class UpdateStreamVersionOperationBase: IStorageOperation
{
    protected UpdateStreamVersionOperationBase(StreamAction stream)
    {
        Stream = stream;
    }

    public StreamAction Stream { get; }

    public abstract void ConfigureCommand(ICommandBuilder builder, IStorageSession session);

    public Type DocumentType => typeof(IEvent);

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
        if (reader.RecordsAffected == 0)
        {
            exceptions.Add(new EventStreamUnexpectedMaxEventIdException(
                Stream.Key ?? (object)Stream.Id, Stream.AggregateType,
                Stream.ExpectedVersionOnServer!.Value, -1));
        }

        return Task.CompletedTask;
    }

    public OperationRole Role()
    {
        return OperationRole.Events;
    }
}
