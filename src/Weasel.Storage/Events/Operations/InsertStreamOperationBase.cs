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
/// Dialect-neutral base for the closed-shape <c>mt_streams</c> insert operation
/// (one per new stream, no result set). Relocated from Marten's
/// <c>Marten.Events.Operations.InsertStreamBase</c> as part of the marten#4821
/// event-storage move (event E3).
/// </summary>
/// <remarks>
/// <para>
/// The concrete command shape lives on a dialect-installed descriptor closure
/// (<c>ConfigureInsertStreamCommand</c>); the subclasses are minimal shells that
/// pull the closure off their descriptor and invoke it.
/// </para>
/// <para>
/// Stream-id-collision friendliness is preserved without leaking any provider
/// type into the neutral library: the base implements the neutral
/// <see cref="IExceptionTransform"/> and delegates to an optional
/// <c>TransformInsertStreamException</c> closure supplied by the descriptor. The
/// Postgres dialect installs a closure that maps a unique-constraint violation on
/// <c>mt_streams</c> to Marten's <c>ExistingStreamIdCollisionException</c>; a
/// SQL Server dialect (Polecat) installs its own. When no closure is installed
/// the raw provider exception flows through unchanged.
/// </para>
/// </remarks>
public abstract class InsertStreamOperationBase: IStorageOperation, IExceptionTransform, NoDataReturnedCall
{
    private readonly Func<Exception, StreamAction, Exception?>? _transformInsertStreamException;

    protected InsertStreamOperationBase(
        StreamAction stream,
        Func<Exception, StreamAction, Exception?>? transformInsertStreamException)
    {
        Stream = stream;
        _transformInsertStreamException = transformInsertStreamException;
    }

    public StreamAction Stream { get; }

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
        return $"InsertStream: {Stream.Key ?? Stream.Id.ToString()}";
    }

    public bool TryTransform(Exception original, out Exception? transformed)
    {
        if (_transformInsertStreamException is { } transform)
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
