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
/// A <see cref="QuickAppendEventWithVersionOperation"/> whose SQL returns the <c>seq_id</c> its server-side sequence
/// drew, written back onto <see cref="IEvent.Sequence"/>. Used when the descriptor says its suffix ends in such a
/// clause.
/// </summary>
/// <remarks>
/// marten#5454: without the sequence, an event appended by this per-event INSERT keeps <c>Sequence</c> 0, so the
/// tombstone batch a failed unit of work writes skips it and the number it drew is never filled. It wraps the
/// no-data operation rather than deriving from it because <see cref="NoDataReturnedCall"/> sits on
/// <see cref="AppendEventOperationBase"/>, and that marker is what tells the batch reader not to read a result.
/// </remarks>
internal sealed class QuickAppendEventWithVersionReadingSequenceOperation: IStorageOperation, IExceptionTransform
{
    private readonly QuickAppendEventWithVersionOperation _inner;

    public QuickAppendEventWithVersionReadingSequenceOperation(QuickAppendEventWithVersionOperation inner)
    {
        _inner = inner;
    }

    public StreamAction Stream => _inner.Stream;
    public IEvent Event => _inner.Event;

    public Type DocumentType => _inner.DocumentType;

    public void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        _inner.ConfigureCommand(builder, session);
    }

    public async Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
        if (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            Event.Sequence = Convert.ToInt64(reader.GetValue(0));
        }
    }

    public OperationRole Role()
    {
        return _inner.Role();
    }

    public bool TryTransform(Exception original, out Exception? transformed)
    {
        return _inner.TryTransform(original, out transformed);
    }

    public override string ToString()
    {
        return _inner.ToString();
    }
}
