#nullable enable
using System;
using System.Buffers;
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
///     Binds the per-event <c>bdata</c> parameter for the operations that write one event per statement
///     (<see cref="RichAppendEventOperation" /> and <see cref="QuickAppendEventWithVersionOperation" />),
///     through the descriptor's buffer-writer slot when the store supplied one (weasel#831), else through
///     its <c>byte[]</c> slot as before.
/// </summary>
internal struct EventBdataParameter
{
    private readonly Func<IEvent, byte[]?> _serialize;
    private readonly Func<IEvent, IBufferWriter<byte>, bool>? _write;

    // Written once, on the first ConfigureCommand, and kept until Dispose: a store may configure the
    // operation again, and must be able to re-execute the command it already built.
    private PooledBdataBuffer? _buffer;
    private bool _written;

    public EventBdataParameter(Func<IEvent, byte[]?> serialize, Func<IEvent, IBufferWriter<byte>, bool>? write)
    {
        _serialize = serialize;
        _write = write;
    }

    public void Append(IGroupedParameterBuilder pb, IStorageDialect dialect, IEvent @event)
    {
        if (_write is null)
        {
            // bdata (nullable): NULL for JSON-serialized events, payload for binary-serialized events. The
            // neutral AppendParameter writes a null byte[] as DBNull.
            dialect.SetParameterType(pb.AppendParameter(_serialize(@event)), StorageColumnType.Binary);
            return;
        }

        if (!_written)
        {
            var buffer = new PooledBdataBuffer();
            try
            {
                if (_write(@event, buffer))
                {
                    _buffer = buffer;
                }
                else
                {
                    // A JSON event: nothing was written, and nothing needs keeping
                    buffer.Dispose();
                }
            }
            catch
            {
                buffer.Dispose();
                throw;
            }

            _written = true;
        }

        var parameter = pb.AppendParameter(null);
        dialect.SetParameterType(parameter, StorageColumnType.Binary);
        if (_buffer is not null)
        {
            dialect.SetBinaryParameterValue(parameter, _buffer.WrittenMemory);
        }
    }

    /// <summary>
    ///     Return the rental. Only once the unit of work is over: until then a retry may re-execute the
    ///     command that binds it.
    /// </summary>
    public void Release()
    {
        _buffer?.Dispose();
        _buffer = null;
        _written = false;
    }
}
