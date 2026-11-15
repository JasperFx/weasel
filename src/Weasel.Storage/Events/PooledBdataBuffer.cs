#nullable enable
using System;
using System.Buffers;

namespace Weasel.Storage;

/// <summary>
///     weasel#831: the buffer one per-event operation writes its event's <c>bdata</c> into, rented from
///     <see cref="ArrayPool{T}.Shared" /> and returned only when the operation is disposed.
/// </summary>
/// <remarks>
///     <see cref="WrittenMemory" /> is a view over the rented array, bound as the parameter value, so it is
///     taken only once the write is complete: growing the buffer moves it to a new rental and returns the
///     old one. Returning the rental while a command can still be executed — a retry re-executes the same
///     batch — would send whatever the pool's next renter wrote there (marten#5262).
/// </remarks>
internal sealed class PooledBdataBuffer: IBufferWriter<byte>, IDisposable
{
    private const int InitialSize = 512;

    private byte[]? _buffer = ArrayPool<byte>.Shared.Rent(InitialSize);
    private int _written;

    public ReadOnlyMemory<byte> WrittenMemory => rented().AsMemory(0, _written);

    public void Advance(int count)
    {
        var buffer = rented();
        if (count < 0 || _written + count > buffer.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(count), count,
                "Cannot advance past the end of the buffer the writer was given.");
        }

        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0) => ensure(sizeHint).AsMemory(_written);

    public Span<byte> GetSpan(int sizeHint = 0) => ensure(sizeHint).AsSpan(_written);

    public void Dispose()
    {
        // Idempotent: returning one array twice would hand it to two renters at once.
        var buffer = _buffer;
        if (buffer is null)
        {
            return;
        }

        _buffer = null;
        ArrayPool<byte>.Shared.Return(buffer);
    }

    private byte[] ensure(int sizeHint)
    {
        var buffer = rented();
        if (sizeHint < 1)
        {
            sizeHint = 1;
        }

        if (buffer.Length - _written >= sizeHint)
        {
            return buffer;
        }

        var next = ArrayPool<byte>.Shared.Rent(Math.Max(buffer.Length * 2, _written + sizeHint));
        buffer.AsSpan(0, _written).CopyTo(next);
        _buffer = next;
        ArrayPool<byte>.Shared.Return(buffer);
        return next;
    }

    private byte[] rented() => _buffer ?? throw new ObjectDisposedException(nameof(PooledBdataBuffer));
}
