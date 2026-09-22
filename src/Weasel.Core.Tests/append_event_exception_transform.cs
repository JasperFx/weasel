using System;
using JasperFx.Core.Exceptions;
using JasperFx.Events;
using Shouldly;
using Weasel.Core;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     weasel#596. <see cref="InsertStreamOperationBase" /> has always carried a per-operation
///     exception transform so a dialect can translate a streams-table collision with the
///     <see cref="StreamAction" /> in hand. The per-event append had no such hook, so the
///     unique violation on <c>(stream_id, version)</c> -- how a lost optimistic-concurrency race
///     surfaces on the rich append path -- fell through to the store's global transform chain,
///     which sees only the driver exception.
/// </summary>
public class append_event_exception_transform
{
    public record SomethingHappened(string Name);

    private sealed class FakeAppendOperation: AppendEventOperationBase
    {
        public FakeAppendOperation(
            StreamAction stream,
            IEvent e,
            Func<Exception, StreamAction, Exception?>? transform)
            : base(stream, e, transform)
        {
        }

        public override void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
        {
            throw new NotSupportedException();
        }
    }

    private static (StreamAction stream, IEvent @event) Sample()
    {
        var @event = new Event<SomethingHappened>(new SomethingHappened("one")) { Version = 3 };
        var stream = StreamAction.Append(Guid.NewGuid(), [@event]);
        return (stream, @event);
    }

    [Fact]
    public void the_base_is_an_exception_transform()
    {
        var (stream, @event) = Sample();
        new FakeAppendOperation(stream, @event, null).ShouldBeAssignableTo<IExceptionTransform>();
    }

    [Fact]
    public void does_not_transform_when_no_closure_is_installed()
    {
        var (stream, @event) = Sample();
        var operation = new FakeAppendOperation(stream, @event, null);

        operation.TryTransform(new Exception("boom"), out var transformed).ShouldBeFalse();
        transformed.ShouldBeNull();
    }

    [Fact]
    public void does_not_transform_when_the_closure_declines()
    {
        var (stream, @event) = Sample();
        var operation = new FakeAppendOperation(stream, @event, (_, _) => null);

        operation.TryTransform(new Exception("boom"), out var transformed).ShouldBeFalse();
        transformed.ShouldBeNull();
    }

    [Fact]
    public void hands_the_stream_action_to_the_closure()
    {
        var (stream, @event) = Sample();
        StreamAction? seen = null;
        Exception? sawOriginal = null;

        var operation = new FakeAppendOperation(stream, @event, (original, action) =>
        {
            sawOriginal = original;
            seen = action;
            return new InvalidOperationException($"stream {action.Id} is at an unexpected version");
        });

        var raw = new Exception("23505: duplicate key value violates unique constraint");

        operation.TryTransform(raw, out var transformed).ShouldBeTrue();

        // The whole point: the dialect gets the StreamAction, so it never has to
        // reconstruct the stream id from provider-specific error detail.
        seen.ShouldBeSameAs(stream);
        sawOriginal.ShouldBeSameAs(raw);
        transformed.ShouldBeOfType<InvalidOperationException>()
            .Message.ShouldBe($"stream {stream.Id} is at an unexpected version");
    }
}
