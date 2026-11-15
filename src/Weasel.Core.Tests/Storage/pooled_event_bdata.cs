using System.Data.Common;
using System.Reflection;
using JasperFx.Events;
using NSubstitute;
using Shouldly;
using Weasel.Storage;
using Xunit;
namespace Weasel.Core.Tests.StorageHelpers;

using IStorageOperation = Weasel.Storage.IStorageOperation;

/// <summary>
///     weasel#831: with <c>WriteEventBdata</c> on the descriptor, the per-event operations write a binary
///     event's payload into a pooled buffer and bind a slice of it, instead of a fresh array per event.
/// </summary>
public class pooled_event_bdata
{
    private readonly ITestOutputHelper _output;
    private readonly IStorageSession theSession = Substitute.For<IStorageSession>();

    public pooled_event_bdata(ITestOutputHelper output)
    {
        _output = output;
    }

    private static byte[] payload(int length, byte seed)
    {
        var bytes = new byte[length];
        for (var i = 0; i < bytes.Length; i++)
        {
            bytes[i] = (byte)(seed + i);
        }

        return bytes;
    }

    private static IEvent binary(byte[] bytes, long version = 1)
        => new Event<BinaryHappened>(new BinaryHappened(bytes)) { Version = version, Sequence = version };

    private static IEvent json(long version = 1)
        => new Event<JsonHappened>(new JsonHappened("json")) { Version = version, Sequence = version };

    public enum PerEventPath
    {
        RichAppend,
        RichSideEffectReplay,
        QuickTombstone,
        QuickPerEvent,
        QuickPerEventReadingSequence,
        QuickWithServerTimestampsTombstone,
        QuickWithServerTimestampsPerEvent
    }

    private static IStorageOperation build(BinaryEventDialect dialect, PerEventPath path, IEvent e,
        IStorageSession session)
    {
        var stream = StreamAction.Append(Guid.NewGuid(), [e]);
        return path switch
        {
            PerEventPath.RichAppend =>
                EventStorageBuilder.Build<Guid>(dialect, EventAppendMode.Rich, null!, null!).AppendEvent(session, stream, e),
            PerEventPath.RichSideEffectReplay =>
                EventStorageBuilder.Build<Guid>(dialect, EventAppendMode.Rich, null!, null!).QuickAppendEventWithVersion(stream, e),
            PerEventPath.QuickTombstone =>
                EventStorageBuilder.Build<Guid>(dialect, EventAppendMode.Quick, null!, null!).AppendEvent(session, stream, e),
            PerEventPath.QuickPerEvent =>
                EventStorageBuilder.Build<Guid>(dialect, EventAppendMode.Quick, null!, null!).QuickAppendEventWithVersion(stream, e),
            PerEventPath.QuickPerEventReadingSequence =>
                EventStorageBuilder.Build<Guid>(new ReturningSequence(dialect), EventAppendMode.Quick, null!, null!)
                    .QuickAppendEventWithVersion(stream, e),
            PerEventPath.QuickWithServerTimestampsTombstone =>
                EventStorageBuilder.Build<Guid>(dialect, EventAppendMode.QuickWithServerTimestamps, null!, null!)
                    .AppendEvent(session, stream, e),
            PerEventPath.QuickWithServerTimestampsPerEvent =>
                EventStorageBuilder.Build<Guid>(dialect, EventAppendMode.QuickWithServerTimestamps, null!, null!)
                    .QuickAppendEventWithVersion(stream, e),
            _ => throw new ArgumentOutOfRangeException(nameof(path))
        };
    }

    /// <summary>The Quick descriptor with a suffix that reads <c>seq_id</c> back, so the operation is wrapped.</summary>
    private sealed class ReturningSequence(BinaryEventDialect inner): IEventStoreSqlDialect
    {
        public RichEventStorageDescriptor BuildRichDescriptor(EventRegistry graph, IStorageSerializer serializer)
            => inner.BuildRichDescriptor(graph, serializer);

        public QuickEventStorageDescriptor BuildQuickDescriptor(EventRegistry graph, IStorageSerializer serializer)
        {
            var d = inner.BuildQuickDescriptor(graph, serializer);
            return new QuickEventStorageDescriptor(d.QuickAppendEventsSql, d.InsertStreamSql, d.UpdateStreamVersionSql,
                d.SerializeEventData, d.SerializeEventBdata)
            {
                AppendEventSqlPrefix = d.AppendEventSqlPrefix,
                AppendEventSqlSuffix = d.AppendEventSqlSuffix,
                AppendEventFullSqlSuffix = d.AppendEventFullSqlSuffix,
                IsGuidStreamIdentity = true,
                Dialect = d.Dialect,
                WriteEventBdata = d.WriteEventBdata,
                AppendEventSqlReturnsSequence = true
            };
        }

        public QuickWithServerTimestampsEventStorageDescriptor BuildQuickWithServerTimestampsDescriptor(
            EventRegistry graph, IStorageSerializer serializer)
            => inner.BuildQuickWithServerTimestampsDescriptor(graph, serializer);
    }

    private DbParameter bdataParameter(IStorageOperation operation)
    {
        var builder = new Weasel.Postgresql.CommandBuilder();
        operation.ConfigureCommand(builder, theSession);
        var command = builder.Compile();

        // data, type, mt_dotnet_type, bdata, ...
        return command.Parameters[3];
    }

    [Theory]
    [InlineData(PerEventPath.RichAppend)]
    [InlineData(PerEventPath.RichSideEffectReplay)]
    [InlineData(PerEventPath.QuickTombstone)]
    [InlineData(PerEventPath.QuickPerEvent)]
    [InlineData(PerEventPath.QuickPerEventReadingSequence)]
    [InlineData(PerEventPath.QuickWithServerTimestampsTombstone)]
    [InlineData(PerEventPath.QuickWithServerTimestampsPerEvent)]
    public void every_per_event_path_binds_a_slice_of_the_pooled_buffer(PerEventPath path)
    {
        var storageDialect = new ZeroCopyStorageDialect();
        var dialect = new BinaryEventDialect(storageDialect);
        var bytes = payload(1500, 7);
        var operation = build(dialect, path, binary(bytes), theSession);

        var parameter = bdataParameter(operation);

        var bound = parameter.Value.ShouldBeOfType<ReadOnlyMemory<byte>>();
        bound.ToArray().ShouldBe(bytes);
        dialect.WriteCalls.ShouldBe(1);
        dialect.SerializeCalls.ShouldBe(0);
        storageDialect.BinaryValues.ShouldBe(1);

        // The store disposes what it was handed, wrapper or not.
        operation.ShouldBeAssignableTo<IDisposable>()!.Dispose();
    }

    [Theory]
    [InlineData(PerEventPath.RichAppend)]
    [InlineData(PerEventPath.QuickTombstone)]
    [InlineData(PerEventPath.QuickWithServerTimestampsPerEvent)]
    public void a_json_event_binds_null_bdata_and_keeps_no_buffer(PerEventPath path)
    {
        var storageDialect = new ZeroCopyStorageDialect();
        var dialect = new BinaryEventDialect(storageDialect);
        var operation = build(dialect, path, json(), theSession);

        var parameter = bdataParameter(operation);

        parameter.Value.ShouldBe(DBNull.Value);
        ((Npgsql.NpgsqlParameter)parameter).NpgsqlDbType.ShouldBe(NpgsqlTypes.NpgsqlDbType.Bytea);
        dialect.WriteCalls.ShouldBe(1);
        storageDialect.BinaryValues.ShouldBe(0);
    }

    [Theory]
    [InlineData(PerEventPath.RichAppend)]
    [InlineData(PerEventPath.RichSideEffectReplay)]
    [InlineData(PerEventPath.QuickTombstone)]
    [InlineData(PerEventPath.QuickWithServerTimestampsPerEvent)]
    public void without_the_buffer_slot_the_array_slot_binds_as_before(PerEventPath path)
    {
        var dialect = new BinaryEventDialect(new ZeroCopyStorageDialect()) { UseBufferSlot = false };
        var bytes = payload(100, 3);

        var parameter = bdataParameter(build(dialect, path, binary(bytes), theSession));

        parameter.Value.ShouldBeOfType<byte[]>().ShouldBe(bytes);
        dialect.SerializeCalls.ShouldBe(1);
        dialect.WriteCalls.ShouldBe(0);
    }

    [Fact]
    public void a_dialect_compiled_before_the_member_gets_a_copy()
    {
        // The interface default: correct for every driver, without the saving.
        var dialect = new BinaryEventDialect(new StaleStorageDialect());
        var bytes = payload(700, 1);

        var parameter = bdataParameter(build(dialect, PerEventPath.RichAppend, binary(bytes), theSession));

        parameter.Value.ShouldBeOfType<byte[]>().ShouldBe(bytes);
    }

    [Fact]
    public void the_new_dialect_member_carries_a_default_body()
    {
        // weasel#682: an abstract interface member is a TypeLoadException for every implementer compiled
        // before it existed -- Marten's, Polecat's and Fisher's dialects among them.
        var method = typeof(IStorageDialect).GetMethod(nameof(IStorageDialect.SetBinaryParameterValue),
            BindingFlags.Public | BindingFlags.Instance);

        method.ShouldNotBeNull();
        method.IsAbstract.ShouldBeFalse();
    }

    [Fact]
    public void the_payload_is_written_once_however_often_the_operation_is_configured()
    {
        // A store may configure the operation again; the command it already built still binds the first
        // slice, so the second binds the same bytes rather than a new rental.
        var dialect = new BinaryEventDialect(new ZeroCopyStorageDialect());
        var bytes = payload(900, 9);
        var operation = build(dialect, PerEventPath.RichAppend, binary(bytes), theSession);

        var first = bdataParameter(operation).Value.ShouldBeOfType<ReadOnlyMemory<byte>>();
        var second = bdataParameter(operation).Value.ShouldBeOfType<ReadOnlyMemory<byte>>();

        dialect.WriteCalls.ShouldBe(1);
        second.Equals(first).ShouldBeTrue("the same slice of the same rental");
        second.ToArray().ShouldBe(bytes);
    }

    [Fact]
    public void disposing_twice_is_harmless_and_a_disposed_operation_writes_afresh()
    {
        var dialect = new BinaryEventDialect(new ZeroCopyStorageDialect());
        var bytes = payload(600, 5);
        var operation = build(dialect, PerEventPath.RichAppend, binary(bytes), theSession);
        bdataParameter(operation);

        var disposable = operation.ShouldBeAssignableTo<IDisposable>()!;
        disposable.Dispose();
        disposable.Dispose();

        bdataParameter(operation).Value.ShouldBeOfType<ReadOnlyMemory<byte>>().ToArray().ShouldBe(bytes);
        dialect.WriteCalls.ShouldBe(2);
        disposable.Dispose();
    }

    [Fact]
    public void a_write_that_throws_propagates_and_the_next_configure_writes_again()
    {
        var dialect = new BinaryEventDialect(new ZeroCopyStorageDialect())
        {
            WriteFailure = new InvalidOperationException("serializer failed")
        };
        var bytes = payload(400, 2);
        var operation = build(dialect, PerEventPath.RichAppend, binary(bytes), theSession);

        Should.Throw<InvalidOperationException>(() => bdataParameter(operation)).Message.ShouldBe("serializer failed");

        // Nothing half-written was kept
        bdataParameter(operation).Value.ShouldBeOfType<ReadOnlyMemory<byte>>().ToArray().ShouldBe(bytes);
        dialect.WriteCalls.ShouldBe(2);
    }

    [Theory]
    [InlineData(PerEventPath.RichAppend)]
    [InlineData(PerEventPath.RichSideEffectReplay)]
    [InlineData(PerEventPath.QuickTombstone)]
    public void a_binary_append_no_longer_allocates_its_payload(PerEventPath path)
    {
        // "Done when ... measured as allocation per append". Synchronous, so every rental and return is on
        // this thread and GetAllocatedBytesForCurrentThread sees all of it.
        const int length = 1024;
        const int appends = 300;
        var bytes = payload(length, 11);

        long perAppend(bool useBufferSlot)
        {
            var dialect = new BinaryEventDialect(new ZeroCopyStorageDialect()) { UseBufferSlot = useBufferSlot };
            var storage = path switch
            {
                PerEventPath.QuickTombstone => EventStorageBuilder.Build<Guid>(dialect, EventAppendMode.Quick, null!, null!),
                _ => EventStorageBuilder.Build<Guid>(dialect, EventAppendMode.Rich, null!, null!)
            };
            var events = Enumerable.Range(0, appends).Select(i => binary(bytes, i + 1)).ToArray();
            var stream = StreamAction.Append(Guid.NewGuid(), events);

            void append(IEvent e)
            {
                var operation = path == PerEventPath.RichSideEffectReplay
                    ? storage.QuickAppendEventWithVersion(stream, e)
                    : storage.AppendEvent(theSession, stream, e);
                var builder = new Weasel.Postgresql.CommandBuilder();
                operation.ConfigureCommand(builder, theSession);
                (operation as IDisposable)?.Dispose();
            }

            // Warm the pool and the JIT
            for (var i = 0; i < 20; i++)
            {
                append(events[i]);
            }

            var before = GC.GetAllocatedBytesForCurrentThread();
            foreach (var e in events)
            {
                append(e);
            }

            return (GC.GetAllocatedBytesForCurrentThread() - before) / appends;
        }

        var withArrays = perAppend(useBufferSlot: false);
        var pooled = perAppend(useBufferSlot: true);
        _output.WriteLine($"{path}: {withArrays} bytes per append with the byte[] slot, {pooled} pooled");

        // The array slot allocates the payload plus its array header; the pooled slot allocates neither,
        // only the buffer-writer object and a boxed ReadOnlyMemory<byte>.
        (withArrays - pooled).ShouldBeGreaterThan(length - 128);
    }
}
