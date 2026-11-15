using System.Buffers;
using JasperFx.Events;
using Npgsql;
using NSubstitute;
using Shouldly;
using Weasel.Core.Tests.StorageHelpers;
using Weasel.Storage;
using Xunit;
using IStorageOperation = Weasel.Storage.IStorageOperation;

namespace Weasel.Postgresql.Tests.Storage;

/// <summary>
///     weasel#831. With <c>WriteEventBdata</c>, a per-event append binds a slice of a pooled buffer, which
///     the operation holds until it is disposed. A store's resilience pipeline re-executes the same batch on a
///     retry, so a buffer returned to the pool before then would send whatever the pool's next renter wrote
///     there: marten#5262's data-loss class. Each test runs the batch, rolls it back as a failed attempt
///     would, churns the pool, and runs the same batch again.
/// </summary>
public class pooled_event_bdata_survives_a_retry: IAsyncLifetime
{
    // Server-scoped, so a name no other test class uses (CLAUDE.md: lanes share the server).
    private const string Schema = "weasel831_pooled_bdata";
    private const byte Churn = 0xEE;

    private readonly NpgsqlConnection theConnection = new(ConnectionSource.ConnectionString);
    private readonly IStorageSession theSession = Substitute.For<IStorageSession>();

    public async ValueTask InitializeAsync()
    {
        await theConnection.OpenAsync();
        await using var command = theConnection.CreateCommand();
        command.CommandText = $"""
            drop schema if exists {Schema} cascade;
            create schema {Schema};
            create table {Schema}.events (
                data jsonb, type text, mt_dotnet_type text, bdata bytea, id uuid, stream_id uuid,
                version bigint, timestamp timestamptz, tenant_id text);
            """;
        await command.ExecuteNonQueryAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await using (var command = theConnection.CreateCommand())
        {
            command.CommandText = $"drop schema if exists {Schema} cascade";
            await command.ExecuteNonQueryAsync();
        }

        await theConnection.DisposeAsync();
    }

    private static byte[] payload(byte seed)
    {
        // Past the buffer's initial 512-byte rental, so writing it grows the buffer and returns a rental
        // to the pool mid-write, which is exactly when a slice taken too early would go stale.
        var bytes = new byte[1500];
        for (var i = 0; i < bytes.Length; i++)
        {
            bytes[i] = (byte)(seed + i);
        }

        return bytes;
    }

    private (IStorageOperation[] Operations, Dictionary<long, byte[]?> Expected) operations(EventAppendMode mode,
        bool replay)
    {
        var dialect = new BinaryEventDialect(new ZeroCopyStorageDialect(),
            $"insert into {Schema}.events (data, type, mt_dotnet_type, bdata, id, stream_id, version, timestamp, tenant_id) values (");
        var storage = EventStorageBuilder.Build<Guid>(dialect, mode, null!, null!);

        IEvent[] events =
        [
            new Event<BinaryHappened>(new BinaryHappened(payload(1))),
            new Event<JsonHappened>(new JsonHappened("json")),
            new Event<BinaryHappened>(new BinaryHappened(payload(50))),
            new Event<BinaryHappened>(new BinaryHappened(payload(100)))
        ];

        for (var i = 0; i < events.Length; i++)
        {
            events[i].Version = i + 1;
            events[i].Sequence = i + 1;
            events[i].Id = Guid.NewGuid();
            events[i].Timestamp = DateTimeOffset.UtcNow;
        }

        var stream = StreamAction.Append(Guid.NewGuid(), events);
        var built = events
            .Select(e => replay ? storage.QuickAppendEventWithVersion(stream, e) : storage.AppendEvent(theSession, stream, e))
            .ToArray();

        return (built, events.ToDictionary(e => e.Version, e => (e.Data as BinaryHappened)?.Payload));
    }

    private NpgsqlBatch compile(IReadOnlyList<IStorageOperation> operations)
    {
        var builder = new BatchBuilder();
        OperationBatchExecutor.Configure(builder, operations, theSession);
        var batch = builder.Compile();
        batch.Connection = theConnection;
        return batch;
    }

    private async Task executeAsync(NpgsqlBatch batch, bool commit)
    {
        await using var tx = await theConnection.BeginTransactionAsync();
        batch.Transaction = tx;
        await batch.ExecuteNonQueryAsync();

        if (commit)
        {
            await tx.CommitAsync();
        }
        else
        {
            await tx.RollbackAsync();
        }
    }

    // Rent everything the pool will hand out at these sizes -- this thread's cached array and the
    // per-core stacks -- scribble over it, and give it back.
    private static void churnThePool()
    {
        foreach (var size in new[] { 512, 1024, 2048, 4096 })
        {
            var rentals = new List<byte[]>();
            for (var i = 0; i < 64; i++)
            {
                var rented = ArrayPool<byte>.Shared.Rent(size);
                rented.AsSpan().Fill(Churn);
                rentals.Add(rented);
            }

            foreach (var rented in rentals)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private async Task<Dictionary<long, byte[]?>> readBackAsync()
    {
        var written = new Dictionary<long, byte[]?>();
        await using var command = theConnection.CreateCommand();
        command.CommandText = $"select version, bdata from {Schema}.events order by version";
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            written[reader.GetInt64(0)] = await reader.IsDBNullAsync(1) ? null : reader.GetFieldValue<byte[]>(1);
        }

        return written;
    }

    private static void dispose(IEnumerable<IStorageOperation> operations)
    {
        foreach (var operation in operations)
        {
            (operation as IDisposable)?.Dispose();
        }
    }

    [Theory]
    [InlineData(EventAppendMode.Rich, false)] // the Rich append
    [InlineData(EventAppendMode.Rich, true)] // side-effect replay
    [InlineData(EventAppendMode.Quick, false)] // a tombstone
    [InlineData(EventAppendMode.QuickWithServerTimestamps, false)]
    public async Task a_retried_batch_still_writes_every_binary_payload(EventAppendMode mode, bool replay)
    {
        var (operations, expected) = this.operations(mode, replay);
        try
        {
            var batch = compile(operations);

            await executeAsync(batch, commit: false); // the failed attempt
            churnThePool();
            await executeAsync(batch, commit: true); // the retry, of the very same batch

            var written = await readBackAsync();
            written.Keys.ShouldBe(expected.Keys);
            foreach (var (version, bytes) in expected)
            {
                written[version].ShouldBe(bytes, $"bdata of version {version}");
            }
        }
        finally
        {
            // After the unit of work, retries included -- never before.
            dispose(operations);
        }
    }

    [Fact]
    public async Task returning_the_buffers_before_the_retry_writes_another_payload_s_bytes()
    {
        // The guard above can fail: release the rentals between the attempts, as a store disposing per
        // execution would, and the retry sends the bytes the churn wrote over them.
        var (operations, expected) = this.operations(EventAppendMode.Rich, replay: false);
        var batch = compile(operations);

        await executeAsync(batch, commit: false);

        // Synchronously, with no await between: the rentals go back to this thread's pool cache and the
        // churn takes them straight out again.
        dispose(operations);
        churnThePool();

        await executeAsync(batch, commit: true);

        var written = await readBackAsync();
        written.Keys.ShouldBe(expected.Keys);
        written.Values.Where(x => x is not null).ShouldContain(x => x!.All(b => b == Churn),
            "a payload's bytes should have been replaced by the churn's");
    }
}
