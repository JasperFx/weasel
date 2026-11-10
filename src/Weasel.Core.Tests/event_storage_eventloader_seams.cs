using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Exceptions;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using JasperFx.Events.Upcasting;
using Microsoft.Data.Sqlite;
using NSubstitute;
using Shouldly;
using Weasel.Storage;
using Xunit;
using SqliteCommandBuilder = Weasel.Sqlite.CommandBuilder;

namespace Weasel.Core.Tests;

/// <summary>
/// weasel#743: the allow list's upcast sources and framework markers, the shared row reader, and the
/// per-event Quick append base with metadata binders.
/// </summary>
public class event_storage_eventloader_seams
{
    public sealed record OrderPlacedV1(string Sku);
    public sealed record OrderPlaced(string Sku, int Quantity);
    public sealed record OrderShipped(string Sku);
    public sealed record Unrelated(string Name);
    public sealed class OrderAggregate;

    /// <summary>Alias = the CLR type's simple name, snake-free; enough for these tests.</summary>
    private sealed class NameRegistry: EventRegistry
    {
        public override IEventType EventMappingFor(Type eventType)
        {
            var data = (IEventType)Activator.CreateInstance(typeof(EventTypeData<>).MakeGenericType(eventType))!;
            var property = data.GetType();
            property.GetProperty(nameof(IEventType.EventTypeName))!.SetValue(data, AliasOf(eventType));
            property.GetProperty(nameof(IEventType.DotNetTypeName))!.SetValue(data, eventType.AssemblyQualifiedName);
            return data;
        }

        public static string AliasOf(Type type) => type.IsGenericType
            ? type.Name[..type.Name.IndexOf('`')] + "_" + type.GetGenericArguments()[0].Name
            : type.Name;
    }

    private static NameRegistry RegistryWithUpcaster()
    {
        var registry = new NameRegistry();
        registry.Upcasters.Upcast<OrderPlacedV1, OrderPlaced>("order_placed_v1", old => new OrderPlaced(old.Sku, 1));
        return registry;
    }

    // ------------------------------------------------------------------------------------------
    // EventTypeAllowList
    // ------------------------------------------------------------------------------------------

    [Fact]
    public void an_upcast_source_of_an_allowed_type_is_on_the_list()
    {
        var list = EventTypeAllowList.For(RegistryWithUpcaster(), [typeof(OrderPlaced)]);

        list.Aliases.ShouldBe(["OrderPlaced"]);
        list.UpcastSourceAliases.ShouldBe(["order_placed_v1"]);
        list.AliasesIncludingUpcastSources.ShouldBe(["OrderPlaced", "order_placed_v1"]);

        // A source has no CLR type to name, so it never lands in the dotnet_type list.
        list.DotNetTypeNames.ShouldBe([typeof(OrderPlaced).AssemblyQualifiedName!]);
    }

    [Fact]
    public void an_upcast_source_of_a_type_the_shard_does_not_read_is_not()
    {
        var list = EventTypeAllowList.For(RegistryWithUpcaster(), [typeof(OrderShipped)]);

        list.UpcastSourceAliases.ShouldBeEmpty();
        list.AliasesIncludingUpcastSources.ShouldBe(["OrderShipped"]);
    }

    [Fact]
    public void no_upcasters_leaves_the_list_as_it_was()
    {
        var list = EventTypeAllowList.For(new NameRegistry(), [typeof(OrderPlaced)]);

        list.UpcastSourceAliases.ShouldBeEmpty();
        list.AliasesIncludingUpcastSources.ShouldBeSameAs(list.Aliases);
    }

    [Fact]
    public void upcast_sources_never_turn_an_unfiltered_shard_into_a_filtered_one()
    {
        var list = EventTypeAllowList.For(RegistryWithUpcaster(), (EventFilterable?)null);

        list.IsEmpty.ShouldBeTrue();
        list.UpcastSourceAliases.ShouldBeEmpty();
    }

    [Fact]
    public void a_single_stream_aggregate_projection_also_reads_archived_and_compacted()
    {
        var projection = Projection(AggregationScope.SingleStream, typeof(OrderPlaced));

        var list = EventTypeAllowList.For(new NameRegistry(), projection);

        list.Aliases.ShouldBe(["OrderPlaced", "Archived", "Compacted_OrderAggregate"]);
        list.DotNetTypeNames.ShouldContain(typeof(Compacted<OrderAggregate>).AssemblyQualifiedName!);
    }

    [Fact]
    public void the_markers_are_not_added_twice_when_upstream_already_declared_them()
    {
        var projection = Projection(AggregationScope.SingleStream, typeof(OrderPlaced), typeof(Archived),
            typeof(Compacted<OrderAggregate>));

        var list = EventTypeAllowList.For(new NameRegistry(), projection);

        list.Aliases.ShouldBe(["OrderPlaced", "Archived", "Compacted_OrderAggregate"]);
    }

    [Fact]
    public void a_multi_stream_projection_gets_no_markers()
    {
        var projection = Projection(AggregationScope.MultiStream, typeof(OrderPlaced));

        EventTypeAllowList.For(new NameRegistry(), projection).Aliases.ShouldBe(["OrderPlaced"]);
    }

    [Fact]
    public void a_plain_filterable_gets_no_markers()
    {
        var filterable = new EventFilterable();
        filterable.IncludeType<OrderPlaced>();

        EventTypeAllowList.FrameworkMarkerTypes(filterable).ShouldBeEmpty();
        EventTypeAllowList.For(new NameRegistry(), filterable).Aliases.ShouldBe(["OrderPlaced"]);
    }

    [Fact]
    public void an_unfiltered_single_stream_projection_stays_unfiltered()
    {
        var projection = Projection(AggregationScope.SingleStream);

        EventTypeAllowList.For(new NameRegistry(), projection).IsEmpty.ShouldBeTrue();
    }

    private static EventFilterable Projection(AggregationScope scope, params Type[] eventTypes)
    {
        var projection = Substitute.For<EventFilterable, IAggregateProjection>();
        ((IAggregateProjection)projection).Scope.Returns(scope);
        ((IAggregateProjection)projection).AggregateType.Returns(typeof(OrderAggregate));

        foreach (var type in eventTypes)
        {
            projection.IncludedEventTypes.Add(type);
        }

        return projection;
    }

    // ------------------------------------------------------------------------------------------
    // EventRowReaderBase
    // ------------------------------------------------------------------------------------------

    private static async Task<T> WithRowAsync<T>(string type, string? dotNetType, string json,
        Func<DbDataReader, Task<T>> read)
    {
        await using var conn = new SqliteConnection("Data Source=:memory:");
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            select 7 as seq_id, @id as id, @stream as stream_id, 3 as version, @json as data,
                   @type as type, @dotnet as dotnet_type, 'tenant-a' as tenant_id, 1 as is_archived,
                   'corr-1' as correlation_id
            """;
        cmd.Parameters.AddWithValue("@id", EventId.ToString());
        cmd.Parameters.AddWithValue("@stream", StreamId.ToString());
        cmd.Parameters.AddWithValue("@json", json);
        cmd.Parameters.AddWithValue("@type", type);
        cmd.Parameters.AddWithValue("@dotnet", (object?)dotNetType ?? DBNull.Value);

        await using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();
        return await read(reader);
    }

    private static readonly Guid EventId = Guid.NewGuid();
    private static readonly Guid StreamId = Guid.NewGuid();

    private sealed class TestRowReader(EventRegistry registry): EventRowReaderBase(registry)
    {
        protected override EventRow ReadRow(DbDataReader reader) => new()
        {
            Sequence = reader.GetInt64(0),
            Id = Guid.Parse(reader.GetString(1)),
            StreamId = Guid.Parse(reader.GetString(2)),
            Version = reader.GetInt64(3),
            Json = reader.GetString(4),
            EventTypeName = reader.GetString(5),
            DotNetTypeName = reader.IsDBNull(6) ? null : reader.GetString(6),
            Timestamp = DateTimeOffset.UnixEpoch,
            TenantId = reader.GetString(7),
            IsArchived = reader.GetInt64(8) == 1
        };

        protected override IUpcastPayload CreateUpcastPayload(EventRow row) => new JsonPayload(row.Json!);

        protected override object DeserializeData(Type eventType, EventRow row)
            => JsonSerializer.Deserialize(row.Json!, eventType)!;

        protected override void ReadMetadata(DbDataReader reader, IEvent @event)
            => @event.CorrelationId = reader.GetString(9);
    }

    private sealed class JsonPayload(string json): IUpcastPayload
    {
        public T As<T>() where T : notnull => JsonSerializer.Deserialize<T>(json)!;

        public ValueTask<T> AsAsync<T>(CancellationToken token) where T : notnull => new(As<T>());

        public JsonDocument AsJsonDocument() => JsonDocument.Parse(json);

        public ValueTask<JsonDocument> AsJsonDocumentAsync(CancellationToken token) => new(AsJsonDocument());
    }

    [Fact]
    public async Task the_row_reader_hydrates_and_stamps_the_row()
    {
        var @event = await WithRowAsync("OrderPlaced", typeof(OrderPlaced).AssemblyQualifiedName,
            """{"Sku":"abc","Quantity":2}""",
            r => new TestRowReader(new NameRegistry()).ReadAsync(r, CancellationToken.None).AsTask());

        @event.ShouldNotBeNull();
        @event.Data.ShouldBe(new OrderPlaced("abc", 2));
        @event.Id.ShouldBe(EventId);
        @event.StreamId.ShouldBe(StreamId);
        @event.Sequence.ShouldBe(7);
        @event.Version.ShouldBe(3);
        @event.TenantId.ShouldBe("tenant-a");
        @event.EventTypeName.ShouldBe("OrderPlaced");
        @event.IsArchived.ShouldBeTrue();
        @event.CorrelationId.ShouldBe("corr-1");
    }

    [Fact]
    public async Task the_row_reader_upcasts_before_it_trusts_the_stored_dotnet_type()
    {
        // The legacy row's old CLR type still resolves. Upcast-first is what keeps that hint from
        // shadowing the registered transformation (marten#4680).
        var registry = RegistryWithUpcaster();

        var asyncEvent = await WithRowAsync("order_placed_v1", typeof(OrderPlacedV1).AssemblyQualifiedName,
            """{"Sku":"old"}""", r => new TestRowReader(registry).ReadAsync(r, CancellationToken.None).AsTask());
        var syncEvent = await WithRowAsync("order_placed_v1", typeof(OrderPlacedV1).AssemblyQualifiedName,
            """{"Sku":"old"}""", r => Task.FromResult(new TestRowReader(registry).Read(r)));

        asyncEvent!.Data.ShouldBe(new OrderPlaced("old", 1));
        syncEvent!.Data.ShouldBe(new OrderPlaced("old", 1));

        // Both stored names are preserved as stored, as Polecat's loader does: the alias and the
        // dotnet_type describe the ROW, while Data is the upcast target.
        asyncEvent.EventTypeName.ShouldBe("order_placed_v1");
        asyncEvent.DotNetTypeName.ShouldBe(typeof(OrderPlacedV1).AssemblyQualifiedName);
    }

    [Fact]
    public async Task an_unresolvable_type_reads_as_null_and_leaves_the_policy_to_the_caller()
    {
        var @event = await WithRowAsync("ghost", "Nowhere.Ghost, Nowhere", "{}",
            r => new TestRowReader(new NameRegistry()).ReadAsync(r, CancellationToken.None).AsTask());

        @event.ShouldBeNull();
    }

    [Fact]
    public async Task an_unreadable_body_is_classified_as_a_serialization_failure()
    {
        var ex = await Should.ThrowAsync<EventDeserializationFailureException>(() => WithRowAsync(
            "OrderPlaced", typeof(OrderPlaced).AssemblyQualifiedName, "not json",
            r => new TestRowReader(new NameRegistry()).ReadAsync(r, CancellationToken.None).AsTask()));

        ex.Sequence.ShouldBe(7);
        EventLoaderBase.Classify(ex).ShouldBe(ShardFailureCategory.EventSerialization);
    }

    [Fact]
    public async Task an_upcast_that_throws_is_a_serialization_failure_too()
    {
        var registry = new NameRegistry();
        registry.Upcasters.Upcast<OrderPlacedV1, OrderPlaced>("order_placed_v1",
            _ => throw new InvalidOperationException("bad shape"));

        var ex = await Should.ThrowAsync<EventDeserializationFailureException>(() => WithRowAsync(
            "order_placed_v1", null, """{"Sku":"old"}""",
            r => new TestRowReader(registry).ReadAsync(r, CancellationToken.None).AsTask()));

        EventLoaderBase.Classify(ex).ShouldBe(ShardFailureCategory.EventSerialization);
    }

    // ------------------------------------------------------------------------------------------
    // PerEventQuickAppendOperationBase
    // ------------------------------------------------------------------------------------------

    private sealed class LiteralBinder(string column, string valueSql): IEventMetadataBinder
    {
        public string ColumnName => column;
        public string ValueSql => valueSql;

        public void Bind(IGroupedParameterBuilder pb, StreamAction stream, IEvent @event, IStorageSession session)
            => throw new InvalidOperationException("A server-side literal binds nothing.");
    }

    private sealed class CorrelationBinder: IEventMetadataBinder
    {
        public string ColumnName => "correlation_id";
        public string ValueSql => "?";

        public void Bind(IGroupedParameterBuilder pb, StreamAction stream, IEvent @event, IStorageSession session)
            => pb.AppendParameter(@event.CorrelationId);
    }

    /// <summary>A SQLite-shaped store: re-selects the rows it wrote, as Fisher does.</summary>
    private sealed class SqliteQuickAppend(StreamAction stream, IEventMetadataBinder[] binders)
        : PerEventQuickAppendOperationBase(stream, binders)
    {
        private const string CoreColumns = "stream_id, version, type";

        protected override void WriteInsertStream(ICommandBuilder builder, IStorageSession session)
        {
            builder.Append("insert into streams (id, version) values (");
            builder.AppendParameter(Stream.Id.ToString());
            builder.Append(", ");
            builder.AppendParameter(Stream.Events[^1].Version);
            builder.Append(");");
        }

        protected override void WriteUpdateStreamVersion(ICommandBuilder builder, IStorageSession session)
        {
            builder.Append("update streams set version = ");
            builder.AppendParameter(Stream.Events[^1].Version);
            builder.Append(" where id = ");
            builder.AppendParameter(Stream.Id.ToString());
            builder.Append(";");
        }

        protected override void WriteEventInsertPrefix(ICommandBuilder builder, IEvent @event, int ordinal)
        {
            builder.Append("insert into events (");
            builder.Append(ComposeColumnList(CoreColumns, MetadataBinders));
            builder.Append(") values (");
        }

        protected override void WriteCoreValues(ICommandBuilder builder, IEvent @event, IStorageSession session)
        {
            builder.AppendParameter(Stream.Id.ToString());
            builder.Append(", ");
            builder.AppendParameter(@event.Version);
            builder.Append(", ");
            builder.AppendParameter(@event.EventTypeName);
        }

        protected override void WriteSequenceReadback(ICommandBuilder builder, IStorageSession session)
        {
            builder.Append("select seq_id from events where stream_id = ");
            builder.AppendParameter(Stream.Id.ToString());
            builder.Append(" order by version;");
        }

        protected override Exception? TransformInsertStreamException(Exception original)
            => original is SqliteException { SqliteExtendedErrorCode: 1555 }
                ? new ExistingStreamIdCollisionException(Stream.Id, null)
                : null;
    }

    private static StreamAction StreamOf(Guid id, long firstVersion, params string[] types)
    {
        var events = types.Select((type, i) => (IEvent)new Event<OrderShipped>(new OrderShipped(type))
        {
            Version = firstVersion + i, EventTypeName = type, CorrelationId = $"corr-{type}"
        }).ToArray();

        return StreamAction.Start(id, events);
    }

    private static async Task<SqliteConnection> EventsDatabaseAsync()
    {
        var conn = new SqliteConnection("Data Source=:memory:");
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            create table streams (id text primary key, version integer);
            create table events (seq_id integer primary key autoincrement, stream_id text, version integer,
                                 type text, correlation_id text, source text);
            insert into events (stream_id, version, type) values ('someone-else', 1, 'noise');
            """;
        await cmd.ExecuteNonQueryAsync();
        return conn;
    }

    private static async Task ExecuteAsync(SqliteConnection conn, PerEventQuickAppendOperationBase operation)
    {
        var builder = new SqliteCommandBuilder();
        operation.ConfigureCommand(builder, Substitute.For<IStorageSession>());

        await using var cmd = builder.Compile();
        cmd.Connection = conn;

        await using var reader = await cmd.ExecuteReaderAsync();
        await operation.PostprocessAsync(reader, new List<Exception>(), CancellationToken.None);
    }

    [Fact]
    public async Task the_binders_fill_their_columns_in_order_and_the_sequences_come_back()
    {
        await using var conn = await EventsDatabaseAsync();
        var stream = StreamOf(Guid.NewGuid(), 1, "a", "b", "c");
        var operation = new SqliteQuickAppend(stream,
            [new CorrelationBinder(), new LiteralBinder("source", "'server'")]);

        await ExecuteAsync(conn, operation);

        stream.Events.Select(x => x.Sequence).ShouldBe([2L, 3L, 4L]);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select type, correlation_id, source from events where seq_id > 1 order by seq_id";
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<string>();
        while (await reader.ReadAsync())
        {
            rows.Add($"{reader.GetString(0)}|{reader.GetString(1)}|{reader.GetString(2)}");
        }

        rows.ShouldBe(["a|corr-a|server", "b|corr-b|server", "c|corr-c|server"]);
    }

    [Fact]
    public async Task the_update_mode_bumps_the_existing_stream_row()
    {
        await using var conn = await EventsDatabaseAsync();
        var id = Guid.NewGuid();

        await ExecuteAsync(conn, new SqliteQuickAppend(StreamOf(id, 1, "a"), []));
        await ExecuteAsync(conn, new SqliteQuickAppend(StreamOf(id, 2, "b", "c"), []) { Mode = StreamRowWrite.Update });

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select version from streams";
        (await cmd.ExecuteScalarAsync()).ShouldBe(3L);
    }

    [Fact]
    public void the_column_list_follows_the_binder_order()
    {
        PerEventQuickAppendOperationBase.ComposeColumnList("id, version",
                [new CorrelationBinder(), new LiteralBinder("source", "'x'")])
            .ShouldBe("id, version, correlation_id, source");
    }

    [Fact]
    public async Task only_a_stream_insert_is_offered_to_the_collision_transform()
    {
        await using var conn = await EventsDatabaseAsync();
        var id = Guid.NewGuid();
        await ExecuteAsync(conn, new SqliteQuickAppend(StreamOf(id, 1, "a"), []));

        var duplicate = new SqliteQuickAppend(StreamOf(id, 1, "a"), []);
        var ex = await Should.ThrowAsync<SqliteException>(() => ExecuteAsync(conn, duplicate));

        duplicate.ShouldBeAssignableTo<IExceptionTransform>();
        duplicate.TryTransform(ex, out var transformed).ShouldBeTrue();
        transformed.ShouldBeOfType<ExistingStreamIdCollisionException>();

        duplicate.Mode = StreamRowWrite.Update;
        duplicate.TryTransform(ex, out _).ShouldBeFalse();
    }
}
