using System.Data;
using System.Data.Common;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using Npgsql;
using Shouldly;
using Weasel.Core.Tests.StorageHelpers;
using Weasel.Storage;
using Xunit;

namespace Weasel.Postgresql.Tests.Storage;

/// <summary>
///     weasel#846 on PostgreSQL: the allow list bound as one <c>text[]</c> parameter and matched with
///     <c>= any(@types)</c>, the shape <see cref="IEventPagingDialect"/> recommends, at 2,500 names.
/// </summary>
[Collection("weasel_846_paging")]
public class event_paging_dialect_renders_any_list_size_against_postgresql: IAsyncLifetime
{
    // Per test class, in the lane's own database.
    private const string Table = "public.weasel_846_events";

    public async ValueTask InitializeAsync()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            drop table if exists {Table};
            create table {Table} (seq_id bigint primary key, type varchar(500) not null, is_archived boolean not null default false);
            insert into {Table} (seq_id, type) values
                (1, '{LargeEventTypeAllowList.AliasAt(0)}'),
                (2, 'unwanted'),
                (3, '{LargeEventTypeAllowList.AliasAt(LargeEventTypeAllowList.Size - 1)}'),
                (4, 'unwanted'),
                (5, '{LargeEventTypeAllowList.AliasAt(2100)}');
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"drop table if exists {Table}";
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task a_list_bound_as_one_array_parameter_reads_the_right_rows_at_2500_names()
    {
        var loader = new Loader(new AnyArrayDialect(), LargeEventTypeAllowList.Build());

        var page = await loader.LoadAsync(new EventRequest
        {
            Floor = 0, HighWater = 10, BatchSize = 10, ErrorOptions = new ErrorHandlingOptions()
        }, TestContext.Current.CancellationToken);

        page.Select(x => x.Sequence).ShouldBe([1, 3, 5]);
        page.Ceiling.ShouldBe(10);
        (await loader.ProbeAsync(1, 10)).ShouldBe(3);
    }

    private sealed class AnyArrayDialect: IEventPagingDialect
    {
        public EventPageCommand BuildPageCommand(EventPageQuery query)
        {
            var (where, parameters) = Filters(query);
            return new EventPageCommand($"select seq_id, type from {Table} where {where} order by seq_id limit @batch",
                [.. parameters, new EventPageParameter("batch", query.BatchSize, DbType.Int32)]);
        }

        public EventPageCommand BuildNextMatchingSequenceCommand(EventPageQuery query)
        {
            var (where, parameters) = Filters(query);
            return new EventPageCommand($"select seq_id from {Table} where {where} order by seq_id limit 1", parameters);
        }

        private static (string, List<EventPageParameter>) Filters(EventPageQuery query)
        {
            var clauses = new List<string> { "seq_id > @floor", "seq_id <= @ceiling", "is_archived = false" };
            var parameters = new List<EventPageParameter>
            {
                new("floor", query.Floor, DbType.Int64), new("ceiling", query.Ceiling, DbType.Int64)
            };

            if (!query.EventTypes.IsEmpty)
            {
                // One parameter, whatever the list's length.
                clauses.Add("type = any(@types)");
                parameters.Add(new EventPageParameter("types", query.EventTypes.AliasesIncludingUpcastSources.ToArray()));
            }

            return (string.Join(" and ", clauses), parameters);
        }
    }

    private sealed class Loader(IEventPagingDialect dialect, EventTypeAllowList allowList)
        : EventLoaderBase(dialect, new EventLoaderOptions { EventTypes = allowList })
    {
        public Task<long?> ProbeAsync(long floor, long ceiling) =>
            ProbeNextMatchingSequenceAsync(floor, ceiling, 10, TestContext.Current.CancellationToken);

        protected override async ValueTask<DbConnection> OpenConnectionAsync(CancellationToken token)
        {
            var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
            await conn.OpenAsync(token);
            return conn;
        }

        protected override ValueTask<IEvent?> ReadEventAsync(DbDataReader reader, CancellationToken token)
        {
            var type = reader.GetString(1);
            return ValueTask.FromResult<IEvent?>(
                new Event<string>(type) { Sequence = reader.GetInt64(0), EventTypeName = type });
        }
    }
}
