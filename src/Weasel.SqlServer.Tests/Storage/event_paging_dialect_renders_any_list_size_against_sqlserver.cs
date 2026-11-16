using System.Data;
using System.Data.Common;
using System.Text.Json;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core.Tests.StorageHelpers;
using Weasel.Storage;
using Xunit;

namespace Weasel.SqlServer.Tests.Storage;

/// <summary>
///     weasel#846: <see cref="EventLoaderBase"/> filters only in SQL, so an <see cref="IEventPagingDialect"/>
///     must render an allow list of any length. SQL Server refuses a request with more than 2,100
///     parameters. A dialect binding one per name kills the shard; one binding the list as a single JSON
///     parameter and expanding it with OPENJSON reads the right rows at 2,500 names.
/// </summary>
public class event_paging_dialect_renders_any_list_size_against_sqlserver: IAsyncLifetime
{
    // Per test class, and in the lane's own database, so no other class or lane touches it.
    private const string Table = "dbo.weasel_846_events";

    public async ValueTask InitializeAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            drop table if exists {Table};
            create table {Table} (
                seq_id bigint not null primary key,
                type varchar(500) not null,
                is_archived bit not null default 0
            );
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
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"drop table if exists {Table}";
        await cmd.ExecuteNonQueryAsync();
    }

    private static EventRequest RequestFor(long floor, long highWater) => new()
    {
        Floor = floor, HighWater = highWater, BatchSize = 10, ErrorOptions = new ErrorHandlingOptions()
    };

    [Fact]
    public async Task a_list_bound_as_one_json_parameter_reads_the_right_rows_at_2500_names()
    {
        var allowList = LargeEventTypeAllowList.Build();
        allowList.AliasesIncludingUpcastSources.Count.ShouldBe(LargeEventTypeAllowList.Size);

        var dialect = new OpenJsonDialect();
        var loader = new Loader(dialect, allowList);

        var page = await loader.LoadAsync(RequestFor(0, 10), TestContext.Current.CancellationToken);

        page.Select(x => x.Sequence).ShouldBe([1, 3, 5]);
        loader.HydratedTypes.ShouldNotContain("unwanted");
        page.Ceiling.ShouldBe(10);

        // The whole list is one parameter, whatever its length.
        dialect.BuildPageCommand(new EventPageQuery { Floor = 0, Ceiling = 10, BatchSize = 10, EventTypes = allowList })
            .Parameters.Count.ShouldBe(4);
    }

    [Fact]
    public async Task the_skip_ahead_probe_takes_the_same_filter()
    {
        var loader = new Loader(new OpenJsonDialect(), LargeEventTypeAllowList.Build());

        (await loader.ProbeAsync(1, 10)).ShouldBe(3);
        (await loader.ProbeAsync(3, 10)).ShouldBe(5);
        (await loader.ProbeAsync(5, 10)).ShouldBeNull();
    }

    [Fact]
    public async Task one_parameter_per_name_is_refused_by_the_server()
    {
        // The negative control, and the reason the contract exists: this is the shard that dies.
        var loader = new Loader(new ParameterPerNameDialect(), LargeEventTypeAllowList.Build());

        var refused = await Should.ThrowAsync<SqlException>(() =>
            loader.LoadAsync(RequestFor(0, 10), TestContext.Current.CancellationToken));

        refused.Message.ShouldContain("2100");
    }

    private abstract class Dialect: IEventPagingDialect
    {
        public EventPageCommand BuildPageCommand(EventPageQuery query)
        {
            var (where, parameters) = Filters(query);
            return new EventPageCommand(
                $"select top (@batch) seq_id, type from {Table} where {where} order by seq_id",
                [.. parameters, new EventPageParameter("@batch", query.BatchSize, DbType.Int32)]);
        }

        public EventPageCommand BuildNextMatchingSequenceCommand(EventPageQuery query)
        {
            var (where, parameters) = Filters(query);
            return new EventPageCommand($"select top (1) seq_id from {Table} where {where} order by seq_id", parameters);
        }

        private (string, List<EventPageParameter>) Filters(EventPageQuery query)
        {
            var clauses = new List<string> { "seq_id > @floor", "seq_id <= @ceiling", "is_archived = 0" };
            var parameters = new List<EventPageParameter>
            {
                new("@floor", query.Floor, DbType.Int64), new("@ceiling", query.Ceiling, DbType.Int64)
            };

            if (!query.EventTypes.IsEmpty)
            {
                clauses.Add(TypeFilter(query.EventTypes.AliasesIncludingUpcastSources, parameters));
            }

            return (string.Join(" and ", clauses), parameters);
        }

        protected abstract string TypeFilter(IReadOnlyList<string> aliases, List<EventPageParameter> parameters);
    }

    /// <summary>What IEventPagingDialect recommends for SQL Server.</summary>
    private sealed class OpenJsonDialect: Dialect
    {
        protected override string TypeFilter(IReadOnlyList<string> aliases, List<EventPageParameter> parameters)
        {
            parameters.Add(new EventPageParameter("@types", JsonSerializer.Serialize(aliases), DbType.String));

            // Typed to the column, so the comparison does not convert the column.
            return "type in (select value from openjson(@types) with (value varchar(500) '$'))";
        }
    }

    private sealed class ParameterPerNameDialect: Dialect
    {
        protected override string TypeFilter(IReadOnlyList<string> aliases, List<EventPageParameter> parameters)
        {
            parameters.AddRange(aliases.Select((alias, i) => new EventPageParameter($"@t{i}", alias)));
            return $"type in ({string.Join(", ", aliases.Select((_, i) => $"@t{i}"))})";
        }
    }

    private sealed class Loader(IEventPagingDialect dialect, EventTypeAllowList allowList)
        : EventLoaderBase(dialect, new EventLoaderOptions { EventTypes = allowList })
    {
        public List<string> HydratedTypes { get; } = [];

        public Task<long?> ProbeAsync(long floor, long ceiling) =>
            ProbeNextMatchingSequenceAsync(floor, ceiling, 10, TestContext.Current.CancellationToken);

        protected override async ValueTask<DbConnection> OpenConnectionAsync(CancellationToken token)
        {
            var conn = new SqlConnection(ConnectionSource.ConnectionString);
            await conn.OpenAsync(token);
            return conn;
        }

        protected override ValueTask<IEvent?> ReadEventAsync(DbDataReader reader, CancellationToken token)
        {
            var type = reader.GetString(1);
            HydratedTypes.Add(type);
            return ValueTask.FromResult<IEvent?>(
                new Event<string>(type) { Sequence = reader.GetInt64(0), EventTypeName = type });
        }
    }
}
