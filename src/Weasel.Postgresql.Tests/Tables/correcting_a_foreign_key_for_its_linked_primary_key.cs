using System.Threading;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
/// <see cref="Table.PostProcess" /> widens a foreign key whose linked table has more primary key columns than
/// the key names. Marten declares each DCB tag table's key as <c>(seq_id)</c> to <c>mt_events(seq_id)</c>, and
/// under per-tenant event partitioning the events table's key is <c>(seq_id, tenant_id)</c>, so the key becomes
/// <c>(seq_id, tenant_id)</c>. PostProcess runs on every migration of every database, and a host with several
/// databases migrates them in parallel over the same <see cref="Table" /> instances (Marten's sharded tenancy
/// applies its shard databases with <c>Parallel.ForEachAsync</c>), so the correction has to come out the same
/// however many migrations run it at once.
/// </summary>
public class correcting_a_foreign_key_for_its_linked_primary_key
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public void post_processing_widens_the_key_to_the_linked_primary_key_once()
    {
        var events = buildEvents();
        var tags = new Table(new PostgresqlObjectName("public", "mt_event_tag_probe"));
        var fk = addTagColumnsAndKey(tags, events);

        tags.PostProcess([events, tags]);

        fk.ColumnNames.ShouldBe(["seq_id", "tenant_id"]);
        fk.LinkedNames.ShouldBe(["seq_id", "tenant_id"]);

        // The next migration post-processes the same instance again
        tags.PostProcess([events, tags]);

        fk.ColumnNames.ShouldBe(["seq_id", "tenant_id"]);
        fk.LinkedNames.ShouldBe(["seq_id", "tenant_id"]);
    }

    [Fact]
    public async Task two_migrations_post_processing_the_same_table_at_once_widen_the_key_once()
    {
        var events = buildEvents();
        var tags = new GatedTable(new PostgresqlObjectName("public", "mt_event_tag_probe"));
        var fk = addTagColumnsAndKey(tags, events);

        // Hold the first migration inside the correction until the second one is inside it too, and let the
        // second go on only once the first has written its corrected key. Both have already seen the key one
        // column short of the linked key by then.
        var secondInside = new ManualResetEventSlim();
        var arrivals = 0;
        tags.OnColumnLookup = () =>
        {
            if (Interlocked.Increment(ref arrivals) == 1)
            {
                secondInside.Wait(Timeout);
            }
            else
            {
                secondInside.Set();
                SpinWait.SpinUntil(() => fk.ColumnNames.Length > 1, Timeout);
            }
        };

        var token = TestContext.Current.CancellationToken;
        ISchemaObject[] allObjects = [events, tags];
        var first = Task.Run(() => tags.PostProcess(allObjects), token);
        var second = Task.Run(() => tags.PostProcess(allObjects), token);
        await Task.WhenAll(first, second).WaitAsync(Timeout * 2, token);

        arrivals.ShouldBe(2, "both migrations were inside the correction at once");
        fk.ColumnNames.ShouldBe(["seq_id", "tenant_id"]);
        fk.LinkedNames.ShouldBe(["seq_id", "tenant_id"]);
        fk.ToDDL(tags).ShouldContain("FOREIGN KEY(seq_id, tenant_id)");
    }

    private static Table buildEvents()
    {
        var events = new Table(new PostgresqlObjectName("public", "mt_events"));
        events.AddColumn<long>("seq_id").AsPrimaryKey();
        events.AddColumn<string>("tenant_id").AsPrimaryKey();
        return events;
    }

    private static ForeignKey addTagColumnsAndKey(Table tags, Table events)
    {
        tags.AddColumn<string>("value").AsPrimaryKey();
        tags.AddColumn<string>("tenant_id").AsPrimaryKey();
        tags.AddColumn<long>("seq_id").AsPrimaryKey();

        var fk = new ForeignKey("fkey_mt_event_tag_probe_seq_id")
        {
            LinkedTable = events.Identifier,
            ColumnNames = ["seq_id"],
            LinkedNames = ["seq_id"]
        };
        tags.ForeignKeys.Add(fk);

        return fk;
    }

    /// <summary>
    /// Runs <see cref="OnColumnLookup" /> the first time each thread looks a column up by name, which
    /// <see cref="ForeignKey.TryToCorrectForLink" /> does for every column it adds to the key.
    /// </summary>
    private class GatedTable(PostgresqlObjectName name): Table(name)
    {
        private readonly ThreadLocal<bool> _seen = new();

        public Action? OnColumnLookup { get; set; }

        protected override StringComparison NameComparison
        {
            get
            {
                if (OnColumnLookup != null && !_seen.Value)
                {
                    _seen.Value = true;
                    OnColumnLookup();
                }

                return base.NameComparison;
            }
        }
    }
}
