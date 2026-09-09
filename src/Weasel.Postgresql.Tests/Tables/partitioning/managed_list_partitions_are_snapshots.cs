using System.Collections.Concurrent;
using System.Linq;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;
using Shouldly;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

/// <summary>
/// weasel#583. <see cref="ManagedListPartitions"/> publishes its tenant registry through a
/// <c>ReadOnlyDictionary</c>, which WRAPS the underlying dictionary rather than copying it, and through
/// <see cref="IListPartitionManager.Partitions"/>, which is a lazy iterator over it. Mutating that
/// dictionary in place therefore mutates what every concurrent reader is holding.
///
/// One manager instance is shared across every database in a store, so a `db-apply --parallel N` over more
/// databases than N has a late-starting worker reloading the registry while an earlier worker is still
/// enumerating it. The reported symptom was "Collection was modified; enumeration operation may not execute"
/// out of DatabaseBase.assertValidIdentifiers; the quieter one is a reader seeing an empty or half-loaded
/// partition set and generating DDL, or a partition delta, against it — with nothing raised at all.
/// </summary>
[Collection("managed_lists")]
public class managed_list_partitions_are_snapshots: IntegrationContext
{
    private const int TenantCount = 400;

    public managed_list_partitions_are_snapshots() : base("managed_lists")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    [Fact]
    public async Task readers_never_see_the_registry_being_reloaded()
    {
        var database = new ManagedListDatabase();
        var values = Enumerable.Range(0, TenantCount)
            .ToDictionary(i => "tenant" + i, i => "tenant" + i);

        await database.Partitions.ResetValues(database, values, CancellationToken.None);

        var manager = (IListPartitionManager)database.Partitions;

        var failures = new ConcurrentQueue<Exception>();
        var tornReads = 0;

        // The writer is the real reload path — ForceReload + InitializeAsync is exactly what each database's
        // schema initializer runs, and what the late worker in the report was running.
        var writer = Task.Run(async () =>
        {
            for (var i = 0; i < 25; i++)
            {
                database.Partitions.ForceReload();
                await database.Partitions.InitializeAsync(database, CancellationToken.None);
            }
        });

        var readers = Enumerable.Range(0, 3).Select(_ => Task.Run(() =>
        {
            while (!writer.IsCompleted)
            {
                try
                {
                    // The DDL and delta read side: ListPartitioning.Partitions() calls straight through here.
                    if (manager.Partitions().Count() != TenantCount)
                    {
                        Interlocked.Increment(ref tornReads);
                    }

                    // ...and the published map, which callers enumerate directly.
                    var published = database.Partitions.Partitions;
                    var counted = 0;
                    foreach (var _ in published)
                    {
                        counted++;
                    }

                    if (counted != TenantCount)
                    {
                        Interlocked.Increment(ref tornReads);
                    }
                }
                catch (Exception e)
                {
                    failures.Enqueue(e);
                }
            }
        })).ToArray();

        await writer;
        await Task.WhenAll(readers);

        if (failures.TryDequeue(out var failure))
        {
            throw new Exception(
                "A reader threw while the partition registry was being reloaded underneath it", failure);
        }

        tornReads.ShouldBe(0, "A reader observed an incomplete partition registry");
    }
}
