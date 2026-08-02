using System.Data;
using NpgsqlTypes;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <remarks>
/// Exercises its own registry instances rather than the static NpgsqlTypeMapper.Mappings.
/// Registering tens of thousands of mappings into the global one would linger for the rest of
/// the run, and PostgresqlProvider.GetTypeMapping scans it linearly, so every other test that
/// resolves an unmapped type would slow to a crawl. weasel#406.
/// </remarks>
public class NpgsqlTypeMappingRegistryTests
{
    private static NpgsqlTypeMappingRegistry EmptyRegistry()
    {
        return new NpgsqlTypeMappingRegistry(new Dictionary<NpgsqlDbType, NpgsqlTypeMapping>());
    }

    private static NpgsqlTypeMapping AnyMapping()
    {
        return new NpgsqlTypeMapping(NpgsqlDbType.Varchar, DbType.String, "varchar", typeof(string));
    }

    [Fact]
    public void concurrent_registrations_are_not_lost()
    {
        // The regression: backed by a JasperFx Cache, the indexer setter was a non-atomic
        // read-modify-write over an ImHashMap, and this landed 7,660 of 40,000 -- an 81% loss,
        // silently. weasel#406.
        const int threads = 8;
        const int perThread = 2000;
        var registry = EmptyRegistry();

        Parallel.For(0, threads, t =>
        {
            for (var i = 0; i < perThread; i++)
            {
                registry[(NpgsqlDbType)(t * perThread + i)] = AnyMapping();
            }
        });

        registry.Count.ShouldBe(threads * perThread);
    }

    [Fact]
    public void enumerating_while_registrations_land_neither_throws_nor_tears()
    {
        const int seeded = 500;
        const int added = 5000;
        const int passes = 50;
        var patience = TimeSpan.FromSeconds(30);

        var registry = EmptyRegistry();
        for (var i = 0; i < seeded; i++)
        {
            registry[(NpgsqlDbType)i] = AnyMapping();
        }

        using var writingHasStarted = new ManualResetEventSlim(false);
        using var readerIsFinished = new ManualResetEventSlim(false);

        // The writer holds the concurrent window open for the reader instead of racing it to the
        // finish: it registers the new keys, then keeps re-registering them -- which mutates the
        // map without moving the final count -- until the reader has taken its passes. The reader
        // in turn does not start until the first registration has landed, so every pass below is
        // taken against a registry actively being written to.
        //
        // The earlier `while (!writer.IsCompleted)` shape had no such handshake: when the pool
        // scheduled the writer promptly it finished all 5,000 registrations before the first
        // IsCompleted check, the loop body never ran, and the test asserted nothing at all --
        // green here, and intermittently red on the trailing `passes > 0` in CI.
        var writer = Task.Run(() =>
        {
            for (var i = 0; i < added; i++)
            {
                registry[(NpgsqlDbType)(100_000 + i)] = AnyMapping();
                writingHasStarted.Set();
            }

            while (!readerIsFinished.IsSet)
            {
                for (var i = 0; i < added && !readerIsFinished.IsSet; i++)
                {
                    registry[(NpgsqlDbType)(100_000 + i)] = AnyMapping();
                }
            }
        });

        try
        {
            writingHasStarted.Wait(patience).ShouldBeTrue();

            for (var pass = 0; pass < passes; pass++)
            {
                // Each read must see a coherent snapshot: never fewer than what was there before
                // the writer started, and never a null hole.
                var seen = registry.ToList();
                seen.Count.ShouldBeGreaterThanOrEqualTo(seeded);
                seen.ShouldAllBe(mapping => mapping != null);
            }
        }
        finally
        {
            // In a finally so a failed assertion above releases the writer rather than hanging
            // the run in its churn loop.
            readerIsFinished.Set();
        }

        writer.GetAwaiter().GetResult();
        registry.Count.ShouldBe(seeded + added);
    }

    [Fact]
    public void seeded_mappings_are_readable_by_key_and_by_enumeration()
    {
        var mapping = AnyMapping();
        var registry = new NpgsqlTypeMappingRegistry(new Dictionary<NpgsqlDbType, NpgsqlTypeMapping>
        {
            { NpgsqlDbType.Varchar, mapping }
        });

        registry[NpgsqlDbType.Varchar].ShouldBeSameAs(mapping);
        registry.TryGetValue(NpgsqlDbType.Varchar, out var found).ShouldBeTrue();
        found.ShouldBeSameAs(mapping);
        registry.ShouldContain(mapping);

        registry.TryGetValue(NpgsqlDbType.Bigint, out _).ShouldBeFalse();
    }

    [Fact]
    public void registering_over_an_existing_key_replaces_it()
    {
        var registry = EmptyRegistry();
        var first = AnyMapping();
        var second = AnyMapping();

        registry[NpgsqlDbType.Varchar] = first;
        registry[NpgsqlDbType.Varchar] = second;

        registry[NpgsqlDbType.Varchar].ShouldBeSameAs(second);
        registry.Count.ShouldBe(1);
    }
}
