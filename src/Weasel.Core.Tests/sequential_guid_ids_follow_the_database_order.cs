using System.Data.SqlTypes;
using System.Reflection;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Shouldly;
using Weasel.Core.Identity;
using Weasel.Core.Sequences;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     Guid ids are made by an <see cref="IGuidGenerator" />. The default is
///     <see cref="SequentialGuid.PostgresOrder" />, and a store that writes to SQL Server passes
///     <see cref="SequentialGuid.SqlServerOrder" />, so each new id sorts after the last one in the
///     order that database compares them.
/// </summary>
public class sequential_guid_ids_follow_the_database_order
{
    private const int HowMany = 1000;

    private readonly ISequenceSource theSequences = new StubSequenceSource();

    public static TheoryData<string> Implementations() => new() { "generic", "reflected" };

    [Fact]
    public void sequential_guid_ids_increase_in_postgresql_order_by_default()
    {
        var strategy = new SequentialGuidIdentification<GuidDoc>(IdMemberOf<GuidDoc>());

        strategy.Generator.ShouldBeSameAs(SequentialGuid.PostgresOrder);
        AssignMany(strategy).ShouldBeIncreasingIn(PostgresOrder);
    }

    [Fact]
    public void sequential_guid_ids_increase_in_sql_server_order_when_asked()
    {
        var strategy = new SequentialGuidIdentification<GuidDoc>(IdMemberOf<GuidDoc>(), SequentialGuid.SqlServerOrder);

        strategy.Generator.ShouldBeSameAs(SequentialGuid.SqlServerOrder);
        AssignMany(strategy).ShouldBeIncreasingIn(SqlServerOrder);
    }

    [Fact]
    public async Task sequential_guid_ids_from_many_threads_are_unique()
    {
        var strategy = new SequentialGuidIdentification<GuidDoc>(IdMemberOf<GuidDoc>(), SequentialGuid.SqlServerOrder);
        const int threads = 8;
        var perThread = new List<Guid>[threads];

        using var start = new Barrier(threads);
        await Task.WhenAll(Enumerable.Range(0, threads).Select(t => Task.Factory.StartNew(() =>
        {
            start.SignalAndWait();
            perThread[t] = AssignMany(strategy);
        }, TaskCreationOptions.LongRunning)));

        var all = perThread.SelectMany(x => x).ToList();
        all.Distinct().Count().ShouldBe(threads * HowMany);

        // Each thread still sees its own ids increase
        foreach (var ids in perThread) ids.ShouldBeIncreasingIn(SqlServerOrder);
    }

    /// <summary>
    ///     Rows written by Weasel 10.0 alphas before this change carry <c>Guid.CreateVersion7()</c> ids.
    ///     New ids sort after them in PostgreSQL order, so an existing table keeps appending at the
    ///     right edge of its primary key index.
    /// </summary>
    [Fact]
    public void new_ids_sort_after_version_7_ids_in_postgresql_order()
    {
        var older = Guid.CreateVersion7();
        var newer = new SequentialGuidIdentification<GuidDoc>(IdMemberOf<GuidDoc>())
            .AssignIfMissing(new GuidDoc(), theSequences);

        newer.CompareTo(older).ShouldBeGreaterThan(0);
    }

    [Fact]
    public void sequential_guid_ids_come_from_the_generator_passed_in()
    {
        var generator = new CountingGenerator();
        var strategy = new SequentialGuidIdentification<GuidDoc>(IdMemberOf<GuidDoc>(), generator);

        var document = new GuidDoc();
        strategy.AssignIfMissing(document, theSequences).ShouldBe(CountingGenerator.IdFor(1));
        document.Id.ShouldBe(CountingGenerator.IdFor(1));

        // An id that is already there is left alone and draws nothing
        strategy.AssignIfMissing(document, theSequences).ShouldBe(CountingGenerator.IdFor(1));
        generator.Count.ShouldBe(1);
    }

    [Fact]
    public void sequential_guid_built_from_runtime_types_uses_postgresql_order_by_default()
    {
        var strategy = Identifications.ForSequentialGuid(typeof(GuidDoc), IdMemberOf<GuidDoc>());

        strategy.ShouldBeOfType<SequentialGuidIdentification<GuidDoc>>()
            .Generator.ShouldBeSameAs(SequentialGuid.PostgresOrder);
        AssignMany(strategy).ShouldBeIncreasingIn(PostgresOrder);
    }

    [Fact]
    public void sequential_guid_built_from_runtime_types_takes_a_generator()
    {
        var strategy = Identifications.ForSequentialGuid(typeof(GuidDoc), IdMemberOf<GuidDoc>(),
            SequentialGuid.SqlServerOrder);

        strategy.ShouldBeOfType<SequentialGuidIdentification<GuidDoc>>()
            .Generator.ShouldBeSameAs(SequentialGuid.SqlServerOrder);
        AssignMany(strategy).ShouldBeIncreasingIn(SqlServerOrder);
    }

    [Theory]
    [MemberData(nameof(Implementations))]
    public void strong_typed_guid_ids_increase_in_postgresql_order_by_default(string implementation)
    {
        // The constructors without a generator, which existing callers use
        IIdentification strategy = implementation == "generic"
            ? new ValueTypeIdentification<GuidWrapped, GuidId, Guid>(IdMemberOf<GuidWrapped>(),
                ValueTypeOf<GuidWrapped>(), typeof(GuidWrapped))
            : new ReflectedValueTypeIdentification(typeof(GuidWrapped), IdMemberOf<GuidWrapped>(),
                ValueTypeOf<GuidWrapped>(), typeof(GuidWrapped));

        AssignManyWrapped(strategy).ShouldBeIncreasingIn(PostgresOrder);
    }

    [Theory]
    [MemberData(nameof(Implementations))]
    public void strong_typed_guid_ids_increase_in_sql_server_order_when_asked(string implementation)
    {
        var strategy = BuildWrapped(implementation, SequentialGuid.SqlServerOrder);

        AssignManyWrapped(strategy).ShouldBeIncreasingIn(SqlServerOrder);
    }

    [Theory]
    [MemberData(nameof(Implementations))]
    public void strong_typed_guid_ids_come_from_the_generator_passed_in(string implementation)
    {
        var strategy = BuildWrapped(implementation, new CountingGenerator());

        ((GuidId)strategy.AssignIfMissing(new GuidWrapped(), theSequences)).Value.ShouldBe(CountingGenerator.IdFor(1));
        ((GuidId)strategy.AssignIfMissing(new GuidWrapped(), theSequences)).Value.ShouldBe(CountingGenerator.IdFor(2));
    }

    [Fact]
    public void strong_typed_ids_built_from_runtime_types_take_a_generator()
    {
        var strategy = Identifications.ForValueType(typeof(GuidWrapped), IdMemberOf<GuidWrapped>(),
            ValueTypeOf<GuidWrapped>(), typeof(GuidWrapped), SequentialGuid.SqlServerOrder);

        strategy.ShouldBeOfType<ValueTypeIdentification<GuidWrapped, GuidId, Guid>>();
        AssignManyWrapped(strategy).ShouldBeIncreasingIn(SqlServerOrder);
    }

    [Fact]
    public void the_generator_does_not_change_how_int_ids_are_drawn()
    {
        var strategy = Identifications.ForValueType(typeof(IntWrapped), IdMemberOf<IntWrapped>(),
            ValueTypeOf<IntWrapped>(), typeof(IntWrapped), new CountingGenerator());

        ((IntId)strategy.AssignIfMissing(new IntWrapped(), theSequences)).Value.ShouldBe(1);
        ((IntId)strategy.AssignIfMissing(new IntWrapped(), theSequences)).Value.ShouldBe(2);
    }

    private static int PostgresOrder(Guid x, Guid y) => x.CompareTo(y);

    private static int SqlServerOrder(Guid x, Guid y) => new SqlGuid(x).CompareTo(new SqlGuid(y));

    private IIdentification BuildWrapped(string implementation, IGuidGenerator generator) =>
        implementation == "generic"
            ? new ValueTypeIdentification<GuidWrapped, GuidId, Guid>(IdMemberOf<GuidWrapped>(),
                ValueTypeOf<GuidWrapped>(), typeof(GuidWrapped), generator)
            : new ReflectedValueTypeIdentification(typeof(GuidWrapped), IdMemberOf<GuidWrapped>(),
                ValueTypeOf<GuidWrapped>(), typeof(GuidWrapped), generator);

    private List<Guid> AssignMany(IIdentification strategy)
    {
        var ids = new List<Guid>(HowMany);
        for (var i = 0; i < HowMany; i++)
        {
            var document = new GuidDoc();
            ids.Add((Guid)strategy.AssignIfMissing(document, theSequences));
            document.Id.ShouldBe(ids[^1]);
        }

        return ids;
    }

    private List<Guid> AssignManyWrapped(IIdentification strategy)
    {
        var ids = new List<Guid>(HowMany);
        for (var i = 0; i < HowMany; i++)
        {
            ids.Add(((GuidId)strategy.AssignIfMissing(new GuidWrapped(), theSequences)).Value);
        }

        return ids;
    }

    private static ValueTypeInfo ValueTypeOf<TDoc>()
        => ValueTypeInfo.ForType(((PropertyInfo)IdMemberOf<TDoc>()).PropertyType);

    private static MemberInfo IdMemberOf<T>() => typeof(T).GetProperty("Id")!;

    /// <summary>
    ///     Hands out 1, 2, 3 … as Guids, so a test can tell its ids came from here.
    /// </summary>
    private class CountingGenerator: IGuidGenerator
    {
        public int Count { get; private set; }

        public Guid NewGuid() => IdFor(++Count);

        public static Guid IdFor(int count) => new(count, 0, 0, new byte[8]);
    }

    public readonly record struct GuidId(Guid Value);

    public readonly record struct IntId(int Value);

    public class GuidDoc
    {
        public Guid Id { get; set; }
    }

    public class GuidWrapped
    {
        public GuidId Id { get; set; }
    }

    public class IntWrapped
    {
        public IntId Id { get; set; }
    }

    private sealed class StubSequenceSource: ISequenceSource
    {
        private readonly StubSequence _sequence = new();
        public ISequence SequenceFor(Type documentType) => _sequence;
    }

    private sealed class StubSequence: ISequence
    {
        private long _current;
        public int MaxLo => 1;
        public int NextInt() => (int)NextLong();
        public long NextLong() => Interlocked.Increment(ref _current);
        public Task SetFloor(long floor) => Task.CompletedTask;
    }
}

internal static class GuidOrderAssertions
{
    public static void ShouldBeIncreasingIn(this IReadOnlyList<Guid> ids, Func<Guid, Guid, int> compare)
    {
        for (var i = 1; i < ids.Count; i++)
        {
            if (compare(ids[i - 1], ids[i]) >= 0)
            {
                throw new ShouldAssertException(
                    $"Id {i} ({ids[i]}) does not sort after id {i - 1} ({ids[i - 1]})");
            }
        }
    }
}
