using System.Data.Common;
using System.Reflection;
using JasperFx.Core.Reflection;
using Shouldly;
using Weasel.Core.Identity;
using Weasel.Core.Sequences;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     Every identity strategy has to be reachable through the non-generic
///     <see cref="IIdentification" /> facade, because a <see cref="Type" />-keyed consumer under
///     Native AOT has no other way in.
/// </summary>
/// <remarks>
///     <para>
///     weasel#689. Marten's <c>ProviderGraph</c> and Polecat's <c>DocumentMapping</c> hold document
///     metadata as <see cref="Type" />, so to <em>use</em> a strategy they closed a generic over
///     <c>(documentType, idType)</c> at runtime. <c>MakeGenericType</c> can close an instantiation
///     whose arguments are all reference types — one canonical body serves them all — but not one
///     with a value-type argument, and an id type is routinely <see cref="Guid" />,
///     <see cref="int" /> or <see cref="long" />. Observed natively published, against SQL Server:
///     </para>
///     <code>
///     System.NotSupportedException: 'Polecat.Internal.IdentityAssigner`2[DeadLetterEvent,System.Guid]'
///     is missing native code or metadata.
///     </code>
///     <para>
///     The obvious workaround — hold the strategy as <see cref="object" /> and invoke through
///     <c>MethodInfo</c> — fails differently, with <c>Sequence contains no matching element</c>,
///     because <c>GetMethods()</c> comes back empty: nothing statically references those members, so
///     the trimmer took the metadata too. You cannot reflect your way out of generics under AOT. The
///     seam has to be an interface something references.
///     </para>
/// </remarks>
public class identification_has_a_non_generic_facade
{
    private readonly ISequenceSource theSequences = new StubSequenceSource();

    /// <summary>
    ///     Every strategy Weasel ships, held the only way an AOT consumer can hold one — as
    ///     <see cref="IIdentification" />, with no type argument named anywhere.
    /// </summary>
    public static TheoryData<string, Strategy> Strategies() => new()
    {
        {
            "SequentialGuidIdentification",
            new Strategy(
                new SequentialGuidIdentification<GuidDoc>(IdMemberOf<GuidDoc>()),
                () => new GuidDoc(),
                doc => ((GuidDoc)doc).Id)
        },
        {
            "GuidIdentification",
            new Strategy(
                new GuidIdentification<GuidDoc>(IdMemberOf<GuidDoc>()),
                () => new GuidDoc(),
                doc => ((GuidDoc)doc).Id)
        },
        {
            "HiloIntIdentification",
            new Strategy(
                new HiloIntIdentification<IntDoc>(IdMemberOf<IntDoc>(), typeof(IntDoc)),
                () => new IntDoc(),
                doc => ((IntDoc)doc).Id)
        },
        {
            "HiloLongIdentification",
            new Strategy(
                new HiloLongIdentification<LongDoc>(IdMemberOf<LongDoc>(), typeof(LongDoc)),
                () => new LongDoc(),
                doc => ((LongDoc)doc).Id)
        },
        {
            "IdentityKeyIdentification",
            new Strategy(
                new IdentityKeyIdentification<StringDoc>(IdMemberOf<StringDoc>(), "stringdoc", typeof(StringDoc)),
                () => new StringDoc(),
                doc => ((StringDoc)doc).Id)
        },
        {
            // Externally assigned, so this one arrives with its id already on it -- AssignIfMissing
            // refuses to invent a string key rather than generating one.
            "StringIdentification",
            new Strategy(
                new StringIdentification<StringDoc>(IdMemberOf<StringDoc>()),
                () => new StringDoc { Id = "assigned-by-the-caller" },
                doc => ((StringDoc)doc).Id)
        },
        {
            "ValueTypeIdentification",
            new Strategy(
                new ValueTypeIdentification<WrappedDoc, FooId, Guid>(
                    IdMemberOf<WrappedDoc>(), ValueTypeInfo.ForType(typeof(FooId)), typeof(WrappedDoc)),
                () => new WrappedDoc(),
                doc => ((WrappedDoc)doc).Id)
        }
    };

    /// <summary>
    ///     The member the issue is actually blocked on: a consumer holding only
    ///     <see cref="IIdentification" /> can assign an id, and the assignment lands on the document
    ///     rather than on a copy.
    /// </summary>
    [Theory]
    [MemberData(nameof(Strategies))]
    public void assigns_an_identity_through_the_facade(string name, Strategy strategy)
    {
        IIdentification facade = strategy.Identification;
        var document = strategy.NewDocument();

        var assigned = facade.AssignIfMissing(document, theSequences);

        assigned.ShouldNotBe(strategy.MissingValue, name);
        strategy.IdOnDocument(document).ShouldBe(assigned, name);
        facade.Identity(document).ShouldBe(assigned, name);
    }

    /// <summary>
    ///     And it stays idempotent through the facade — a second call must not mint a second id.
    /// </summary>
    [Theory]
    [MemberData(nameof(Strategies))]
    public void a_second_assignment_through_the_facade_is_a_no_op(string name, Strategy strategy)
    {
        IIdentification facade = strategy.Identification;
        var document = strategy.NewDocument();

        var first = facade.AssignIfMissing(document, theSequences);
        var second = facade.AssignIfMissing(document, theSequences);

        second.ShouldBe(first, name);
    }

    /// <summary>
    ///     The three members the issue calls useful-by-the-same-argument. They matter most for the
    ///     strong-typed case, where the answer is <em>not</em> the id type — which is exactly what a
    ///     caller holding <see cref="object" /> cannot work out for itself.
    /// </summary>
    [Theory]
    [MemberData(nameof(Strategies))]
    public void reports_the_raw_sql_value_and_type_through_the_facade(string name, Strategy strategy)
    {
        IIdentification facade = strategy.Identification;
        var document = strategy.NewDocument();
        var assigned = facade.AssignIfMissing(document, theSequences);

        facade.ToRawSqlValue(assigned).GetType().ShouldBe(facade.RawSqlType, name);
    }

    [Fact]
    public void a_strong_typed_id_unwraps_to_its_inner_primitive_through_the_facade()
    {
        IIdentification facade = new ValueTypeIdentification<WrappedDoc, FooId, Guid>(
            IdMemberOf<WrappedDoc>(), ValueTypeInfo.ForType(typeof(FooId)), typeof(WrappedDoc));

        var document = new WrappedDoc();
        var assigned = (FooId)facade.AssignIfMissing(document, theSequences);

        facade.RawSqlType.ShouldBe(typeof(Guid));
        facade.ToRawSqlValue(assigned).ShouldBe(assigned.Value);
    }

    [Fact]
    public void reads_an_id_from_a_reader_through_the_facade()
    {
        var id = Guid.CreateVersion7();
        IIdentification facade = new SequentialGuidIdentification<GuidDoc>(IdMemberOf<GuidDoc>());

        facade.ReadIdFromReader(new SingleValueReader(id), 0).ShouldBe(id);
    }

    [Fact]
    public void reads_a_strong_typed_id_from_a_reader_through_the_facade()
    {
        var inner = Guid.CreateVersion7();
        IIdentification facade = new ValueTypeIdentification<WrappedDoc, FooId, Guid>(
            IdMemberOf<WrappedDoc>(), ValueTypeInfo.ForType(typeof(FooId)), typeof(WrappedDoc));

        facade.ReadIdFromReader(new SingleValueReader(inner), 0).ShouldBe(new FooId(inner));
    }

    /// <summary>
    ///     The compatibility half, and the reason every facade member is a default implementation
    ///     rather than an abstract one. <see cref="StrategyFromAnEarlierWeasel" /> implements only the
    ///     members that existed before weasel#689 — which is the shape of every strategy already
    ///     compiled into Marten and Polecat.
    /// </summary>
    /// <remarks>
    ///     weasel#682 is the warning this is written against: adding an abstract member to a public
    ///     interface builds here, passes here, restores and compiles downstream, and then the CLR
    ///     refuses the type the first time a host loads it. A stand-in that leaves every new member to
    ///     its default is the in-tree half of that check — it stops compiling the moment a default
    ///     goes missing.
    /// </remarks>
    [Fact]
    public void a_strategy_compiled_before_the_facade_existed_still_satisfies_it()
    {
        var id = Guid.CreateVersion7();
        var document = new GuidDoc();
        IIdentification facade = new StrategyFromAnEarlierWeasel(id);

        facade.AssignIfMissing(document, theSequences).ShouldBe(id);
        document.Id.ShouldBe(id);
        facade.Identity(document).ShouldBe(id);
        facade.ToRawSqlValue(id).ShouldBe(id);
        facade.RawSqlType.ShouldBe(typeof(Guid));
        facade.ReadIdFromReader(new SingleValueReader(id), 0).ShouldBe(id);
    }

    /// <summary>
    ///     And the same claim read off the metadata, so the failure names the cause. Every facade
    ///     member must resolve to a non-abstract body declared on
    ///     <see cref="IIdentification{TDoc,TId}" />; an abstract one there is a
    ///     <c>TypeLoadException</c> in somebody else's process.
    /// </summary>
    [Fact]
    public void every_facade_member_is_defaulted_on_the_generic_interface()
    {
        var defaulted = typeof(IIdentification<,>)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic |
                        BindingFlags.DeclaredOnly)
            .Where(x => !x.IsAbstract)
            .Select(x => x.Name)
            .ToArray();

        foreach (var member in typeof(IIdentification).GetMethods())
        {
            defaulted.ShouldContain($"{typeof(IIdentification).FullName}.{member.Name}",
                $"IIdentification.{member.Name} needs a default implementation on IIdentification<TDoc, TId>, or every strategy compiled against an earlier Weasel fails to load with TypeLoadException");
        }
    }

    private static MemberInfo IdMemberOf<T>() => typeof(T).GetProperty("Id")!;

    /// <summary>
    ///     One strategy under test, with the two things a generic-free test needs alongside it: how to
    ///     make a fresh document, and how to read the id back off one without naming the id type.
    /// </summary>
    public sealed record Strategy(IIdentification Identification, Func<object> NewDocument,
        Func<object, object> IdOnDocument)
    {
        public object MissingValue => Identification.RawSqlType == typeof(string)
            ? string.Empty
            : Activator.CreateInstance(Identification.RawSqlType)!;
    }

    public class GuidDoc
    {
        public Guid Id { get; set; }
    }

    public class IntDoc
    {
        public int Id { get; set; }
    }

    public class LongDoc
    {
        public long Id { get; set; }
    }

    public class StringDoc
    {
        public string Id { get; set; } = string.Empty;
    }

    public readonly record struct FooId(Guid Value);

    public class WrappedDoc
    {
        public FooId Id { get; set; }
    }

    /// <summary>
    ///     Stands in for a strategy compiled before <see cref="IIdentification" /> existed: it
    ///     implements the two abstract generic members and leaves every facade member to its default.
    /// </summary>
    private sealed class StrategyFromAnEarlierWeasel(Guid id): IIdentification<GuidDoc, Guid>
    {
        public Guid Identity(GuidDoc document) => document.Id;

        public Guid AssignIfMissing(GuidDoc document, ISequenceSource sequences)
        {
            if (document.Id == Guid.Empty) document.Id = id;
            return document.Id;
        }
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

    /// <summary>
    ///     The smallest reader that can answer <c>GetFieldValue&lt;T&gt;(0)</c> — enough to exercise
    ///     <see cref="IIdentification.ReadIdFromReader" /> without a database.
    /// </summary>
    private sealed class SingleValueReader(object value): DbDataReader
    {
        public override T GetFieldValue<T>(int ordinal) => (T)value;
        public override object GetValue(int ordinal) => value;
        public override int FieldCount => 1;
        public override bool HasRows => true;
        public override bool IsClosed => false;
        public override int RecordsAffected => 0;
        public override int Depth => 0;
        public override object this[int ordinal] => value;
        public override object this[string name] => value;
        public override bool Read() => true;
        public override bool NextResult() => false;
        public override bool IsDBNull(int ordinal) => false;
        public override Type GetFieldType(int ordinal) => value.GetType();
        public override string GetName(int ordinal) => "id";
        public override int GetOrdinal(string name) => 0;
        public override string GetDataTypeName(int ordinal) => value.GetType().Name;
        public override IEnumerator<object> GetEnumerator() => throw new NotSupportedException();
        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) =>
            throw new NotSupportedException();

        public override char GetChar(int ordinal) => throw new NotSupportedException();

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) =>
            throw new NotSupportedException();

        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        public override double GetDouble(int ordinal) => throw new NotSupportedException();
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override Guid GetGuid(int ordinal) => (Guid)value;
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
        public override int GetInt32(int ordinal) => (int)value;
        public override long GetInt64(int ordinal) => (long)value;
        public override string GetString(int ordinal) => (string)value;
        public override int GetValues(object[] values) => throw new NotSupportedException();
    }
}
