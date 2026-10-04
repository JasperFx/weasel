using System.Reflection;
using System.Runtime.CompilerServices;
using JasperFx.Core.Reflection;
using Shouldly;
using Weasel.Core.Identity;
using Weasel.Core.Sequences;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     The strong-typed id strategy has to be buildable from runtime <see cref="Type" />s, and the
///     two implementations that back <see cref="Identifications.ForValueType" /> have to answer
///     identically.
/// </summary>
/// <remarks>
///     <para>
///     weasel#690, the construction-side companion to weasel#689. A consumer builds the strategy by
///     closing Weasel's generic over its runtime types, and
///     <c>ValueTypeIdentification&lt;TDoc, TWrapper, TInner&gt;</c> is the one strategy whose type
///     arguments are not all reference types — <c>TInner</c> is the wrapped primitive and
///     <c>TWrapper</c> is usually a <c>readonly record struct</c>. Reproduced on macOS arm64, ILC,
///     net9.0, which is what separates this from a prediction:
///     </para>
///     <code>
///     IsDynamicCodeSupported = False
///     SequentialGuidIdentification&lt;Doc&gt;: constructed OK
///     ValueTypeIdentification&lt;Doc,FooId,Guid&gt;: NotSupportedException:
///       'Weasel.Core.Identity.ValueTypeIdentification`3[Doc,FooId,System.Guid]'
///       is missing native code or metadata.
///     </code>
///     <para>
///     So there are now two descriptions of the same behaviour — the FEC-compiled generic one and
///     <see cref="ReflectedValueTypeIdentification" /> — and the cost of that is exactly the risk the
///     issue named: they can drift. Most of what follows runs both against the same assertions for
///     that reason, rather than testing the new one on its own.
///     </para>
/// </remarks>
public class value_type_identification_needs_no_closed_generic
{
    private readonly ISequenceSource theSequences = new StubSequenceSource();

    /// <summary>
    ///     The two implementations, named the way the failure message should read.
    /// </summary>
    public static TheoryData<string> Implementations() => new() { "generic", "reflected" };

    [Theory]
    [MemberData(nameof(Implementations))]
    public void generates_a_guid_id_and_writes_it_onto_the_document(string implementation)
    {
        var strategy = Build<GuidWrapped>(implementation);
        var document = new GuidWrapped();

        var assigned = (GuidId)strategy.AssignIfMissing(document, theSequences);

        assigned.Value.ShouldNotBe(Guid.Empty);
        document.Id.ShouldBe(assigned);
        strategy.Identity(document).ShouldBe(assigned);
    }

    [Theory]
    [MemberData(nameof(Implementations))]
    public void leaves_an_id_that_is_already_there(string implementation)
    {
        var existing = new GuidId(Guid.CreateVersion7());
        var strategy = Build<GuidWrapped>(implementation);
        var document = new GuidWrapped { Id = existing };

        strategy.AssignIfMissing(document, theSequences).ShouldBe(existing);
        document.Id.ShouldBe(existing);
    }

    [Theory]
    [MemberData(nameof(Implementations))]
    public void draws_an_int_id_from_the_hilo_sequence(string implementation)
    {
        var strategy = Build<IntWrapped>(implementation);

        ((IntId)strategy.AssignIfMissing(new IntWrapped(), theSequences)).Value.ShouldBe(1);
        ((IntId)strategy.AssignIfMissing(new IntWrapped(), theSequences)).Value.ShouldBe(2);
    }

    [Theory]
    [MemberData(nameof(Implementations))]
    public void draws_a_long_id_from_the_hilo_sequence(string implementation)
    {
        var strategy = Build<LongWrapped>(implementation);

        ((LongId)strategy.AssignIfMissing(new LongWrapped(), theSequences)).Value.ShouldBe(1L);
    }

    /// <summary>
    ///     A wrapped string id is externally assigned, exactly like the unwrapped one — the strategy
    ///     refuses to invent a key rather than generating something.
    /// </summary>
    [Theory]
    [MemberData(nameof(Implementations))]
    public void refuses_to_generate_a_wrapped_string_id(string implementation)
    {
        var strategy = Build<StringWrapped>(implementation);

        Should.Throw<InvalidOperationException>(
            () => strategy.AssignIfMissing(new StringWrapped(), theSequences));
    }

    [Theory]
    [MemberData(nameof(Implementations))]
    public void reads_back_a_wrapped_string_id_that_was_assigned(string implementation)
    {
        var strategy = Build<StringWrapped>(implementation);
        var document = new StringWrapped { Id = new StringId("assigned-by-the-caller") };

        strategy.AssignIfMissing(document, theSequences).ShouldBe(new StringId("assigned-by-the-caller"));
    }

    /// <summary>
    ///     The case the generic strategy needed a separate expression tree for: a
    ///     <c>Nullable&lt;wrapper&gt;</c> property on a freshly constructed document, where reading
    ///     the id at all is what breaks.
    /// </summary>
    [Theory]
    [MemberData(nameof(Implementations))]
    public void assigns_through_a_nullable_wrapper_property(string implementation)
    {
        var strategy = Build<NullableGuidWrapped>(implementation);
        var document = new NullableGuidWrapped();

        var assigned = (GuidId)strategy.AssignIfMissing(document, theSequences);

        assigned.Value.ShouldNotBe(Guid.Empty);
        document.Id.ShouldBe(assigned);
    }

    [Theory]
    [MemberData(nameof(Implementations))]
    public void keeps_an_id_that_is_already_on_a_nullable_wrapper_property(string implementation)
    {
        var existing = new GuidId(Guid.CreateVersion7());
        var strategy = Build<NullableGuidWrapped>(implementation);
        var document = new NullableGuidWrapped { Id = existing };

        strategy.AssignIfMissing(document, theSequences).ShouldBe(existing);
    }

    [Theory]
    [MemberData(nameof(Implementations))]
    public void reports_the_inner_primitive_as_the_raw_sql_type(string implementation)
    {
        Build<GuidWrapped>(implementation).RawSqlType.ShouldBe(typeof(Guid));
        Build<IntWrapped>(implementation).RawSqlType.ShouldBe(typeof(int));
        Build<LongWrapped>(implementation).RawSqlType.ShouldBe(typeof(long));
        Build<StringWrapped>(implementation).RawSqlType.ShouldBe(typeof(string));
    }

    [Theory]
    [MemberData(nameof(Implementations))]
    public void unwraps_to_the_inner_primitive_for_binding(string implementation)
    {
        var id = new GuidId(Guid.CreateVersion7());

        Build<GuidWrapped>(implementation).ToRawSqlValue(id).ShouldBe(id.Value);
    }

    [Theory]
    [MemberData(nameof(Implementations))]
    public void wraps_the_inner_primitive_coming_back_off_a_reader(string implementation)
    {
        var inner = Guid.CreateVersion7();

        Build<GuidWrapped>(implementation)
            .ReadIdFromReader(new SingleValueReader(inner), 0)
            .ShouldBe(new GuidId(inner));
    }

    /// <summary>
    ///     Reading the id of a never-assigned <c>Nullable&lt;wrapper&gt;</c> property is the one place
    ///     the two implementations reach the same outcome by different routes — the generic one's
    ///     compiled <c>.Value</c> access throws, and the reflected one has a null to report. They have
    ///     to throw the same kind of thing.
    /// </summary>
    [Theory]
    [MemberData(nameof(Implementations))]
    public void reading_an_unassigned_nullable_id_throws_either_way(string implementation)
    {
        var strategy = Build<NullableGuidWrapped>(implementation);

        Should.Throw<InvalidOperationException>(() => strategy.Identity(new NullableGuidWrapped()));
    }

    /// <summary>
    ///     And the factory's own choice. On a host with dynamic code — every test run, and every
    ///     Marten or Polecat process that is not natively published — the fast path has to be the one
    ///     taken, or this change would have made every strong-typed id slower to buy an AOT fix.
    /// </summary>
    [Fact]
    public void keeps_the_compiled_strategy_where_dynamic_code_is_available()
    {
        RuntimeFeature.IsDynamicCodeSupported.ShouldBeTrue("this test is meaningless on a natively published host");

        ForValueType<GuidWrapped>()
            .ShouldBeOfType<ValueTypeIdentification<GuidWrapped, GuidId, Guid>>();
    }

    /// <summary>
    ///     The reflected implementation closes nothing — which is the entire claim, and is readable
    ///     straight off the type.
    /// </summary>
    [Fact]
    public void the_reflected_strategy_is_not_a_generic_type()
    {
        typeof(ReflectedValueTypeIdentification).IsGenericType.ShouldBeFalse();
        typeof(IIdentification).IsAssignableFrom(typeof(ReflectedValueTypeIdentification)).ShouldBeTrue();
    }

    /// <summary>
    ///     The rest of the factory. These six close a generic over the document type alone, which is
    ///     a reference type and so has native code under AOT — they are here because the point of
    ///     weasel#690 is that <em>no</em> consumer closes one of these generics itself, not only the
    ///     one that breaks.
    /// </summary>
    [Fact]
    public void builds_every_other_strategy_from_runtime_types()
    {
        Identifications.ForSequentialGuid(typeof(GuidDoc), IdMemberOf<GuidDoc>())
            .ShouldBeOfType<SequentialGuidIdentification<GuidDoc>>();

        Identifications.ForRandomGuid(typeof(GuidDoc), IdMemberOf<GuidDoc>())
            .ShouldBeOfType<GuidIdentification<GuidDoc>>();

        Identifications.ForHiloInt(typeof(IntDoc), IdMemberOf<IntDoc>(), typeof(IntDoc))
            .ShouldBeOfType<HiloIntIdentification<IntDoc>>();

        Identifications.ForHiloLong(typeof(LongDoc), IdMemberOf<LongDoc>(), typeof(LongDoc))
            .ShouldBeOfType<HiloLongIdentification<LongDoc>>();

        Identifications.ForIdentityKey(typeof(StringDoc), IdMemberOf<StringDoc>(), "stringdoc", typeof(StringDoc))
            .ShouldBeOfType<IdentityKeyIdentification<StringDoc>>();

        Identifications.ForExternallyAssignedString(typeof(StringDoc), IdMemberOf<StringDoc>())
            .ShouldBeOfType<StringIdentification<StringDoc>>();
    }

    [Fact]
    public void a_strategy_built_from_runtime_types_still_assigns()
    {
        var document = new GuidDoc();

        var assigned = Identifications
            .ForSequentialGuid(typeof(GuidDoc), IdMemberOf<GuidDoc>())
            .AssignIfMissing(document, theSequences);

        assigned.ShouldBe(document.Id);
        document.Id.ShouldNotBe(Guid.Empty);
    }

    private IIdentification Build<TDoc>(string implementation) => implementation == "generic"
        ? Generic<TDoc>()
        : new ReflectedValueTypeIdentification(typeof(TDoc), IdMemberOf<TDoc>(), ValueTypeOf<TDoc>(), typeof(TDoc));

    private static IIdentification ForValueType<TDoc>()
        => Identifications.ForValueType(typeof(TDoc), IdMemberOf<TDoc>(), ValueTypeOf<TDoc>(), typeof(TDoc));

    /// <summary>
    ///     Closes the generic the way a consumer used to — which is the call that has no native code
    ///     under AOT, and is why the reflected implementation exists.
    /// </summary>
    private static IIdentification Generic<TDoc>()
    {
        var valueType = ValueTypeOf<TDoc>();
        return (IIdentification)Activator.CreateInstance(
            typeof(ValueTypeIdentification<,,>).MakeGenericType(typeof(TDoc), valueType.OuterType,
                valueType.SimpleType),
            IdMemberOf<TDoc>(), valueType, typeof(TDoc))!;
    }

    private static ValueTypeInfo ValueTypeOf<TDoc>()
    {
        var declared = ((PropertyInfo)IdMemberOf<TDoc>()).PropertyType;
        return ValueTypeInfo.ForType(Nullable.GetUnderlyingType(declared) ?? declared);
    }

    private static MemberInfo IdMemberOf<T>() => typeof(T).GetProperty("Id")!;

    public readonly record struct GuidId(Guid Value);

    public readonly record struct IntId(int Value);

    public readonly record struct LongId(long Value);

    public readonly record struct StringId(string Value);

    public class GuidWrapped
    {
        public GuidId Id { get; set; }
    }

    public class NullableGuidWrapped
    {
        public GuidId? Id { get; set; }
    }

    public class IntWrapped
    {
        public IntId Id { get; set; }
    }

    public class LongWrapped
    {
        public LongId Id { get; set; }
    }

    public class StringWrapped
    {
        public StringId Id { get; set; }
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
