using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.CompilerServices;
using JasperFx.Core.Reflection;
using Weasel.Core.Sequences;

namespace Weasel.Core.Identity;

/// <summary>
///     Builds identity strategies from runtime <see cref="Type" />s, so a consumer whose document
///     metadata is <see cref="Type" />-based never closes one of Weasel's generics itself.
/// </summary>
/// <remarks>
///     <para>
///     weasel#690, the construction-side half of weasel#689's use-side facade. Marten and Polecat
///     both reached for <c>MakeGenericType</c> here, which closes an instantiation whose arguments
///     are all reference types — they share one canonical body — and fails when any argument is a
///     value type. Three of Weasel's strategies take only the document type, so they are fine;
///     <see cref="ValueTypeIdentification{TDoc,TWrapper,TInner}" /> takes the wrapper and its inner
///     primitive as well, and a strong-typed id is therefore the shape that breaks. Reproduced on
///     macOS arm64, ILC, net9.0:
///     </para>
///     <code>
///     IsDynamicCodeSupported = False
///     SequentialGuidIdentification&lt;Doc&gt;: constructed OK
///     ValueTypeIdentification&lt;Doc,FooId,Guid&gt;: NotSupportedException:
///       'Weasel.Core.Identity.ValueTypeIdentification`3[Doc,FooId,System.Guid]'
///       is missing native code or metadata.
///     </code>
///     <para>
///     <strong>Which</strong> strategy fits an id is deliberately still the caller's decision rather
///     than something inferred from the id type here. Marten and Polecat do not agree on it — the
///     same <see cref="Guid" /> id is a sequential GUID in one configuration and a caller-assigned
///     one in another, and an <see cref="int" /> may be Hi-Lo or externally assigned — so a mapping
///     from id type to strategy in Weasel would be a guess wearing a library's authority. What
///     belongs here, and is what the consumer could not do for itself, is the construction.
///     </para>
/// </remarks>
public static class Identifications
{
    /// <summary>
    ///     <see cref="SequentialGuidIdentification{TDoc}" /> — <see cref="Guid" /> ids generated as
    ///     time-ordered UUIDv7, no database round-trip.
    /// </summary>
    [RequiresUnreferencedCode("Builds FEC-compiled accessor delegates over the id member via LambdaBuilder.")]
    [RequiresDynamicCode("Closes SequentialGuidIdentification<TDoc> over the document type.")]
    public static IIdentification ForSequentialGuid(Type documentType, MemberInfo idMember)
        => Close(typeof(SequentialGuidIdentification<>), documentType, idMember);

    /// <summary>
    ///     <see cref="GuidIdentification{TDoc}" /> — <see cref="Guid" /> ids from
    ///     <see cref="Guid.NewGuid" />, random rather than time-ordered.
    /// </summary>
    [RequiresUnreferencedCode("Builds FEC-compiled accessor delegates over the id member via LambdaBuilder.")]
    [RequiresDynamicCode("Closes GuidIdentification<TDoc> over the document type.")]
    public static IIdentification ForRandomGuid(Type documentType, MemberInfo idMember)
        => Close(typeof(GuidIdentification<>), documentType, idMember);

    /// <summary>
    ///     <see cref="HiloIntIdentification{TDoc}" /> — <see cref="int" /> ids from the Hi-Lo sequence
    ///     keyed by <paramref name="sequenceKey" />.
    /// </summary>
    [RequiresUnreferencedCode("Builds FEC-compiled accessor delegates over the id member via LambdaBuilder.")]
    [RequiresDynamicCode("Closes HiloIntIdentification<TDoc> over the document type.")]
    public static IIdentification ForHiloInt(Type documentType, MemberInfo idMember, Type sequenceKey)
        => Close(typeof(HiloIntIdentification<>), documentType, idMember, sequenceKey);

    /// <summary>
    ///     <see cref="HiloLongIdentification{TDoc}" /> — <see cref="long" /> ids from the Hi-Lo
    ///     sequence keyed by <paramref name="sequenceKey" />.
    /// </summary>
    [RequiresUnreferencedCode("Builds FEC-compiled accessor delegates over the id member via LambdaBuilder.")]
    [RequiresDynamicCode("Closes HiloLongIdentification<TDoc> over the document type.")]
    public static IIdentification ForHiloLong(Type documentType, MemberInfo idMember, Type sequenceKey)
        => Close(typeof(HiloLongIdentification<>), documentType, idMember, sequenceKey);

    /// <summary>
    ///     <see cref="IdentityKeyIdentification{TDoc}" /> — string ids of the form
    ///     <c>"{mappingAlias}/{nextLong}"</c>.
    /// </summary>
    [RequiresUnreferencedCode("Builds FEC-compiled accessor delegates over the id member via LambdaBuilder.")]
    [RequiresDynamicCode("Closes IdentityKeyIdentification<TDoc> over the document type.")]
    public static IIdentification ForIdentityKey(Type documentType, MemberInfo idMember, string mappingAlias,
        Type sequenceKey)
        => Close(typeof(IdentityKeyIdentification<>), documentType, idMember, mappingAlias, sequenceKey);

    /// <summary>
    ///     <see cref="StringIdentification{TDoc}" /> — externally assigned string keys, which the
    ///     strategy reads back and refuses to generate.
    /// </summary>
    [RequiresUnreferencedCode("Builds an FEC-compiled accessor delegate over the id member via LambdaBuilder.")]
    [RequiresDynamicCode("Closes StringIdentification<TDoc> over the document type.")]
    public static IIdentification ForExternallyAssignedString(Type documentType, MemberInfo idMember)
        => Close(typeof(StringIdentification<>), documentType, idMember);

    /// <summary>
    ///     The strong-typed id strategy — Vogen / StronglyTypedId wrappers and F# single-case
    ///     discriminated unions.
    /// </summary>
    /// <remarks>
    ///     This is the one that has to choose. Where dynamic code is available it closes
    ///     <see cref="ValueTypeIdentification{TDoc,TWrapper,TInner}" /> exactly as a caller used to, so
    ///     nothing gets slower: the hot path stays an FEC-compiled delegate. Under Native AOT, where
    ///     that instantiation is missing native code because <c>TInner</c> and usually
    ///     <c>TWrapper</c> are value types, it hands back
    ///     <see cref="ReflectedValueTypeIdentification" /> instead, which closes nothing and compiles
    ///     nothing.
    ///     <para>
    ///     The branch is on <see cref="RuntimeFeature.IsDynamicCodeSupported" /> rather than on a
    ///     caught exception on purpose. It is a published feature switch that ILC substitutes to
    ///     <c>false</c> and then trims the dead branch, so the AOT build neither warns about the
    ///     <c>MakeGenericType</c> it will never reach nor carries it; catching
    ///     <see cref="NotSupportedException" /> would do neither, and would swallow real failures.
    ///     </para>
    /// </remarks>
    [RequiresUnreferencedCode(
        "Reads the id member and the wrapper's value property reflectively; both must survive trimming.")]
    public static IIdentification ForValueType(Type documentType, MemberInfo idMember, ValueTypeInfo valueType,
        Type sequenceKey)
    {
        ArgumentNullException.ThrowIfNull(documentType);
        ArgumentNullException.ThrowIfNull(idMember);
        ArgumentNullException.ThrowIfNull(valueType);

        if (RuntimeFeature.IsDynamicCodeSupported)
        {
            return CloseValueType(documentType, idMember, valueType, sequenceKey);
        }

        return new ReflectedValueTypeIdentification(documentType, idMember, valueType, sequenceKey);
    }

    [RequiresUnreferencedCode(
        "Builds FEC-compiled accessor / wrap / unwrap delegates via LambdaBuilder and ValueTypeInfo.")]
    [RequiresDynamicCode("Closes ValueTypeIdentification<TDoc, TWrapper, TInner> and compiles accessor delegates.")]
    private static IIdentification CloseValueType(Type documentType, MemberInfo idMember, ValueTypeInfo valueType,
        Type sequenceKey)
        => (IIdentification)Activator.CreateInstance(
            typeof(ValueTypeIdentification<,,>).MakeGenericType(documentType, valueType.OuterType,
                valueType.SimpleType),
            idMember, valueType, sequenceKey)!;

    [RequiresUnreferencedCode("Builds FEC-compiled accessor delegates over the id member via LambdaBuilder.")]
    [RequiresDynamicCode("Closes the strategy's open generic over the document type.")]
    private static IIdentification Close(Type openStrategy, Type documentType, params object?[] arguments)
    {
        ArgumentNullException.ThrowIfNull(documentType);

        var closed = openStrategy.MakeGenericType(documentType);
        return (IIdentification)Activator.CreateInstance(closed, arguments)!;
    }
}
