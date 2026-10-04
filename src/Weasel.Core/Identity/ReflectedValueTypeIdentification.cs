using System;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Reflection;
using JasperFx.Core.Reflection;
using Weasel.Core.Sequences;

namespace Weasel.Core.Identity;

/// <summary>
///     The strong-typed id strategy, implemented without closing a generic over the wrapper or its
///     inner primitive — the one shape of <see cref="IIdentification" /> that
///     <see cref="ValueTypeIdentification{TDoc,TWrapper,TInner}" /> cannot serve under Native AOT.
/// </summary>
/// <remarks>
///     <para>
///     weasel#690. <c>MakeGenericType</c> closes an instantiation whose arguments are all reference
///     types, because those share one canonical body, and fails when any argument is a value type.
///     For <c>ValueTypeIdentification&lt;TDoc, TWrapper, TInner&gt;</c>, <c>TInner</c> is the wrapped
///     primitive and <c>TWrapper</c> is usually a <c>readonly record struct</c>, so two of the three
///     arguments are value types. Reproduced on macOS arm64, ILC, net9.0:
///     </para>
///     <code>
///     IsDynamicCodeSupported = False
///     SequentialGuidIdentification&lt;Doc&gt;: constructed OK
///     ValueTypeIdentification&lt;Doc,FooId,Guid&gt;: NotSupportedException:
///       'Weasel.Core.Identity.ValueTypeIdentification`3[Doc,FooId,System.Guid]'
///       is missing native code or metadata.
///     </code>
///     <para>
///     So this one reaches the id member through <see cref="PropertyInfo.GetValue(object)" /> /
///     <see cref="PropertyInfo.SetValue(object,object)" /> and the wrapper through
///     <see cref="ValueTypeInfo" />'s own <see cref="ValueTypeInfo.Ctor" /> /
///     <see cref="ValueTypeInfo.Builder" /> — the members jasperfx#942's fallback uses. No generic is
///     closed and no expression is compiled, so nothing here needs code ILC did not generate.
///     </para>
///     <para>
///     It is slower than the generic strategy — every call boxes and goes through reflection rather
///     than an FEC-compiled delegate — so <see cref="Identifications.ForValueType" /> hands it out
///     only where the fast path cannot run. Its answers are required to be identical, and
///     <c>Weasel.Core.Tests/value_type_identification_needs_no_closed_generic.cs</c> holds the two
///     implementations against each other case for case.
///     </para>
/// </remarks>
public sealed class ReflectedValueTypeIdentification: IIdentification
{
    private readonly Func<object, object?> _getter;
    private readonly Func<ISequenceSource, object> _generator;
    private readonly Func<object?, bool> _isDefaultInner;
    private readonly Action<object, object> _setter;
    private readonly PropertyInfo _valueProperty;
    private readonly Func<object, object> _wrap;

    /// <param name="documentType">The document type owning <paramref name="idMember" />.</param>
    /// <param name="idMember">
    ///     The id member — a property or field whose type is the wrapper, or
    ///     <c>Nullable&lt;wrapper&gt;</c>.
    /// </param>
    /// <param name="valueType">The wrapper's shape, from <see cref="ValueTypeInfo.ForType" />.</param>
    /// <param name="sequenceKey">
    ///     The type used as the cache key for <see cref="ISequenceSource.SequenceFor" />, for the
    ///     int / long inner types that generate from a Hi-Lo sequence.
    /// </param>
    [RequiresUnreferencedCode(
        "Reads the id member and the wrapper's value property reflectively; both must survive trimming.")]
    public ReflectedValueTypeIdentification(Type documentType, MemberInfo idMember, ValueTypeInfo valueType,
        Type sequenceKey)
    {
        ArgumentNullException.ThrowIfNull(documentType);
        ArgumentNullException.ThrowIfNull(idMember);
        ArgumentNullException.ThrowIfNull(valueType);

        DocumentType = documentType;
        RawSqlType = valueType.SimpleType;
        _valueProperty = valueType.ValueProperty;
        (_getter, _setter) = BuildAccessors(idMember);
        _wrap = BuildWrapper(valueType);
        _generator = PickGenerator(valueType.SimpleType, sequenceKey);
        _isDefaultInner = BuildDefaultTest(valueType.SimpleType);
    }

    /// <summary>
    ///     The document type this strategy was built for. A caller holding only
    ///     <see cref="IIdentification" /> has no type argument to read it off.
    /// </summary>
    public Type DocumentType { get; }

    /// <inheritdoc />
    public Type RawSqlType { get; }

    /// <inheritdoc />
    public object Identity(object document)
    {
        ArgumentNullException.ThrowIfNull(document);

        return _getter(document) ?? throw new InvalidOperationException(
            $"The id on {DocumentType.FullName} has not been assigned. Call AssignIfMissing first.");
    }

    /// <inheritdoc />
    public object AssignIfMissing(object document, ISequenceSource sequences)
    {
        ArgumentNullException.ThrowIfNull(document);

        // A null here is either a Nullable<wrapper> property that has never been set or a reference
        // wrapper left null -- both mean "generate one", and neither can be unwrapped to look at.
        var current = _getter(document);
        if (current is not null && !_isDefaultInner(InnerOf(current)))
        {
            return current;
        }

        var wrapped = _wrap(_generator(sequences));
        _setter(document, wrapped);
        return wrapped;
    }

    /// <inheritdoc />
    public object ToRawSqlValue(object id)
    {
        ArgumentNullException.ThrowIfNull(id);

        return InnerOf(id)!;
    }

    /// <inheritdoc />
    public object ReadIdFromReader(DbDataReader reader, int columnOrdinal)
    {
        ArgumentNullException.ThrowIfNull(reader);

        // The generic strategy reads reader.GetFieldValue<TInner>, which is exactly the generic call
        // that cannot be closed here. GetValue plus a conversion is the same answer: the provider
        // hands back the column's own CLR type, and the only time it differs from the inner type is
        // a widened integer.
        var inner = reader.GetValue(columnOrdinal);
        if (inner.GetType() != RawSqlType)
        {
            inner = Convert.ChangeType(inner, RawSqlType, CultureInfo.InvariantCulture);
        }

        return _wrap(inner);
    }

    /// <summary>
    ///     The wrapper's inner primitive. Vogen wrappers throw "Use of uninitialized Value Object"
    ///     from the value accessor rather than returning a default, and the generic strategy lets that
    ///     through — so unwrap the <see cref="TargetInvocationException" /> reflection adds, to keep
    ///     the two implementations throwing the same thing.
    /// </summary>
    private object? InnerOf(object wrapper)
    {
        try
        {
            return _valueProperty.GetValue(wrapper);
        }
        catch (TargetInvocationException e) when (e.InnerException is not null)
        {
            throw e.InnerException;
        }
    }

    [RequiresUnreferencedCode("Reads the id member reflectively; it must survive trimming.")]
    private static (Func<object, object?> Getter, Action<object, object> Setter) BuildAccessors(MemberInfo idMember)
    {
        switch (idMember)
        {
            case PropertyInfo property:
                return (property.GetValue, property.SetValue);
            case FieldInfo field:
                return (field.GetValue, field.SetValue);
            default:
                throw new ArgumentException(
                    $"Strong-typed id member must be a property or field; got {idMember.MemberType}.",
                    nameof(idMember));
        }
    }

    private static Func<object, object> BuildWrapper(ValueTypeInfo valueType)
    {
        if (valueType.Builder is { } builder)
        {
            return inner => builder.Invoke(null, [inner])
                            ?? throw new InvalidOperationException(
                                $"{builder.DeclaringType?.FullName}.{builder.Name} returned null for a strong-typed id.");
        }

        if (valueType.Ctor is { } ctor)
        {
            return inner => ctor.Invoke([inner]);
        }

        throw new NotSupportedException(
            $"{valueType.OuterType.FullName} exposes neither a single-argument constructor nor a static factory method, so Weasel cannot build one from its inner {valueType.SimpleType.Name} value.");
    }

    private static Func<ISequenceSource, object> PickGenerator(Type simpleType, Type sequenceKey)
    {
        if (simpleType == typeof(Guid))
        {
            return _ => Guid.CreateVersion7();
        }

        if (simpleType == typeof(int))
        {
            return sequences => sequences.SequenceFor(sequenceKey).NextInt();
        }

        if (simpleType == typeof(long))
        {
            return sequences => sequences.SequenceFor(sequenceKey).NextLong();
        }

        if (simpleType == typeof(string))
        {
            return _ => throw new InvalidOperationException(
                "Strong-typed string ids are externally assigned — the caller must populate the id before saving.");
        }

        throw new NotSupportedException($"Strong-typed id inner type {simpleType.FullName} is not supported.");
    }

    /// <summary>
    ///     Stands in for <c>EqualityComparer&lt;TInner&gt;.Default.Equals(inner, default)</c>, which
    ///     is itself a generic that cannot be closed over a value type here. Written out over the
    ///     four supported inner types rather than through <c>Activator.CreateInstance</c>, which
    ///     would need a trim annotation this call site cannot honestly give.
    /// </summary>
    /// <remarks>
    ///     Note that the default for a string inner type is <c>null</c> and not <c>""</c> — an empty
    ///     externally-assigned key is still a key, and the generic strategy treats it as one.
    /// </remarks>
    private static Func<object?, bool> BuildDefaultTest(Type simpleType)
    {
        if (simpleType == typeof(Guid))
        {
            return inner => inner is null || Guid.Empty.Equals(inner);
        }

        if (simpleType == typeof(int))
        {
            return inner => inner is null || 0.Equals(inner);
        }

        if (simpleType == typeof(long))
        {
            return inner => inner is null || 0L.Equals(inner);
        }

        if (simpleType == typeof(string))
        {
            return inner => inner is null;
        }

        throw new NotSupportedException($"Strong-typed id inner type {simpleType.FullName} is not supported.");
    }
}
