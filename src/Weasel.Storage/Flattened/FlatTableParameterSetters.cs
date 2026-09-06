using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using JasperFx.Events;

namespace Weasel.Storage.Flattened;

/// <summary>
///     How a store converts a CLR value into what its database should actually hold.
/// </summary>
/// <remarks>
///     Supplied by the store rather than assumed here, because the conversions are genuinely
///     store-shaped: SQLite has to write a Guid as lowercase canonical text and a bool as 0/1, SQL
///     Server has to unwrap a strong-typed id to its inner primitive, and PostgreSQL needs neither.
///     The default is to hand the value over untouched.
/// </remarks>
public delegate object? FlatTableValueConversion(object? value);

/// <summary>
///     Pulls one parameter value out of an event for a flat-table statement.
/// </summary>
/// <remarks>
///     Deliberately provider-neutral: it yields the value rather than setting it on a provider's
///     parameter type, so the same setters serve a store that binds
///     <see cref="System.Data.Common.DbParameter" /> objects and one that hands a dictionary to a
///     command builder.
/// </remarks>
public interface IFlatTableParameterSetter
{
    /// <summary>The value this setter contributes for <paramref name="source" />.</summary>
    object? ValueFor(IEvent source);
}

/// <summary>Reads a member off the event body through a compiled accessor.</summary>
public sealed class EventMemberParameterSetter<TEvent>: IFlatTableParameterSetter
{
    private readonly Func<TEvent, object?> _accessor;
    private readonly FlatTableValueConversion? _conversion;

    public EventMemberParameterSetter(Func<TEvent, object?> accessor, FlatTableValueConversion? conversion = null)
    {
        _accessor = accessor ?? throw new ArgumentNullException(nameof(accessor));
        _conversion = conversion;
    }

    /// <inheritdoc />
    public object? ValueFor(IEvent source)
    {
        var value = _accessor((TEvent)source.Data);

        return _conversion is null ? value : _conversion(value);
    }
}

/// <summary>The stream's Guid identity, for a table keyed on the stream.</summary>
public sealed class StreamIdParameterSetter: IFlatTableParameterSetter
{
    private readonly FlatTableValueConversion? _conversion;

    public StreamIdParameterSetter(FlatTableValueConversion? conversion = null)
    {
        _conversion = conversion;
    }

    /// <inheritdoc />
    public object? ValueFor(IEvent source)
        => _conversion is null ? source.StreamId : _conversion(source.StreamId);
}

/// <summary>The stream's string identity, for a store configured <c>AsString</c>.</summary>
public sealed class StreamKeyParameterSetter: IFlatTableParameterSetter
{
    private readonly FlatTableValueConversion? _conversion;

    public StreamKeyParameterSetter(FlatTableValueConversion? conversion = null)
    {
        _conversion = conversion;
    }

    /// <inheritdoc />
    public object? ValueFor(IEvent source)
        => _conversion is null ? source.StreamKey : _conversion(source.StreamKey);
}

/// <summary>
///     Factories for the setters a flat-table mapping needs.
/// </summary>
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification =
        "Class-level: compiles member-access lambdas over event types supplied at projection registration, "
        + "which are preserved per the AOT publishing guide.")]
public static class FlatTableParameterSetters
{
    /// <summary>
    ///     A setter that walks <paramref name="members" /> from the event body.
    /// </summary>
    /// <remarks>
    ///     The accessor is compiled to <c>Func&lt;TEvent, object?&gt;</c> rather than to the member's
    ///     own type, so nothing here has to close a generic over a type only known at runtime. The
    ///     value is boxed either way — <see cref="IFlatTableParameterSetter.ValueFor" /> yields
    ///     <see cref="object" />.
    /// </remarks>
    public static IFlatTableParameterSetter ForMembers<TEvent>(IReadOnlyList<MemberInfo> members,
        FlatTableValueConversion? conversion = null)
    {
        ArgumentNullException.ThrowIfNull(members);

        if (members.Count == 0)
        {
            throw new ArgumentException("At least one member is required.", nameof(members));
        }

        var parameter = Expression.Parameter(typeof(TEvent), "x");
        Expression body = parameter;

        foreach (var member in members)
        {
            body = Expression.MakeMemberAccess(body, member);
        }

        if (body.Type != typeof(object))
        {
            body = Expression.Convert(body, typeof(object));
        }

        var accessor = Expression.Lambda<Func<TEvent, object?>>(body, parameter).Compile();

        return new EventMemberParameterSetter<TEvent>(accessor, conversion);
    }

    /// <summary>
    ///     A setter reading the member a mapping lambda names.
    /// </summary>
    public static IFlatTableParameterSetter ForMember<TEvent, TValue>(
        Expression<Func<TEvent, TValue>> expression, FlatTableValueConversion? conversion = null)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var compiled = expression.Compile();

        return new EventMemberParameterSetter<TEvent>(e => compiled(e), conversion);
    }

    /// <summary>
    ///     The setter for a table keyed on the stream rather than on a member of the event.
    /// </summary>
    public static IFlatTableParameterSetter ForStream(StreamIdentity identity,
        FlatTableValueConversion? conversion = null)
        => identity == StreamIdentity.AsGuid
            ? new StreamIdParameterSetter(conversion)
            : new StreamKeyParameterSetter(conversion);
}
