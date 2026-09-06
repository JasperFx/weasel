using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Weasel.Storage.Flattened;

/// <summary>
///     The reflection and naming odds and ends the flat-table mapping API needs, so that
///     <c>Map(x =&gt; x.MemberCount)</c> means the same column on every store.
/// </summary>
public static class FlatTableMembers
{
    /// <summary>The member a single-member-access lambda reads.</summary>
    /// <exception cref="ArgumentException">The lambda is not a member access.</exception>
    public static MemberInfo MemberOf<TSource, TValue>(Expression<Func<TSource, TValue>> expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var body = Unwrap(expression.Body);

        return body is MemberExpression member
            ? member.Member
            : throw new ArgumentException(
                $"'{expression}' is not a member access. A flat table mapping has to name a property or "
                + "field of the event, such as x => x.Amount.", nameof(expression));
    }

    /// <summary>
    ///     The chain of members a possibly-nested member-access lambda reads, outermost first, or
    ///     <see langword="null" /> when the expression names none.
    /// </summary>
    public static MemberInfo[]? MemberPath(LambdaExpression? expression)
    {
        if (expression is null)
        {
            return null;
        }

        var body = Unwrap(expression.Body);
        var members = new List<MemberInfo>();

        while (body is MemberExpression member)
        {
            members.Insert(0, member.Member);
            body = member.Expression!;
        }

        return members.Count > 0 ? members.ToArray() : null;
    }

    /// <summary>
    ///     The default column name for a mapped member path: each member snake-cased and joined with
    ///     <c>_</c>, so <c>MemberCount</c> becomes <c>member_count</c> and <c>Shipping.City</c>
    ///     becomes <c>shipping_city</c>.
    /// </summary>
    public static string DefaultColumnName(IReadOnlyList<MemberInfo> members)
    {
        ArgumentNullException.ThrowIfNull(members);

        if (members.Count == 1)
        {
            return SnakeCase(members[0].Name);
        }

        var builder = new StringBuilder();

        for (var i = 0; i < members.Count; i++)
        {
            if (i > 0)
            {
                builder.Append('_');
            }

            builder.Append(SnakeCase(members[i].Name));
        }

        return builder.ToString();
    }

    /// <summary>
    ///     <c>MemberCount</c> to <c>member_count</c>, <c>A</c> to <c>a</c>.
    /// </summary>
    public static string SnakeCase(string name)
    {
        ArgumentNullException.ThrowIfNull(name);

        var builder = new StringBuilder(name.Length + 4);

        for (var i = 0; i < name.Length; i++)
        {
            if (i > 0 && char.IsUpper(name[i]))
            {
                builder.Append('_');
            }

            builder.Append(char.ToLowerInvariant(name[i]));
        }

        return builder.ToString();
    }

    private static Expression Unwrap(Expression body)
        => body is UnaryExpression unary ? unary.Operand : body;
}
