#nullable enable
using Weasel.Core;
using Weasel.Core.SqlGeneration;

namespace Weasel.Storage;

/// <summary>
///     Two or more filters joined with <c>and</c>, each in parentheses so a child holding an <c>or</c>
///     cannot capture its neighbour. A compound fragment, so a store searching the tree for a filter of
///     its own (a soft-delete or tenant filter, say) still finds the children.
/// </summary>
internal sealed class ClosedShapeAndFragment: ICompoundFragment
{
    private readonly ISqlFragment[] _children;

    private ClosedShapeAndFragment(ISqlFragment[] children)
    {
        _children = children;
    }

    public static ISqlFragment Of(ISqlFragment first, ISqlFragment second)
        => new ClosedShapeAndFragment([first, second]);

    public IEnumerable<ISqlFragment> Children => _children;

    public void Apply(ICommandBuilder builder)
    {
        for (var i = 0; i < _children.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(" and ");
            }

            builder.Append("(");
            _children[i].Apply(builder);
            builder.Append(")");
        }
    }
}
