using System.Text;
using Weasel.Core;

namespace Weasel.Firebird.Tables;

internal static class StringWriterExtensions
{
    /// <summary>
    ///     Append a referential action. <see cref="CascadeAction.NoAction" /> and
    ///     <see cref="CascadeAction.Restrict" /> write nothing: an absent clause is Firebird's default,
    ///     which it records as <c>RESTRICT</c>, and <c>ON DELETE RESTRICT</c> itself is a syntax error.
    /// </summary>
    public static void AppendCascadeAction(this StringBuilder builder, string prefix, CascadeAction action)
    {
        var clause = action switch
        {
            CascadeAction.Cascade => "CASCADE",
            CascadeAction.SetNull => "SET NULL",
            CascadeAction.SetDefault => "SET DEFAULT",
            _ => null
        };

        if (clause != null)
        {
            builder.Append(' ').Append(prefix).Append(' ').Append(clause);
        }
    }
}
