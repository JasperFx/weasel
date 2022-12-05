using JasperFx.Core;
using Microsoft.Data.SqlClient;

namespace Weasel.SqlServer.SqlGeneration;

public static class SqlFragmentExtensions
{
    /// <summary>
    ///     Combine an "and" compound filter with the two filters
    /// </summary>
    /// <param name="filter"></param>
    /// <param name="fragments"></param>
    /// <returns></returns>
    public static ISqlFragment CombineAnd(this ISqlFragment filter, ISqlFragment other)
    {
        if (filter is CompoundWhereFragment c && c.Separator.EqualsIgnoreCase("and"))
        {
            c.Add(other);
            return c;
        }

        return CompoundWhereFragment.And(filter, other);
    }

    /// <summary>
    ///     If extras has any items, return an "and" compound fragment. Otherwise return the original filter
    /// </summary>
    /// <param name="filter"></param>
    /// <param name="fragments"></param>
    /// <returns></returns>
    public static ISqlFragment CombineAnd(this ISqlFragment filter, IReadOnlyList<ISqlFragment> extras)
    {
        if (extras.Any())
        {
            if (filter is CompoundWhereFragment c && c.Separator.EqualsIgnoreCase("and"))
            {
                c.Add(extras);
                return c;
            }

            var compound = CompoundWhereFragment.And(extras);
            compound.Add(filter);

            return compound;
        }

        return filter;
    }

    public static ISqlFragment[] Flatten(this ISqlFragment? fragment)
    {
        if (fragment == null)
        {
            return Array.Empty<ISqlFragment>();
        }

        if (fragment is CompoundWhereFragment c)
        {
            return c.Children.ToArray();
        }

        return new[] { fragment };
    }

    public static string? ToSql(this ISqlFragment? fragment)
    {
        if (fragment == null)
        {
            return null;
        }

        var cmd = new SqlCommand();
        var builder = new CommandBuilder(cmd);
        fragment.Apply(builder);

        return builder.ToString().Trim();
    }
}
