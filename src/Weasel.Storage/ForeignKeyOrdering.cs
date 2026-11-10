#nullable enable
namespace Weasel.Storage;

/// <summary>
///     Orders things related by foreign keys — document types, tables — so that
///     creating or writing them never names a row that is not there yet, and deleting them never leaves a
///     row that something still references.
/// </summary>
/// <remarks>
///     <para>
///         A depth-first topological sort with a visited set, which suits the graphs this sees: small and
///         shallow. Items keep their input order wherever the references do not force otherwise.
///     </para>
///     <para>
///         ⚠️ A <b>cycle</b> is left in whatever order the walk reaches it rather than refused. No order
///         satisfies two items that reference each other; refusing here would reject a configuration a
///         migration's deferral can still handle, and a statement that cannot run would fail on its own
///         anyway.
///     </para>
///     <para>
///         A reference to something not among the items is ignored: it constrains nothing in this set.
///     </para>
/// </remarks>
public static class ForeignKeyOrdering
{
    /// <summary>
    ///     Every item after the items it references: the order to create tables or insert rows in.
    /// </summary>
    /// <param name="items">The items to order.</param>
    /// <param name="references">What an item references (its foreign keys' targets).</param>
    /// <param name="comparer">Identity of items; the default comparer when <see langword="null" />.</param>
    public static IReadOnlyList<T> ReferencedFirst<T>(IEnumerable<T> items, Func<T, IEnumerable<T>> references,
        IEqualityComparer<T>? comparer = null) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(references);

        var list = distinct(items, comparer);
        var known = canonical(list, comparer);
        var visited = new HashSet<T>(comparer);
        var ordered = new List<T>(list.Count);

        foreach (var item in list)
        {
            visit(item);
        }

        return ordered;

        void visit(T item)
        {
            if (!visited.Add(item))
            {
                return;
            }

            foreach (var target in references(item))
            {
                // Through the canonical instance, so the result holds the caller's items rather than
                // whatever equal-but-different value the reference function returned.
                if (known.TryGetValue(target, out var referenced))
                {
                    visit(referenced);
                }
            }

            ordered.Add(item);
        }
    }

    /// <summary>
    ///     Every item before the items it references: the order to delete rows or drop tables in.
    /// </summary>
    /// <param name="items">The items to order.</param>
    /// <param name="references">What an item references (its foreign keys' targets).</param>
    /// <param name="comparer">Identity of items; the default comparer when <see langword="null" />.</param>
    public static IReadOnlyList<T> ReferencingFirst<T>(IEnumerable<T> items, Func<T, IEnumerable<T>> references,
        IEqualityComparer<T>? comparer = null) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(references);

        var list = distinct(items, comparer);
        var known = canonical(list, comparer);

        // Inverted once: who references each item, in input order.
        var referencedBy = new Dictionary<T, List<T>>(comparer);
        foreach (var item in list)
        {
            foreach (var target in references(item))
            {
                if (!known.TryGetValue(target, out var referenced))
                {
                    continue;
                }

                if (!referencedBy.TryGetValue(referenced, out var sources))
                {
                    sources = [];
                    referencedBy[referenced] = sources;
                }

                sources.Add(item);
            }
        }

        var visited = new HashSet<T>(comparer);
        var ordered = new List<T>(list.Count);

        foreach (var item in list)
        {
            visit(item);
        }

        return ordered;

        void visit(T item)
        {
            if (!visited.Add(item))
            {
                return;
            }

            if (referencedBy.TryGetValue(item, out var sources))
            {
                foreach (var source in sources)
                {
                    visit(source);
                }
            }

            ordered.Add(item);
        }
    }

    private static Dictionary<T, T> canonical<T>(List<T> items, IEqualityComparer<T>? comparer) where T : notnull
    {
        var map = new Dictionary<T, T>(items.Count, comparer);
        foreach (var item in items)
        {
            map[item] = item;
        }

        return map;
    }

    private static List<T> distinct<T>(IEnumerable<T> items, IEqualityComparer<T>? comparer) where T : notnull
    {
        var seen = new HashSet<T>(comparer);
        var list = new List<T>();
        foreach (var item in items)
        {
            if (seen.Add(item))
            {
                list.Add(item);
            }
        }

        return list;
    }
}
