using JasperFx.Core;

namespace Weasel.Core;

/// <summary>
///     Orders a set of tables so that every table comes before the tables it references through a
///     foreign key, which is the order data has to be removed in: a referenced row can only go once
///     nothing points at it any more.
/// </summary>
internal static class ReferencingTablesFirst
{
    /// <summary>
    ///     Sort <paramref name="tables" /> so a table holding a foreign key to another table in the set
    ///     comes before that table. Only foreign keys between members of the set count; tables with no
    ///     such relationship keep their relative order.
    /// </summary>
    /// <remarks>
    ///     A self-referencing foreign key is ignored, since it says nothing about the order between
    ///     tables. A cycle between two or more tables cannot be ordered: the sort breaks it at the
    ///     edge it meets last, so the result is still a complete, stable ordering of every table, and
    ///     the caller's removal fails on that cycle the same way it would have without the sort.
    /// </remarks>
    public static T[] Sort<T>(IReadOnlyList<T> tables) where T : ITable
    {
        if (tables.Count < 2)
        {
            return tables.ToArray();
        }

        // The "dependencies" of a table are the tables that reference it: TopologicalSort emits a
        // node's dependencies before the node itself, so referencing tables come out first.
        return tables
            .TopologicalSort(table => referencing(tables, table), throwOnCycle: false)
            .ToArray();
    }

    private static IEnumerable<T> referencing<T>(IReadOnlyList<T> tables, T referenced) where T : ITable
    {
        return tables.Where(other =>
            !ReferenceEquals(other, referenced) &&
            other.ForeignKeys.Any(fk => fk.LinkedTable != null && referenced.Identifier.Equals(fk.LinkedTable)));
    }
}
