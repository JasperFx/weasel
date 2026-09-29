using Weasel.Core;

namespace Weasel.Postgresql.Tables;

internal class ItemDelta<T> where T : INamed
{
    private readonly List<Change<T>> _different = new();
    private readonly List<T> _extras = new();
    private readonly List<T> _matched = new();
    private readonly List<T> _missing = new();

    public ItemDelta(IEnumerable<T> expectedItems, IEnumerable<T> actualItems, Func<T, T, bool>? comparison = null)
    {
        comparison ??= (expected, actual) => expected.Equals(actual);

        // Name matching is case-insensitive: PostgreSQL folds unquoted identifiers
        // to lowercase, so an expected case-preserved name ("Id") and the catalog's
        // folded or quoted spelling must pair up as the same item.
        var expecteds = expectedItems.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);

        foreach (var actual in actualItems)
        {
            if (expecteds.TryGetValue(actual.Name, out var expected))
            {
                if (comparison(expected, actual))
                {
                    _matched.Add(actual);
                }
                else
                {
                    _different.Add(new Change<T>(expected, actual));
                }
            }
            else
            {
                _extras.Add(actual);
            }
        }

        var actuals = actualItems.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);
        _missing.AddRange(expectedItems.Where(x => !actuals.ContainsKey(x.Name)));
    }

    /// <summary>
    ///     The delta against no actuals at all -- a table that is not in the database yet -- where
    ///     everything expected is missing by definition (weasel#658).
    /// </summary>
    /// <remarks>
    ///     Deliberately does not go through the pairing constructor: there is nothing to pair with, and
    ///     that constructor refuses a table declaring two names that differ only in case (weasel#224),
    ///     which a table being created for the first time is still allowed to do.
    /// </remarks>
    public static ItemDelta<T> AllMissing(IEnumerable<T> expectedItems) => new(expectedItems);

    private ItemDelta(IEnumerable<T> expectedItems)
    {
        _missing.AddRange(expectedItems);
    }

    public IReadOnlyList<Change<T>> Different => _different;

    public IReadOnlyList<T> Matched => _matched;

    public IReadOnlyList<T> Extras => _extras;

    public IReadOnlyList<T> Missing => _missing;

    public bool HasChanges()
    {
        return _different.Any() || _extras.Any() || _missing.Any();
    }

    public SchemaPatchDifference Difference()
    {
        if (!HasChanges())
        {
            return SchemaPatchDifference.None;
        }

        return SchemaPatchDifference.Update;
    }
}
