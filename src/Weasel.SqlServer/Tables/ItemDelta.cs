using Weasel.Core;

namespace Weasel.SqlServer.Tables;

internal class ItemDelta<T> where T : INamed
{
    private readonly List<Change<T>> _different = new();
    private readonly List<T> _extras = new();
    private readonly List<T> _matched = new();
    private readonly List<T> _missing = new();

    public ItemDelta(IEnumerable<T> expectedItems, IEnumerable<T> actualItems, Func<T, T, bool>? comparison = null)
    {
        comparison ??= (expected, actual) => expected.Equals(actual);
        // SQL Server identifiers are case-insensitive under the default collation
        var expecteds = expectedItems.ToDictionary(NameKey, StringComparer.OrdinalIgnoreCase);

        foreach (var actual in actualItems)
        {
            if (expecteds.TryGetValue(NameKey(actual), out var expected))
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

        var actuals = actualItems.ToDictionary(NameKey, StringComparer.OrdinalIgnoreCase);
        _missing.AddRange(expectedItems.Where(x => !actuals.ContainsKey(NameKey(x))));
    }

    /// <summary>
    ///     Pair on the undelimited name. The provider's own types normalize as names arrive,
    ///     but <see cref="TableCheckConstraint" /> is a shared Weasel.Core type the caller
    ///     constructs directly, so a name they bracketed themselves reaches the comparison
    ///     as-is and would never match the bare name the database reports.
    /// </summary>
    private static string NameKey(T item) => SchemaUtils.Unbracket(item.Name);

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
