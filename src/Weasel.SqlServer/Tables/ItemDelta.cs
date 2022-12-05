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
        var expecteds = expectedItems.ToDictionary(x => x.Name);

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

        var actuals = actualItems.ToDictionary(x => x.Name);
        _missing.AddRange(expectedItems.Where(x => !actuals.ContainsKey(x.Name)));
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
