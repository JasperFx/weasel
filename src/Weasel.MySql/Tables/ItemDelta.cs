using Weasel.Core;

namespace Weasel.MySql.Tables;

public class ItemDelta<T> where T : class, INamed
{
    public ItemDelta(IEnumerable<T> expected, IEnumerable<T> actual, Func<T, T, bool> comparison)
    {
        var expectedList = expected.ToList();
        var actualList = actual.ToList();

        foreach (var item in expectedList)
        {
            var match = actualList.FirstOrDefault(a =>
                a.Name.Equals(item.Name, StringComparison.OrdinalIgnoreCase));

            if (match == null)
            {
                Missing.Add(item);
            }
            else if (!comparison(item, match))
            {
                Different.Add(new Change<T>(item, match));
            }
            else
            {
                Matched.Add(item);
            }
        }

        foreach (var item in actualList)
        {
            var match = expectedList.FirstOrDefault(e =>
                e.Name.Equals(item.Name, StringComparison.OrdinalIgnoreCase));

            if (match == null)
            {
                Extras.Add(item);
            }
        }
    }

    public List<T> Missing { get; } = new();
    public List<T> Extras { get; } = new();
    public List<T> Matched { get; } = new();
    public List<Change<T>> Different { get; } = new();

    public bool HasChanges() => Missing.Any() || Extras.Any() || Different.Any();
}
