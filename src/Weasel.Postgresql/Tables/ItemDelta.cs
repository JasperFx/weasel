using System;
using System.Collections.Generic;
using System.Linq;

namespace Weasel.Postgresql.Tables
{
    internal class ItemDelta<T> where T: INamed
    {
        private readonly List<Change<T>> _different = new List<Change<T>>();
        private readonly List<T> _matched = new List<T>();
        private readonly List<T> _extras = new List<T>();
        private readonly List<T> _missing = new List<T>();

        public bool HasChanges()
        {
            return _different.Any() || _extras.Any() || _missing.Any();
        }

        public IReadOnlyList<Change<T>> Different => _different;

        public IReadOnlyList<T> Matched => _matched;

        public IReadOnlyList<T> Extras => _extras;

        public IReadOnlyList<T> Missing => _missing;

        public SchemaPatchDifference Difference()
        {
            if (!HasChanges()) return SchemaPatchDifference.None;

            return SchemaPatchDifference.Update;
        }
        
        public ItemDelta(IEnumerable<T> expectedItems, IEnumerable<T> actualItems, Func<T, T, bool> comparison = null)
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
    }
}