using System;
using System.Collections.Generic;
using System.Linq;
using Baseline;

namespace Weasel.Postgresql.Tables
{
    public class Change<T>
    {
        public Change(T expected, T actual)
        {
            Expected = expected;
            Actual = actual;
        }

        public T Expected { get; }
        public T Actual { get; }
    }
    
    public class ItemDelta<T> where T: INamed
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
    
    public class TableDelta
    {
        private readonly DbObjectName _tableName;

        public TableDelta(Table expected, Table actual)
        {
            Columns = new ItemDelta<TableColumn>(expected.Columns, actual.Columns);
            Indexes = new ItemDelta<IIndexDefinition>(expected.Indexes, actual.Indexes,
                (e, a) => ActualIndex.Matches(e, a, expected));

            ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys, actual.ForeignKeys);
            
            _tableName = expected.Identifier;
        }
        
        public ItemDelta<TableColumn> Columns { get; }
        public ItemDelta<IIndexDefinition> Indexes { get; }
        
        public ItemDelta<ForeignKey> ForeignKeys { get; }

        public readonly IList<string> AlteredColumnTypes = new List<string>();
        public readonly IList<string> AlteredColumnTypeRollbacks = new List<string>();

        public bool Matches
        {
            get
            {
                if (Columns.HasChanges()) return false;

                if (Indexes.HasChanges()) return false;

                if (ForeignKeys.HasChanges()) return false;

                return true;
            }
        }

        public override string ToString()
        {
            return $"TableDiff for {_tableName}";
        }

    }
}
