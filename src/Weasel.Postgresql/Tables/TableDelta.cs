using System;
using System.Collections.Generic;
using System.IO;
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
    
    public class TableDelta : ISchemaObjectDelta
    {
        private readonly DbObjectName _tableName;

        public TableDelta(Table expected, Table actual)
        {
            SchemaObject = expected;

            if (actual == null)
            {
                Difference = SchemaPatchDifference.None;
                return;
            }
            
            Columns = new ItemDelta<TableColumn>(expected.Columns, actual.Columns);
            Indexes = new ItemDelta<IIndexDefinition>(expected.Indexes, actual.Indexes,
                (e, a) => ActualIndex.Matches(e, a, expected));

            ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys, actual.ForeignKeys);
            
            _tableName = expected.Identifier;

            PrimaryKeyDifference = SchemaPatchDifference.None;
            if (expected.PrimaryKeyName.IsEmpty())
            {
                if (actual.PrimaryKeyName.IsNotEmpty())
                {
                    PrimaryKeyDifference = SchemaPatchDifference.Update;
                }
            }
            else if (actual.PrimaryKeyName.IsEmpty())
            {
                PrimaryKeyDifference = SchemaPatchDifference.Create;
            }
            else if (!expected.PrimaryKeyColumns.SequenceEqual(actual.PrimaryKeyColumns))
            {
                PrimaryKeyDifference = SchemaPatchDifference.Update;
            }

            Difference = determinePatchDifference();
        }

        public ISchemaObject SchemaObject { get; }
        public SchemaPatchDifference Difference { get; }

        public void WriteUpdate(DdlRules rules, StringWriter writer)
        {
            throw new NotImplementedException();
        }

        public void WriteRollback(DdlRules rules, StringWriter writer)
        {
            throw new NotImplementedException();
        }

        private SchemaPatchDifference determinePatchDifference()
        {
            if (!HasChanges()) return SchemaPatchDifference.None;
            
            // If there are any columns that are different and at least one cannot
            // automatically generate an `ALTER TABLE` statement, the patch is invalid
            if (Columns.Different.Any(x => !x.Expected.CanAlter(x.Actual)))
            {
                return SchemaPatchDifference.Invalid;
            }

            // If there are any missing columns and at least one
            // cannot generate an `ALTER TABLE * ADD COLUMN` statement
            if (Columns.Missing.Any(x => !x.CanAdd()))
            {
                return SchemaPatchDifference.Invalid;
            }

            var differences = new SchemaPatchDifference[]
            {
                Columns.Difference(),
                ForeignKeys.Difference(),
                Indexes.Difference(),
                PrimaryKeyDifference
            };

            return differences.Min();
        }
        
        public ItemDelta<TableColumn> Columns { get; }
        public ItemDelta<IIndexDefinition> Indexes { get; }
        
        public ItemDelta<ForeignKey> ForeignKeys { get; }

        public bool HasChanges()
        {
            return Columns.HasChanges() || Indexes.HasChanges() || ForeignKeys.HasChanges() || PrimaryKeyDifference != SchemaPatchDifference.None;
        }

        public SchemaPatchDifference PrimaryKeyDifference { get; }

        public override string ToString()
        {
            return $"TableDiff for {_tableName}";
        }

    }
}
