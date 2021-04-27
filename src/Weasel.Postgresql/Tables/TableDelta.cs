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
        private readonly Table _table;
        private readonly Table _actual;

        public TableDelta(Table table, Table actual)
        {
            _table = table;
            _actual = actual;

            if (actual == null)
            {
                Difference = SchemaPatchDifference.None;
                return;
            }
            
            Columns = new ItemDelta<TableColumn>(table.Columns, actual.Columns);
            Indexes = new ItemDelta<IIndexDefinition>(table.Indexes, actual.Indexes,
                (e, a) => ActualIndex.Matches(e, a, table));

            ForeignKeys = new ItemDelta<ForeignKey>(table.ForeignKeys, actual.ForeignKeys);
            
            _tableName = table.Identifier;

            PrimaryKeyDifference = SchemaPatchDifference.None;
            if (table.PrimaryKeyName.IsEmpty())
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
            else if (!table.PrimaryKeyColumns.SequenceEqual(actual.PrimaryKeyColumns))
            {
                PrimaryKeyDifference = SchemaPatchDifference.Update;
            }

            Difference = determinePatchDifference();
        }

        public ISchemaObject SchemaObject => _table;
        public SchemaPatchDifference Difference { get; }

        public void WriteUpdate(DdlRules rules, StringWriter writer)
        {
            if (Difference == SchemaPatchDifference.Invalid)
            {
                throw new InvalidOperationException($"TableDelta for {_tableName} is invalid");
            }
            
            if (Difference == SchemaPatchDifference.Create)
            {
                SchemaObject.WriteCreateStatement(rules, writer);
            }

            writeColumnUpdates(writer);
            
            writeIndexUpdates(writer);

            writeForeignKeyUpdates(writer);

            switch (PrimaryKeyDifference)
            {
                case SchemaPatchDifference.Invalid:
                case SchemaPatchDifference.Update:
                    writer.WriteLine($"alter table {_table.Identifier} drop constraint {_actual.PrimaryKeyName};");
                    writer.WriteLine($"alter table {_table.Identifier} add {_table.PrimaryKeyDeclaration()};");
                    break;
                
                case SchemaPatchDifference.Create:
                    writer.WriteLine($"alter table {_table.Identifier} add {_table.PrimaryKeyDeclaration()};");
                    break;
            }
        }

        private void writeForeignKeyUpdates(StringWriter writer)
        {
            foreach (var foreignKey in ForeignKeys.Missing)
            {
                foreignKey.WriteAddStatement(_table, writer);
            }

            foreach (var foreignKey in ForeignKeys.Extras)
            {
                foreignKey.WriteDropStatement(_table, writer);
            }

            foreach (var change in ForeignKeys.Different)
            {
                change.Actual.WriteDropStatement(_table, writer);
                change.Expected.WriteAddStatement(_table, writer);
            }
        }

        private void writeIndexUpdates(StringWriter writer)
        {
            // Missing indexes
            foreach (var indexDefinition in Indexes.Missing)
            {
                writer.WriteLine(indexDefinition.ToDDL(_table));
            }

            // Extra indexes
            foreach (var extra in Indexes.Extras)
            {
                writer.WriteLine($"drop index concurrently if exists {_table.Identifier.Schema}.{extra.Name};");
            }

            // Different indexes
            foreach (var change in Indexes.Different)
            {
                writer.WriteLine($"drop index concurrently if exists {_table.Identifier.Schema}.{change.Actual.Name};");
                writer.WriteLine(change.Expected.ToDDL(_table));
            }
        }

        private void writeColumnUpdates(StringWriter writer)
        {
            // Missing columns
            foreach (var column in Columns.Missing)
            {
                writer.WriteLine(column.AddColumnSql(_table));
            }

            // Extra columns
            foreach (var column in Columns.Extras)
            {
                writer.WriteLine($"alter table {_table.Identifier} drop column {column.Name};");
            }

            // Different columns
            foreach (var change in Columns.Different)
            {
                writer.WriteLine(change.Expected.AlterColumnTypeSql(_table, change.Actual));
            }
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
