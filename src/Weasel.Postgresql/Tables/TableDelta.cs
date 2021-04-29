using System;
using System.IO;
using System.Linq;
using Baseline;

namespace Weasel.Postgresql.Tables
{
    public class TableDelta : SchemaObjectDelta<Table>
    {
        public TableDelta(Table expected, Table actual) : base(expected, actual)
        {

        }

        protected override SchemaPatchDifference compare(Table expected, Table actual)
        {
            if (actual == null)
            {
                return SchemaPatchDifference.None;
            }
            
            Columns = new ItemDelta<TableColumn>(expected.Columns, actual.Columns);
            Indexes = new ItemDelta<IIndexDefinition>(expected.Indexes, actual.Indexes,
                (e, a) => ActualIndex.Matches(e, a, expected));

            ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys, actual.ForeignKeys);
            
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
            
            return determinePatchDifference();
        }

        public override void WriteUpdate(DdlRules rules, TextWriter writer)
        {
            if (Difference == SchemaPatchDifference.Invalid)
            {
                throw new InvalidOperationException($"TableDelta for {Expected.Identifier} is invalid");
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
                    writer.WriteLine($"alter table {Expected.Identifier} drop constraint {Actual.PrimaryKeyName};");
                    writer.WriteLine($"alter table {Expected.Identifier} add {Expected.PrimaryKeyDeclaration()};");
                    break;
                
                case SchemaPatchDifference.Create:
                    writer.WriteLine($"alter table {Expected.Identifier} add {Expected.PrimaryKeyDeclaration()};");
                    break;
            }
        }

        private void writeForeignKeyUpdates(TextWriter writer)
        {
            foreach (var foreignKey in ForeignKeys.Missing)
            {
                foreignKey.WriteAddStatement(Expected, writer);
            }

            foreach (var foreignKey in ForeignKeys.Extras)
            {
                foreignKey.WriteDropStatement(Expected, writer);
            }

            foreach (var change in ForeignKeys.Different)
            {
                change.Actual.WriteDropStatement(Expected, writer);
                change.Expected.WriteAddStatement(Expected, writer);
            }
        }

        private void writeIndexUpdates(TextWriter writer)
        {
            // Missing indexes
            foreach (var indexDefinition in Indexes.Missing)
            {
                writer.WriteLine(indexDefinition.ToDDL(Expected));
            }

            // Extra indexes
            foreach (var extra in Indexes.Extras)
            {
                writer.WriteLine($"drop index concurrently if exists {Expected.Identifier.Schema}.{extra.Name};");
            }

            // Different indexes
            foreach (var change in Indexes.Different)
            {
                writer.WriteLine($"drop index concurrently if exists {Expected.Identifier.Schema}.{change.Actual.Name};");
                writer.WriteLine(change.Expected.ToDDL(Expected));
            }
        }

        private void writeColumnUpdates(TextWriter writer)
        {
            // Missing columns
            foreach (var column in Columns.Missing)
            {
                writer.WriteLine(column.AddColumnSql(Expected));
            }

            // Extra columns
            foreach (var column in Columns.Extras)
            {
                writer.WriteLine($"alter table {Expected.Identifier} drop column {column.Name};");
            }

            // Different columns
            foreach (var change in Columns.Different)
            {
                writer.WriteLine(change.Expected.AlterColumnTypeSql(Expected, change.Actual));
            }
        }

        public override void WriteRollback(DdlRules rules, TextWriter writer)
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
        
        internal ItemDelta<TableColumn> Columns { get; private set; }
        internal ItemDelta<IIndexDefinition> Indexes { get; private set; }
        
        internal ItemDelta<ForeignKey> ForeignKeys { get; private set; }

        public bool HasChanges()
        {
            return Columns.HasChanges() || Indexes.HasChanges() || ForeignKeys.HasChanges() || PrimaryKeyDifference != SchemaPatchDifference.None;
        }

        public SchemaPatchDifference PrimaryKeyDifference { get; private set; }

        public override string ToString()
        {
            return $"TableDelta for {Expected.Identifier}";
        }

    }
}
