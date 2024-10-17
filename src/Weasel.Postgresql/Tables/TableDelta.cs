using JasperFx.Core;
using Weasel.Core;
using Weasel.Postgresql.Tables.Partitioning;

namespace Weasel.Postgresql.Tables;

public class TableDelta: SchemaObjectDelta<Table>
{


    public TableDelta(Table expected, Table? actual): base(expected, actual)
    {

    }

    public IPartition[] MissingPartitions { get; private set; } = Array.Empty<IPartition>();

    public PartitionDelta PartitionDelta { get; private set; } = PartitionDelta.None;

    internal ItemDelta<TableColumn> Columns { get; private set; } = null!;
    internal ItemDelta<IndexDefinition> Indexes { get; private set; } = null!;

    internal ItemDelta<ForeignKey> ForeignKeys { get; private set; } = null!;

    public SchemaPatchDifference PrimaryKeyDifference { get; private set; }

    protected override SchemaPatchDifference compare(Table expected, Table? actual)
    {
        if (actual == null)
        {
            return SchemaPatchDifference.Create;
        }

        Columns = new ItemDelta<TableColumn>(expected.Columns, actual.Columns);
        Indexes = new ItemDelta<IndexDefinition>(expected.Indexes.Where(x => !expected.HasIgnoredIndex(x.Name)),
            actual.Indexes.Where(x => !expected.HasIgnoredIndex(x.Name)),
            (e, a) => e.Matches(a, Expected));

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
        else if (actual.PrimaryKeyName != expected.PrimaryKeyName &&
                 actual.TruncatedNameIdentifier(actual.PrimaryKeyName) !=
                 expected.TruncatedNameIdentifier(expected.PrimaryKeyName))
        {
            PrimaryKeyDifference = SchemaPatchDifference.Update;
        }
        else if (!expected.PrimaryKeyColumns.SequenceEqual(actual.PrimaryKeyColumns) &&
                 !expected.PrimaryKeyColumns.Select(expected.TruncatedNameIdentifier).SequenceEqual(
                     actual.PrimaryKeyColumns.Select(actual.TruncatedNameIdentifier)))
        {
            PrimaryKeyDifference = SchemaPatchDifference.Update;
        }

        if (expected.Partitioning == null)
        {
            if (actual.Partitioning != null)
            {
                PartitionDelta = PartitionDelta.Rebuild;
            }
        }
        else
        {
            if (actual.Partitioning == null)
            {
                PartitionDelta = PartitionDelta.Rebuild;
            }
            else
            {
                PartitionDelta = expected.Partitioning.CreateDelta(expected, actual.Partitioning, out var missing);
                MissingPartitions = missing;
            }
        }

        return determinePatchDifference();
    }

    public override void WriteUpdate(Migrator rules, TextWriter writer)
    {
        if (Difference == SchemaPatchDifference.Invalid)
        {
            throw new InvalidOperationException($"TableDelta for {Expected.Identifier} is invalid");
        }

        if (Difference == SchemaPatchDifference.Create)
        {
            SchemaObject.WriteCreateStatement(rules, writer);
            return;
        }

        // Extra indexes
        foreach (var extra in Indexes.Extras) writer.WriteDropIndex(Expected, extra);

        // Different indexes
        foreach (var change in Indexes.Different) writer.WriteDropIndex(Expected, change.Actual);

        // Missing columns
        foreach (var column in Columns.Missing) writer.WriteLine(column.AddColumnSql(Expected));

        // Different columns
        foreach (var change1 in Columns.Different)
            writer.WriteLine(change1.Expected.AlterColumnTypeSql(Expected, change1.Actual));

        writeForeignKeyUpdates(writer);

        // Missing indexes
        foreach (var indexDefinition in Indexes.Missing) writer.WriteLine(indexDefinition.ToDDL(Expected));

        // Different indexes
        foreach (var change in Indexes.Different) writer.WriteLine(change.Expected.ToDDL(Expected));

        // Need to make Primary key changes before dropping extra columns
        writePrimaryKeyChanges(writer);

        // Extra columns
        foreach (var column in Columns.Extras) writer.WriteLine(column.DropColumnSql(Expected));

        if (this.PartitionDelta == PartitionDelta.Additive)
        {
            foreach (var partition in MissingPartitions)
            {
                writer.WriteLine();
                partition.WriteCreateStatement(writer, Expected);
            }
        }
        else if (this.PartitionDelta == PartitionDelta.Rebuild)
        {
            var columns = Expected.Columns.Select(x => x.Name).Join(", ");

            var tempName = new DbObjectName(Expected.Identifier.Schema, Expected.Identifier.Name + "_temp");
            writer.WriteLine($"create table {tempName} as select * from {Expected.Identifier};");
            writer.WriteLine($"drop table {Expected.Identifier} cascade;");

            Expected.WriteCreateStatement(rules, writer);

            writer.WriteLine();

            writer.WriteLine($"insert into {Expected.Identifier}({columns}) select {columns} from {tempName};");

            writer.WriteLine($"drop table {tempName} cascade;");
        }
    }

    private void writePrimaryKeyChanges(TextWriter writer)
    {
        switch (PrimaryKeyDifference)
        {
            case SchemaPatchDifference.Invalid:
            case SchemaPatchDifference.Update:
                if (Expected.PrimaryKeyColumns.SequenceEqual(Actual!.PrimaryKeyColumns))
                {
                    //for when PK constraint name changes only
                    writer.WriteLine(
                        $"alter table {Expected.Identifier} rename constraint {Actual!.PrimaryKeyName} to {Expected.PrimaryKeyName};");
                    break;
                }

                writer.WriteLine($"alter table {Expected.Identifier} drop constraint {Actual!.PrimaryKeyName} CASCADE;");
                writer.WriteLine($"alter table {Expected.Identifier} add {Expected.PrimaryKeyDeclaration()};");
                break;

            case SchemaPatchDifference.Create:
                writer.WriteLine($"alter table {Expected.Identifier} add {Expected.PrimaryKeyDeclaration()};");
                break;
        }
    }

    private void writeForeignKeyUpdates(TextWriter writer)
    {
        foreach (var foreignKey in ForeignKeys.Missing) foreignKey.WriteAddStatement(Expected, writer);

        foreach (var foreignKey in ForeignKeys.Extras) foreignKey.WriteDropStatement(Expected, writer);

        foreach (var change in ForeignKeys.Different)
        {
            change.Actual.WriteDropStatement(Expected, writer);
            change.Expected.WriteAddStatement(Expected, writer);
        }
    }

    public override void WriteRollback(Migrator rules, TextWriter writer)
    {
        if (Actual == null)
        {
            Expected.WriteDropStatement(rules, writer);
            return;
        }

        foreach (var foreignKey in ForeignKeys.Missing) foreignKey.WriteDropStatement(Expected, writer);

        foreach (var change in ForeignKeys.Different) change.Expected.WriteDropStatement(Expected, writer);

        // Extra columns
        foreach (var column in Columns.Extras) writer.WriteLine(column.AddColumnSql(Expected));

        // Different columns
        foreach (var change1 in Columns.Different)
            writer.WriteLine(change1.Actual.AlterColumnTypeSql(Actual, change1.Expected));

        foreach (var change in ForeignKeys.Different) change.Actual.WriteAddStatement(Expected, writer);

        rollbackIndexes(writer);

        // Missing columns
        foreach (var column in Columns.Missing) writer.WriteLine(column.DropColumnSql(Expected));

        foreach (var foreignKey in ForeignKeys.Extras) foreignKey.WriteAddStatement(Expected, writer);

        switch (PrimaryKeyDifference)
        {
            case SchemaPatchDifference.Invalid:
            case SchemaPatchDifference.Update:
                writer.WriteLine(
                    $"alter table {Expected.Identifier} drop constraint if exists {Expected.PrimaryKeyName};");
                writer.WriteLine($"alter table {Expected.Identifier} add {Actual.PrimaryKeyDeclaration()};");
                break;

            case SchemaPatchDifference.Create:
                writer.WriteLine(
                    $"alter table {Expected.Identifier} drop constraint if exists {Expected.PrimaryKeyName};");
                break;
        }
    }

    private void rollbackIndexes(TextWriter writer)
    {
        // Missing indexes
        foreach (var indexDefinition in Indexes.Missing) writer.WriteDropIndex(Expected, indexDefinition);

        // Extra indexes
        foreach (var extra in Indexes.Extras) writer.WriteLine(extra.ToDDL(Actual!));

        // Different indexes
        foreach (var change in Indexes.Different)
        {
            writer.WriteDropIndex(Actual!, change.Expected);
            writer.WriteLine(change.Actual.ToDDL(Actual!));
        }
    }

    private SchemaPatchDifference determinePatchDifference()
    {
        if (!HasChanges())
        {
            return SchemaPatchDifference.None;
        }

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

        var differences = new[]
        {
            Columns.Difference(), ForeignKeys.Difference(), Indexes.Difference(), PrimaryKeyDifference, partitionDifference()
        };

        return differences.Min();
    }

    private SchemaPatchDifference partitionDifference()
    {
        switch (this.PartitionDelta)
        {
            case PartitionDelta.None:
                return SchemaPatchDifference.None;
            case PartitionDelta.Additive:
                return SchemaPatchDifference.Update;
            case PartitionDelta.Rebuild:
                return SchemaPatchDifference.Update;
        }

        return SchemaPatchDifference.None;
    }

    public bool HasChanges()
    {
        return Columns.HasChanges() || Indexes.HasChanges() || ForeignKeys.HasChanges() ||
               PrimaryKeyDifference != SchemaPatchDifference.None || PartitionDelta != PartitionDelta.None;
    }

    public override string ToString()
    {
        return $"TableDelta for {Expected.Identifier}";
    }
}
