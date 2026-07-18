using System.Text.RegularExpressions;

namespace Weasel.EntityFrameworkCore.Tests.SchemaComparison;

public enum DifferenceCategory
{
    MissingTable,
    ExtraTable,
    MissingColumn,
    ExtraColumn,
    /// <summary>
    ///     Same identifier but different case. Fatal on PostgreSQL where EF
    ///     emits quoted, case-sensitive identifiers; harmless on SQL Server.
    /// </summary>
    IdentifierCasing,
    ColumnType,
    ColumnNullability,
    ColumnDefault,
    ColumnIdentity,
    ColumnComputed,
    PrimaryKeyName,
    PrimaryKeyColumns,
    MissingForeignKey,
    ExtraForeignKey,
    ForeignKeyTarget,
    ForeignKeyDeleteAction,
    MissingIndex,
    ExtraIndex,
    IndexColumns,
    IndexUniqueness,
    IndexPredicate,
    IndexMethod,
    IndexIncludedColumns,
    IndexSortOrder,
    /// <summary>Same unique guarantee but one side used a UNIQUE constraint and the other a unique index</summary>
    UniqueConstraintVsUniqueIndex,
    MissingCheckConstraint,
    ExtraCheckConstraint,
    CheckConstraintExpression,
    MissingSequence,
    ExtraSequence,
    SequenceDefinition
}

public record SchemaDifference(DifferenceCategory Category, string Table, string Detail)
{
    public override string ToString() => $"[{Category}] {Table}: {Detail}";
}

/// <summary>
///     Compares the schema EF Core created ("expected") against the schema
///     Weasel created for the same model ("actual"). Both sides come from the
///     same catalog introspection, so the database has already canonicalized
///     expression text on each side — remaining differences are real.
/// </summary>
public static class SchemaComparer
{
    public static IReadOnlyList<SchemaDifference> Compare(SchemaSnapshot efSchema, SchemaSnapshot weaselSchema)
    {
        var differences = new List<SchemaDifference>();

        foreach (var efTable in efSchema.Tables)
        {
            var weaselTable = weaselSchema.TableFor(efTable.Name);
            if (weaselTable == null)
            {
                differences.Add(new(DifferenceCategory.MissingTable, efTable.Name,
                    "table created by EF Core is missing from the Weasel-created schema"));
                continue;
            }

            compareTables(efTable, weaselTable, differences);
        }

        foreach (var weaselTable in weaselSchema.Tables)
        {
            if (efSchema.TableFor(weaselTable.Name) == null)
            {
                differences.Add(new(DifferenceCategory.ExtraTable, weaselTable.Name,
                    "table created by Weasel does not exist in the EF Core-created schema"));
            }
        }

        compareSequences(efSchema, weaselSchema, differences);

        return differences;
    }

    private static void compareSequences(SchemaSnapshot ef, SchemaSnapshot weasel, List<SchemaDifference> differences)
    {
        foreach (var efSequence in ef.Sequences)
        {
            var weaselSequence = weasel.SequenceFor(efSequence.Name);
            if (weaselSequence == null)
            {
                differences.Add(new(DifferenceCategory.MissingSequence, efSequence.Name,
                    $"sequence (start {efSequence.StartValue}, increment {efSequence.IncrementBy}) missing from Weasel-created schema"));
            }
            else if (efSequence.StartValue != weaselSequence.StartValue
                     || efSequence.IncrementBy != weaselSequence.IncrementBy)
            {
                differences.Add(new(DifferenceCategory.SequenceDefinition, efSequence.Name,
                    $"EF start {efSequence.StartValue} / increment {efSequence.IncrementBy}, Weasel start {weaselSequence.StartValue} / increment {weaselSequence.IncrementBy}"));
            }
        }

        foreach (var weaselSequence in weasel.Sequences)
        {
            if (ef.SequenceFor(weaselSequence.Name) == null)
            {
                differences.Add(new(DifferenceCategory.ExtraSequence, weaselSequence.Name,
                    "sequence created by Weasel does not exist in the EF Core-created schema"));
            }
        }
    }

    private static void compareTables(TableSnapshot ef, TableSnapshot weasel, List<SchemaDifference> differences)
    {
        compareColumns(ef, weasel, differences);
        comparePrimaryKey(ef, weasel, differences);
        compareForeignKeys(ef, weasel, differences);
        compareIndexes(ef, weasel, differences);
        compareCheckConstraints(ef, weasel, differences);
    }

    private static void compareColumns(TableSnapshot ef, TableSnapshot weasel, List<SchemaDifference> differences)
    {
        foreach (var efColumn in ef.Columns)
        {
            var weaselColumn = weasel.ColumnFor(efColumn.Name);
            if (weaselColumn == null)
            {
                differences.Add(new(DifferenceCategory.MissingColumn, ef.Name,
                    $"column '{efColumn.Name}' ({efColumn.DataType}) missing from Weasel-created table"));
                continue;
            }

            if (!efColumn.Name.Equals(weaselColumn.Name, StringComparison.Ordinal))
            {
                differences.Add(new(DifferenceCategory.IdentifierCasing, ef.Name,
                    $"column casing: EF created '{efColumn.Name}', Weasel created '{weaselColumn.Name}' — quoted EF SQL will not find the Weasel column on PostgreSQL"));
            }

            if (!NormalizeType(efColumn.DataType).Equals(NormalizeType(weaselColumn.DataType), StringComparison.OrdinalIgnoreCase))
            {
                differences.Add(new(DifferenceCategory.ColumnType, ef.Name,
                    $"column '{efColumn.Name}': EF created '{efColumn.DataType}', Weasel created '{weaselColumn.DataType}'"));
            }

            if (efColumn.IsNullable != weaselColumn.IsNullable)
            {
                differences.Add(new(DifferenceCategory.ColumnNullability, ef.Name,
                    $"column '{efColumn.Name}': EF created {(efColumn.IsNullable ? "NULL" : "NOT NULL")}, Weasel created {(weaselColumn.IsNullable ? "NULL" : "NOT NULL")}"));
            }

            // Serial-style nextval defaults are the PG implementation detail of identity-ish
            // generation; compare generation strategy first, then remaining plain defaults.
            var efGeneration = efColumn.IsIdentity || efColumn.IsSerialStyle;
            var weaselGeneration = weaselColumn.IsIdentity || weaselColumn.IsSerialStyle;
            if (efGeneration != weaselGeneration)
            {
                differences.Add(new(DifferenceCategory.ColumnIdentity, ef.Name,
                    $"column '{efColumn.Name}': EF {describeGeneration(efColumn)}, Weasel {describeGeneration(weaselColumn)}"));
            }
            else if (!efGeneration)
            {
                var efDefault = NormalizeExpression(efColumn.DefaultExpression);
                var weaselDefault = NormalizeExpression(weaselColumn.DefaultExpression);
                if (!string.Equals(efDefault, weaselDefault, StringComparison.OrdinalIgnoreCase))
                {
                    differences.Add(new(DifferenceCategory.ColumnDefault, ef.Name,
                        $"column '{efColumn.Name}': EF default '{efColumn.DefaultExpression ?? "<none>"}', Weasel default '{weaselColumn.DefaultExpression ?? "<none>"}'"));
                }
            }

            if (efColumn.IsComputed != weaselColumn.IsComputed)
            {
                differences.Add(new(DifferenceCategory.ColumnComputed, ef.Name,
                    $"column '{efColumn.Name}': EF computed={efColumn.IsComputed}, Weasel computed={weaselColumn.IsComputed}"));
            }
        }

        foreach (var weaselColumn in weasel.Columns)
        {
            if (ef.ColumnFor(weaselColumn.Name) == null)
            {
                differences.Add(new(DifferenceCategory.ExtraColumn, ef.Name,
                    $"column '{weaselColumn.Name}' ({weaselColumn.DataType}) created by Weasel does not exist in EF-created table"));
            }
        }
    }

    private static string describeGeneration(ColumnSnapshot column)
    {
        if (column.IsIdentity) return "identity";
        if (column.IsSerialStyle) return "serial (nextval default)";
        return "no generation";
    }

    private static void comparePrimaryKey(TableSnapshot ef, TableSnapshot weasel, List<SchemaDifference> differences)
    {
        if (!string.Equals(ef.PrimaryKeyName, weasel.PrimaryKeyName, StringComparison.OrdinalIgnoreCase))
        {
            differences.Add(new(DifferenceCategory.PrimaryKeyName, ef.Name,
                $"EF PK name '{ef.PrimaryKeyName ?? "<none>"}', Weasel PK name '{weasel.PrimaryKeyName ?? "<none>"}'"));
        }

        if (!ef.PrimaryKeyColumns.SequenceEqual(weasel.PrimaryKeyColumns, StringComparer.OrdinalIgnoreCase))
        {
            differences.Add(new(DifferenceCategory.PrimaryKeyColumns, ef.Name,
                $"EF PK columns ({string.Join(", ", ef.PrimaryKeyColumns)}), Weasel PK columns ({string.Join(", ", weasel.PrimaryKeyColumns)})"));
        }
    }

    private static void compareForeignKeys(TableSnapshot ef, TableSnapshot weasel, List<SchemaDifference> differences)
    {
        foreach (var efFk in ef.ForeignKeys)
        {
            var weaselFk = weasel.ForeignKeys.FirstOrDefault(f =>
                f.Name.Equals(efFk.Name, StringComparison.OrdinalIgnoreCase));
            if (weaselFk == null)
            {
                differences.Add(new(DifferenceCategory.MissingForeignKey, ef.Name,
                    $"FK '{efFk.Name}' ({string.Join(", ", efFk.Columns)}) missing from Weasel-created table"));
                continue;
            }

            if (!efFk.Columns.SequenceEqual(weaselFk.Columns, StringComparer.OrdinalIgnoreCase)
                || !efFk.PrincipalTable.Equals(weaselFk.PrincipalTable, StringComparison.OrdinalIgnoreCase)
                || !efFk.PrincipalColumns.SequenceEqual(weaselFk.PrincipalColumns, StringComparer.OrdinalIgnoreCase))
            {
                differences.Add(new(DifferenceCategory.ForeignKeyTarget, ef.Name,
                    $"FK '{efFk.Name}': EF ({string.Join(", ", efFk.Columns)}) -> {efFk.PrincipalTable}({string.Join(", ", efFk.PrincipalColumns)}), " +
                    $"Weasel ({string.Join(", ", weaselFk.Columns)}) -> {weaselFk.PrincipalTable}({string.Join(", ", weaselFk.PrincipalColumns)})"));
            }

            if (efFk.OnDelete != weaselFk.OnDelete)
            {
                differences.Add(new(DifferenceCategory.ForeignKeyDeleteAction, ef.Name,
                    $"FK '{efFk.Name}': EF ON DELETE {efFk.OnDelete}, Weasel ON DELETE {weaselFk.OnDelete}"));
            }
        }

        foreach (var weaselFk in weasel.ForeignKeys)
        {
            if (!ef.ForeignKeys.Any(f => f.Name.Equals(weaselFk.Name, StringComparison.OrdinalIgnoreCase)))
            {
                differences.Add(new(DifferenceCategory.ExtraForeignKey, ef.Name,
                    $"FK '{weaselFk.Name}' created by Weasel does not exist in EF-created table"));
            }
        }
    }

    private static void compareIndexes(TableSnapshot ef, TableSnapshot weasel, List<SchemaDifference> differences)
    {
        // The primary key's backing index is covered by the PK comparison
        var efIndexes = ef.Indexes.Where(i => !i.IsPrimaryKey).ToList();
        var weaselIndexes = weasel.Indexes.Where(i => !i.IsPrimaryKey).ToList();

        foreach (var efIndex in efIndexes)
        {
            var weaselIndex = weaselIndexes.FirstOrDefault(i =>
                i.Name.Equals(efIndex.Name, StringComparison.OrdinalIgnoreCase));
            if (weaselIndex == null)
            {
                differences.Add(new(DifferenceCategory.MissingIndex, ef.Name,
                    $"index '{efIndex.Name}' ({string.Join(", ", efIndex.KeyColumns)}){(efIndex.IsUnique ? " UNIQUE" : "")} missing from Weasel-created table"));
                continue;
            }

            if (!efIndex.KeyColumns.SequenceEqual(weaselIndex.KeyColumns, StringComparer.OrdinalIgnoreCase))
            {
                differences.Add(new(DifferenceCategory.IndexColumns, ef.Name,
                    $"index '{efIndex.Name}': EF columns ({string.Join(", ", efIndex.KeyColumns)}), Weasel columns ({string.Join(", ", weaselIndex.KeyColumns)})"));
            }

            if (efIndex.IsUnique != weaselIndex.IsUnique)
            {
                differences.Add(new(DifferenceCategory.IndexUniqueness, ef.Name,
                    $"index '{efIndex.Name}': EF unique={efIndex.IsUnique}, Weasel unique={weaselIndex.IsUnique}"));
            }
            else if (efIndex.IsConstraintBacked != weaselIndex.IsConstraintBacked)
            {
                differences.Add(new(DifferenceCategory.UniqueConstraintVsUniqueIndex, ef.Name,
                    $"index '{efIndex.Name}': EF constraint-backed={efIndex.IsConstraintBacked}, Weasel constraint-backed={weaselIndex.IsConstraintBacked}"));
            }

            var efPredicate = NormalizeExpression(efIndex.Predicate);
            var weaselPredicate = NormalizeExpression(weaselIndex.Predicate);
            if (!string.Equals(efPredicate, weaselPredicate, StringComparison.OrdinalIgnoreCase))
            {
                differences.Add(new(DifferenceCategory.IndexPredicate, ef.Name,
                    $"index '{efIndex.Name}': EF predicate '{efIndex.Predicate ?? "<none>"}', Weasel predicate '{weaselIndex.Predicate ?? "<none>"}'"));
            }

            if (!string.Equals(efIndex.Method, weaselIndex.Method, StringComparison.OrdinalIgnoreCase))
            {
                differences.Add(new(DifferenceCategory.IndexMethod, ef.Name,
                    $"index '{efIndex.Name}': EF method '{efIndex.Method}', Weasel method '{weaselIndex.Method}'"));
            }

            if (!efIndex.IncludedColumns.SequenceEqual(weaselIndex.IncludedColumns, StringComparer.OrdinalIgnoreCase))
            {
                differences.Add(new(DifferenceCategory.IndexIncludedColumns, ef.Name,
                    $"index '{efIndex.Name}': EF includes ({string.Join(", ", efIndex.IncludedColumns)}), Weasel includes ({string.Join(", ", weaselIndex.IncludedColumns)})"));
            }

            if (!efIndex.IsDescending.SequenceEqual(weaselIndex.IsDescending))
            {
                differences.Add(new(DifferenceCategory.IndexSortOrder, ef.Name,
                    $"index '{efIndex.Name}': EF descending ({string.Join(", ", efIndex.IsDescending)}), Weasel descending ({string.Join(", ", weaselIndex.IsDescending)})"));
            }
        }

        foreach (var weaselIndex in weaselIndexes)
        {
            if (!efIndexes.Any(i => i.Name.Equals(weaselIndex.Name, StringComparison.OrdinalIgnoreCase)))
            {
                differences.Add(new(DifferenceCategory.ExtraIndex, ef.Name,
                    $"index '{weaselIndex.Name}' created by Weasel does not exist in EF-created table"));
            }
        }
    }

    private static void compareCheckConstraints(TableSnapshot ef, TableSnapshot weasel, List<SchemaDifference> differences)
    {
        foreach (var efCheck in ef.CheckConstraints)
        {
            var weaselCheck = weasel.CheckConstraints.FirstOrDefault(c =>
                c.Name.Equals(efCheck.Name, StringComparison.OrdinalIgnoreCase));
            if (weaselCheck == null)
            {
                differences.Add(new(DifferenceCategory.MissingCheckConstraint, ef.Name,
                    $"check constraint '{efCheck.Name}' {efCheck.Expression} missing from Weasel-created table"));
            }
            else if (!string.Equals(NormalizeExpression(efCheck.Expression), NormalizeExpression(weaselCheck.Expression),
                         StringComparison.OrdinalIgnoreCase))
            {
                differences.Add(new(DifferenceCategory.CheckConstraintExpression, ef.Name,
                    $"check constraint '{efCheck.Name}': EF {efCheck.Expression}, Weasel {weaselCheck.Expression}"));
            }
        }

        foreach (var weaselCheck in weasel.CheckConstraints)
        {
            if (!ef.CheckConstraints.Any(c => c.Name.Equals(weaselCheck.Name, StringComparison.OrdinalIgnoreCase)))
            {
                differences.Add(new(DifferenceCategory.ExtraCheckConstraint, ef.Name,
                    $"check constraint '{weaselCheck.Name}' created by Weasel does not exist in EF-created table"));
            }
        }
    }

    /// <summary>
    ///     Normalize a store type for comparison, absorbing pure synonyms
    ///     (timestamptz vs timestamp with time zone etc.).
    /// </summary>
    public static string NormalizeType(string type)
    {
        var normalized = type.Trim().ToLowerInvariant();
        return normalized switch
        {
            "timestamptz" => "timestamp with time zone",
            "timestamp" => "timestamp without time zone",
            "timetz" => "time with time zone",
            "int8" => "bigint",
            "int4" => "integer",
            "int2" => "smallint",
            "int" => "integer",
            "bool" => "boolean",
            "float8" => "double precision",
            "float4" => "real",
            "character varying" => "varchar",
            "decimal" => "numeric",
            _ => normalized
        };
    }

    /// <summary>
    ///     Normalize a default / predicate / check expression for comparison.
    ///     Both sides come from the same catalog so text is mostly canonical
    ///     already; strip cosmetic parens, casts and whitespace differences.
    /// </summary>
    public static string? NormalizeExpression(string? expression)
    {
        if (expression == null) return null;

        var normalized = expression.Trim();

        // Strip ::type casts (including quoted and multi-word types, e.g. ::character varying(200))
        normalized = Regex.Replace(normalized, "::\"?[a-zA-Z_][a-zA-Z_ 0-9]*\"?(\\([0-9, ]*\\))?(\\[\\])?", "");

        // Collapse whitespace
        normalized = Regex.Replace(normalized, @"\s+", " ");

        // Strip redundant outer parens repeatedly: ((expr)) -> expr
        while (normalized.Length > 1 && normalized[0] == '(' && normalized[^1] == ')' && parensAreBalancedOuter(normalized))
        {
            normalized = normalized[1..^1].Trim();
        }

        // Canonical spellings
        normalized = normalized
            .Replace("CURRENT_TIMESTAMP", "now()", StringComparison.OrdinalIgnoreCase)
            .Replace("''", "'"); // SQL Server doubles quotes inside catalog text

        return normalized.ToLowerInvariant();
    }

    private static bool parensAreBalancedOuter(string text)
    {
        var depth = 0;
        for (var i = 0; i < text.Length; i++)
        {
            if (text[i] == '(') depth++;
            else if (text[i] == ')')
            {
                depth--;
                if (depth == 0 && i < text.Length - 1) return false;
            }
        }

        return depth == 0;
    }
}
