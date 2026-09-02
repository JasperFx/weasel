using JasperFx.Core;
using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.SqlServer.Tables;

public class TableColumn: ITableColumn
{
    public TableColumn(string name, string type)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentOutOfRangeException(nameof(name));
        }

        if (string.IsNullOrEmpty(type))
        {
            throw new ArgumentOutOfRangeException(nameof(type));
        }

        // Preserve the user's casing — SQL Server identifiers are case-insensitive
        // by default but legacy schemas often use PascalCase. Lowercasing here
        // produced duplicate-column DDL when callers added the same logical column
        // with different casings (issue: JasperFx/polecat#45).
        // Unbracketed first: a caller who wrote "[Order Date]" means the column Order Date,
        // and the database will report it back that way -- so that is the column that gets
        // created, rather than Order_Date (weasel#458).
        Name = SchemaUtils.Unbracket(name.Trim());
        Type = type.ToLowerInvariant();
    }


    public IList<ColumnCheck> ColumnChecks { get; } = new List<ColumnCheck>();

    public bool AllowNulls { get; set; } = true;

    public string? DefaultExpression { get; set; }


    public string Type { get; set; }
    public Table Parent { get; internal set; } = null!;

    public bool IsPrimaryKey { get; internal set; }
    public bool IsAutoNumber { get; set; }

    /// <summary>
    ///     Computed column expression: emitted as [name] AS (expr) [PERSISTED],
    ///     replacing the data type in the column declaration (SQL Server derives
    ///     the type from the expression).
    /// </summary>
    public string? ComputedExpression { get; set; }

    public bool ComputedColumnIsStored { get; set; }

    public string Name { get; }
    public string QuotedName => SchemaUtils.QuoteName(Name);

    public string RawType()
    {
        return Type.Split('(')[0].Trim();
    }

    public string Declaration()
    {
        var declaration = !IsPrimaryKey && AllowNulls ? "NULL" : "NOT NULL";
        if (IsAutoNumber)
        {
            declaration += " IDENTITY";
        }

        if (DefaultExpression.IsNotEmpty())
        {
            declaration += " DEFAULT " + DefaultExpression;
        }

        return $"{declaration} {ColumnChecks.Select(x => x.FullDeclaration()).Join(" ")}".TrimEnd();
    }

    /// <summary>
    ///     Drift comparison for <see cref="Core.ITable.DetectColumnDrift" />:
    ///     nullability (primary key columns excluded — they are implicitly NOT
    ///     NULL) and canonicalized default expressions.
    /// </summary>
    internal bool HasSameDefaultAndNullability(TableColumn actual)
    {
        if (!IsPrimaryKey && !actual.IsPrimaryKey && AllowNulls != actual.AllowNulls)
        {
            return false;
        }

        return canonicalDefault(DefaultExpression) == canonicalDefault(actual.DefaultExpression);
    }

    private static string? canonicalDefault(string? expression)
        => expression == null ? null : TableCheckConstraint.Canonicalize(expression);

    /// <summary>
    ///     Column matching for delta detection. Computed columns are compared by
    ///     their canonicalized expression and PERSISTED flag; the declared model
    ///     type is skipped because SQL Server derives the type from the
    ///     expression and the type is never emitted in the DDL. Only columns
    ///     whose expected model declares a computed expression participate, so
    ///     an actual computed column the model doesn't declare is left alone
    ///     (mirroring the conservative check-constraint comparison).
    /// </summary>
    internal bool MatchesForDelta(TableColumn actual, bool detectDrift)
    {
        if (ComputedExpression.IsNotEmpty())
        {
            return HasSameComputedDefinition(actual);
        }

        if (actual.ComputedExpression.IsNotEmpty())
        {
            // the actual column is computed but the model doesn't declare it —
            // leave it alone (mirrors the unknown-check-constraint handling)
            return true;
        }

        return equalsVirtual(actual) && (!detectDrift || HasSameDefaultAndNullability(actual));
    }

    // weasel#399: compare through the VIRTUAL Equals(object) so a subclass override
    // participates. A bare Equals(actual) binds to the protected, non-virtual
    // Equals(TableColumn) overload and silently bypasses it. See the PostgreSQL twin.
    private bool equalsVirtual(TableColumn actual) => Equals((object)actual);

    internal bool HasSameComputedDefinition(TableColumn actual)
    {
        return actual.ComputedExpression.IsNotEmpty() &&
               TableCheckConstraint.Canonicalize(ComputedExpression!) ==
               TableCheckConstraint.Canonicalize(actual.ComputedExpression!) &&
               ComputedColumnIsStored == actual.ComputedColumnIsStored;
    }

    internal bool ComputedDefinitionChanged(TableColumn actual)
        => ComputedExpression.IsNotEmpty() && !HasSameComputedDefinition(actual);

    internal void WriteDriftCorrections(Table parent, TableColumn actual, TextWriter writer)
    {
        if (!IsPrimaryKey && !actual.IsPrimaryKey && AllowNulls != actual.AllowNulls)
        {
            writer.WriteLine(
                $"alter table {parent.Identifier} alter column {QuotedName} {Type} {(AllowNulls ? "NULL" : "NOT NULL")};");
        }

        if (canonicalDefault(DefaultExpression) != canonicalDefault(actual.DefaultExpression))
        {
            // SQL Server default constraints have (often server-generated) names;
            // drop whatever default currently exists before adding the new one
            var variable = $"@dc_{Guid.NewGuid().ToString("N")[..8]}";
            writer.WriteLine($"declare {variable} nvarchar(max);");
            writer.WriteLine(
                $"select {variable} = dc.name from sys.default_constraints dc " +
                $"inner join sys.columns c on c.default_object_id = dc.object_id " +
                $"where dc.parent_object_id = OBJECT_ID('{SchemaUtils.EscapeLiteral(parent.Identifier.QualifiedName)}') " +
                $"and c.name = '{SchemaUtils.EscapeLiteral(Name)}';");
            writer.WriteLine(
                $"if {variable} is not null exec('alter table {parent.Identifier} drop constraint ' + {variable});");

            if (DefaultExpression.IsNotEmpty())
            {
                writer.WriteLine($"alter table {parent.Identifier} add default {DefaultExpression} for {QuotedName};");
            }
        }
    }

    protected bool Equals(TableColumn other)
    {
        // RawType() throws the parenthesised part away, which is right for a decimal precision or a
        // datetime2 scale and wrong for a character length the model declared. See CharacterColumnLength:
        // a widened varchar used to be invisible here, so an existing table kept the narrow column forever.
        if (CharacterColumnLength.Differ(Type, other.Type))
        {
            return false;
        }

        return string.Equals(QuotedName, other.QuotedName, StringComparison.OrdinalIgnoreCase) &&
               string.Equals(SqlServerProvider.Instance.ConvertSynonyms(RawType()),
                   SqlServerProvider.Instance.ConvertSynonyms(other.RawType()));
    }

    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj))
        {
            return false;
        }

        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (!obj.GetType().CanBeCastTo<TableColumn>())
        {
            return false;
        }

        return Equals((TableColumn)obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            return (StringComparer.OrdinalIgnoreCase.GetHashCode(Name) * 397) ^ Type.GetHashCode();
        }
    }

    public string ToDeclaration()
    {
        if (ComputedExpression.IsNotEmpty())
        {
            return $"{QuotedName} AS ({ComputedExpression}){(ComputedColumnIsStored ? " PERSISTED" : string.Empty)}";
        }

        var declaration = Declaration();

        return declaration.IsEmpty()
            ? $"{QuotedName} {Type}"
            : $"{QuotedName} {Type} {declaration}";
    }

    public override string ToString()
    {
        return ToDeclaration();
    }


    public virtual string AlterColumnTypeSql(Table table, TableColumn changeActual)
    {
        return $"alter table {table.Identifier} alter column {ToDeclaration()};";
    }

    /// <summary>
    ///     Drop the column, and the default constraint that would otherwise block it.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         SQL Server refuses <c>DROP COLUMN</c> while a default constraint references the column —
    ///         "The object 'DF__orders__stamp__2FEFE172' is dependent on column 'stamp'." So a column
    ///         declared with a default could be added by a migration but never removed by one: the patch
    ///         was generated, it just always failed (weasel#505). PostgreSQL drops the two together,
    ///         which is why this is SQL-Server-only.
    ///     </para>
    ///     <para>
    ///         The constraint cannot be named statically. SQL Server invents one when the DDL does not
    ///         supply it — <c>DF__table__column__hash</c>, where the hash is per-object — so the name has
    ///         to be read out of <c>sys.default_constraints</c> at run time.
    ///     </para>
    ///     <para>
    ///         The lookup is wrapped in its own <c>EXEC</c> so that <c>@constraint</c> is scoped to a
    ///         nested batch. <c>DECLARE</c> in T-SQL is scoped to the batch and not to a <c>BEGIN/END</c>
    ///         block, and <c>SqlServerMigrator.executeDelta</c> sends one command per table — so a table
    ///         dropping two defaulted columns would otherwise fail with "The variable name '@constraint'
    ///         has already been declared."
    ///     </para>
    ///     <para>
    ///         Only the default is handled. A check constraint or an index over the column blocks the
    ///         drop the same way, but Weasel emits a default for any column that declares one, which
    ///         makes it the case that arises on its own.
    ///     </para>
    /// </remarks>
    public string DropColumnSql(Table table)
    {
        // Written as the SQL it should be when it runs, then escaped once to embed in the EXEC.
        var inner = $"""
                     declare @constraint sysname;
                     select @constraint = dc.name
                     from sys.default_constraints dc
                     inner join sys.columns c
                         on c.object_id = dc.parent_object_id and c.column_id = dc.parent_column_id
                     where dc.parent_object_id = object_id('{SchemaUtils.EscapeLiteral(table.Identifier.QualifiedName)}')
                       and c.name = '{SchemaUtils.EscapeLiteral(Name)}';
                     if @constraint is not null
                         exec('alter table {table.Identifier} drop constraint [' + @constraint + ']');
                     alter table {table.Identifier} drop column {QuotedName};
                     """;

        return $"EXEC(N'{inner.Replace("'", "''")}');";
    }


    public virtual bool CanAdd()
    {
        // computed columns are derived, so the database back-fills them on add
        return AllowNulls || DefaultExpression.IsNotEmpty() || ComputedExpression.IsNotEmpty();
    }

    public virtual string AddColumnSql(Table parent)
    {
        return $"alter table {parent.Identifier} add {ToDeclaration()};";
    }


    public virtual bool CanAlter(TableColumn actual)
    {
        // TODO -- need this to be more systematic
        return true;
    }
}

public abstract class ColumnCheck
{
    /// <summary>
    ///     The database name for the check. This can be null
    /// </summary>
    public string? Name { get; set; } // TODO -- validate good name

    public abstract string Declaration();

    public string FullDeclaration()
    {
        if (Name.IsEmpty())
        {
            return Declaration();
        }

        return $"CONSTRAINT {SchemaUtils.QuoteName(Name)} {Declaration()}";
    }
}

public class SerialValue: ColumnCheck
{
    public override string Declaration()
    {
        return "SERIAL";
    }
}

/*

public class GeneratedAlwaysAsStored : ColumnCheck
{
    // GENERATED ALWAYS AS ( generation_expr ) STORED
}

public class GeneratedAsIdentity : ColumnCheck
{
    // GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( sequence_options ) ]
}

*/
