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
        Name = name.Trim().Replace(' ', '_');
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
                $"where dc.parent_object_id = OBJECT_ID('{parent.Identifier}') and c.name = '{Name}';");
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

    public string DropColumnSql(Table table)
    {
        return $"alter table {table.Identifier} drop column {QuotedName};";
    }


    public virtual bool CanAdd()
    {
        return AllowNulls || DefaultExpression.IsNotEmpty();
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

        return $"CONSTRAINT {Name} {Declaration()}";
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
