using JasperFx.Core;

namespace Weasel.SqlServer.Tables.Partitioning;

/// <summary>
///     SQL Server RANGE partitioning via partition function + partition scheme.
///     Supports RANGE LEFT (default) and RANGE RIGHT boundary semantics.
/// </summary>
public class RangePartitioning : ISqlServerPartitioning
{
    private readonly List<string> _boundaryValues = new();

    public RangePartitioning(string column, string sqlDataType)
    {
        Column = column;
        SqlDataType = sqlDataType;
    }

    public string Column { get; }
    public string SqlDataType { get; }

    /// <summary>
    ///     RANGE LEFT means the boundary value is the upper bound of a partition.
    ///     RANGE RIGHT means the boundary value is the lower bound.
    ///     Default is RANGE RIGHT (more natural for date ranges).
    /// </summary>
    public bool IsRangeRight { get; set; } = true;

    /// <summary>
    ///     The filegroup for all partitions. Defaults to PRIMARY.
    /// </summary>
    public string Filegroup { get; set; } = "PRIMARY";

    public IReadOnlyList<string> BoundaryValues => _boundaryValues;

    /// <summary>
    ///     Add a boundary value as a SQL literal (e.g., "'2024-01-01'", "1", "'true'").
    /// </summary>
    public RangePartitioning AddBoundary(string sqlValue)
    {
        _boundaryValues.Add(sqlValue);
        return this;
    }

    /// <summary>
    ///     Add a typed boundary value. Converts to SQL literal.
    /// </summary>
    public RangePartitioning AddBoundary<T>(T value)
    {
        _boundaryValues.Add(FormatSqlValue(value));
        return this;
    }

    public string PartitionFunctionName(Table parent)
        => $"pf_{parent.Identifier.Name}_{Column}";

    public string PartitionSchemeName(Table parent)
        => $"ps_{parent.Identifier.Name}_{Column}";

    public void WritePartitionDdl(TextWriter writer, Table parent)
    {
        var pfName = PartitionFunctionName(parent);
        var psName = PartitionSchemeName(parent);
        var rangeDir = IsRangeRight ? "RIGHT" : "LEFT";

        // Drop existing (for idempotent creation)
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{psName}')");
        writer.WriteLine($"    DROP PARTITION SCHEME [{psName}];");
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{pfName}')");
        writer.WriteLine($"    DROP PARTITION FUNCTION [{pfName}];");

        // Create partition function
        writer.Write($"CREATE PARTITION FUNCTION [{pfName}] ({SqlDataType})");
        writer.Write($" AS RANGE {rangeDir}");
        if (_boundaryValues.Any())
        {
            writer.Write($" FOR VALUES ({_boundaryValues.Join(", ")})");
        }

        writer.WriteLine(";");

        // Create partition scheme
        writer.WriteLine($"CREATE PARTITION SCHEME [{psName}] AS PARTITION [{pfName}] ALL TO ([{Filegroup}]);");
    }

    public void WriteOnClause(TextWriter writer, Table parent)
    {
        writer.Write($" ON [{PartitionSchemeName(parent)}]([{Column}])");
    }

    public void WriteDropDdl(TextWriter writer, Table parent)
    {
        var psName = PartitionSchemeName(parent);
        var pfName = PartitionFunctionName(parent);

        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{psName}')");
        writer.WriteLine($"    DROP PARTITION SCHEME [{psName}];");
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{pfName}')");
        writer.WriteLine($"    DROP PARTITION FUNCTION [{pfName}];");
    }

    public PartitionDelta CreateDelta(SqlServerPartitionInfo? actual)
    {
        if (actual == null) return PartitionDelta.Rebuild;

        // If the column or data type changed, need rebuild
        if (!actual.Column.EqualsIgnoreCase(Column)) return PartitionDelta.Rebuild;
        if (!actual.SqlDataType.EqualsIgnoreCase(SqlDataType)) return PartitionDelta.Rebuild;
        if (actual.IsRangeRight != IsRangeRight) return PartitionDelta.Rebuild;

        // Check boundary values
        var expectedSet = new HashSet<string>(_boundaryValues, StringComparer.OrdinalIgnoreCase);
        var actualSet = new HashSet<string>(actual.BoundaryValues, StringComparer.OrdinalIgnoreCase);

        if (actualSet.SetEquals(expectedSet)) return PartitionDelta.None;

        // If we only have NEW boundaries (additions), it's additive
        if (expectedSet.IsSupersetOf(actualSet)) return PartitionDelta.Additive;

        // Boundaries were removed or changed — need rebuild
        return PartitionDelta.Rebuild;
    }

    /// <summary>
    ///     Write ALTER PARTITION FUNCTION ... SPLIT RANGE statements for missing boundaries.
    /// </summary>
    public void WriteSplitStatements(TextWriter writer, Table parent, SqlServerPartitionInfo actual)
    {
        var pfName = PartitionFunctionName(parent);
        var psName = PartitionSchemeName(parent);
        var actualSet = new HashSet<string>(actual.BoundaryValues, StringComparer.OrdinalIgnoreCase);

        foreach (var boundary in _boundaryValues)
        {
            if (!actualSet.Contains(boundary))
            {
                writer.WriteLine($"ALTER PARTITION SCHEME [{psName}] NEXT USED [{Filegroup}];");
                writer.WriteLine($"ALTER PARTITION FUNCTION [{pfName}]() SPLIT RANGE ({boundary});");
            }
        }
    }

    internal static string FormatSqlValue<T>(T? value)
    {
        if (value == null) return "NULL";

        return value switch
        {
            bool b => b ? "1" : "0",
            int or long or short or byte or decimal or float or double => value.ToString()!,
            DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
            DateTimeOffset dto => $"'{dto:yyyy-MM-dd HH:mm:ss.fffffff zzz}'",
            _ => $"'{value}'"
        };
    }
}
