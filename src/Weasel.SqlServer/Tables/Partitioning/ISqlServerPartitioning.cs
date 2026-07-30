namespace Weasel.SqlServer.Tables.Partitioning;

/// <summary>
///     SQL Server partitioning strategy. Unlike PostgreSQL (which uses child tables),
///     SQL Server partitioning uses partition functions and partition schemes.
///     A partition function defines boundary values; a partition scheme maps
///     the function's partitions to filegroups; and the table is created ON the scheme.
/// </summary>
public interface ISqlServerPartitioning
{
    /// <summary>
    ///     The column used for partitioning.
    /// </summary>
    string Column { get; }

    /// <summary>
    ///     The SQL Server data type for the partition function parameter.
    /// </summary>
    string SqlDataType { get; }

    /// <summary>
    ///     Name of the partition function (e.g., "pf_tablename_column").
    /// </summary>
    string PartitionFunctionName(Table parent);

    /// <summary>
    ///     Name of the partition scheme (e.g., "ps_tablename_column").
    /// </summary>
    string PartitionSchemeName(Table parent);

    /// <summary>
    ///     Write the DDL to create the partition function and partition scheme.
    ///     These must be created BEFORE the table.
    /// </summary>
    void WritePartitionDdl(TextWriter writer, Table parent);

    /// <summary>
    ///     Write the ON clause for the CREATE TABLE statement.
    ///     E.g., "ON ps_name(column)"
    /// </summary>
    void WriteOnClause(TextWriter writer, Table parent);

    /// <summary>
    ///     Write DDL to drop the partition function and scheme (for rebuild scenarios).
    /// </summary>
    void WriteDropDdl(TextWriter writer, Table parent);

    /// <summary>
    ///     Detect changes between expected and actual partitioning.
    /// </summary>
    PartitionDelta CreateDelta(SqlServerPartitionInfo? actual);
}

/// <summary>
///     A partitioning strategy whose boundary changes can be migrated in place with
///     <c>ALTER PARTITION FUNCTION ... SPLIT RANGE</c> rather than requiring the partition function and
///     scheme to be rebuilt. <see cref="TableDelta" /> only migrates boundaries for strategies that
///     implement this; a strategy that owns its boundaries purely at runtime (e.g.
///     <see cref="ManagedTenantPartitions" />) deliberately does not, and is left alone by migration.
/// </summary>
public interface ISplittablePartitioning: ISqlServerPartitioning
{
    /// <summary>
    ///     Write <c>ALTER PARTITION SCHEME ... NEXT USED</c> + <c>ALTER PARTITION FUNCTION ... SPLIT
    ///     RANGE</c> for every expected boundary missing from <paramref name="actual" />.
    /// </summary>
    void WriteSplitStatements(TextWriter writer, Table parent, SqlServerPartitionInfo actual);

    /// <summary>
    ///     Write the <c>MERGE RANGE</c> statements that undo <see cref="WriteSplitStatements" />, so an
    ///     additive partition migration can be rolled back.
    /// </summary>
    void WriteMergeStatements(TextWriter writer, Table parent, SqlServerPartitionInfo actual);
}

public enum PartitionDelta
{
    /// <summary>No changes needed.</summary>
    None,

    /// <summary>New boundary values can be added via ALTER PARTITION FUNCTION ... SPLIT RANGE.</summary>
    Additive,

    /// <summary>Partitioning must be completely rebuilt (column changed, boundaries removed, etc.).</summary>
    Rebuild
}

/// <summary>
///     Represents the actual partitioning state read from the database.
/// </summary>
public class SqlServerPartitionInfo
{
    public string? PartitionFunctionName { get; set; }
    public string? PartitionSchemeName { get; set; }
    public string? Column { get; set; }
    public string? SqlDataType { get; set; }
    public bool IsRangeRight { get; set; }
    public List<string> BoundaryValues { get; set; } = new();
}
