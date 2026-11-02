namespace Weasel.SqlServer.Tables.Partitioning;

/// <summary>
///     The drop-then-create preamble every <see cref="ISqlServerPartitioning" /> implementation writes
///     ahead of a partitioned table, factored out so the three of them cannot drift.
/// </summary>
/// <remarks>
///     <para>
///     A partition function and scheme are database-scoped objects with no <c>CREATE OR ALTER</c>, so the
///     creation DDL refreshed them by dropping first. That made the generated creation script
///     single-use: run it a second time and <c>DROP PARTITION SCHEME</c> failed with
///     <c>The partition scheme "ps_..." is currently being used to partition one or more tables</c>,
///     because the table it was written for now exists. Every other shape Weasel generates is
///     re-runnable, which is what Polecat's <c>generated_schema_script_executes</c> relies on
///     (polecat#842, weasel#765).
///     </para>
///     <para>
///     So the drop is conditional on the scheme being unused, and the creates are conditional on the
///     objects being absent. The two guards have to agree: skipping the drop and then creating anyway
///     would fail on <c>There is already an object named ...</c> instead, one error over.
///     </para>
///     <para>
///     What this deliberately does not do is reshape a scheme that is already bound to a table. Changing
///     boundaries on a live partitioned table is <c>SPLIT RANGE</c> / <c>MERGE RANGE</c> work, and that
///     is what <see cref="ISqlServerPartitioning.CreateDelta" /> and the delta path exist for. The
///     creation script's job is to converge, not to rebuild.
///     </para>
/// </remarks>
internal static class PartitionDdl
{
    /// <summary>
    ///     Drop the partition scheme and its function, but only when nothing is partitioned on them.
    /// </summary>
    internal static void WriteDropIfUnused(TextWriter writer, string psName, string pfName)
    {
        // sys.indexes carries a row per heap (index_id 0) as well as per index, so this sees a
        // partitioned table whether or not it has a clustered index.
        writer.WriteLine($"IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes ps");
        writer.WriteLine($"               INNER JOIN sys.indexes i ON i.data_space_id = ps.data_space_id");
        writer.WriteLine($"               WHERE ps.name = '{SchemaUtils.EscapeLiteral(psName)}')");
        writer.WriteLine("BEGIN");
        writer.WriteLine($"    IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{SchemaUtils.EscapeLiteral(psName)}')");
        writer.WriteLine($"        DROP PARTITION SCHEME {SchemaUtils.BracketName(psName)};");

        // A function cannot be dropped while any scheme still references it -- including a scheme the
        // branch above left alone.
        writer.WriteLine($"    IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{SchemaUtils.EscapeLiteral(pfName)}')");
        writer.WriteLine($"       AND NOT EXISTS (SELECT 1 FROM sys.partition_schemes ps");
        writer.WriteLine($"                       INNER JOIN sys.partition_functions pf ON pf.function_id = ps.function_id");
        writer.WriteLine($"                       WHERE pf.name = '{SchemaUtils.EscapeLiteral(pfName)}')");
        writer.WriteLine($"        DROP PARTITION FUNCTION {SchemaUtils.BracketName(pfName)};");
        writer.WriteLine("END");
    }

    /// <summary>
    ///     The guard that goes immediately before a <c>CREATE PARTITION FUNCTION</c>, so a function the
    ///     drop above declined to remove is not created a second time.
    /// </summary>
    internal static void WriteCreateFunctionGuard(TextWriter writer, string pfName)
    {
        writer.WriteLine($"IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{SchemaUtils.EscapeLiteral(pfName)}')");
    }

    /// <summary>
    ///     The guard that goes immediately before a <c>CREATE PARTITION SCHEME</c>.
    /// </summary>
    internal static void WriteCreateSchemeGuard(TextWriter writer, string psName)
    {
        writer.WriteLine($"IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{SchemaUtils.EscapeLiteral(psName)}')");
    }

    /// <summary>
    ///     The unconditional drop, for <see cref="ISqlServerPartitioning.WriteDropDdl" />: an explicit
    ///     request to remove the partition objects should say so when they are still in use rather than
    ///     silently leaving them behind.
    /// </summary>
    internal static void WriteDrop(TextWriter writer, string psName, string pfName)
    {
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{SchemaUtils.EscapeLiteral(psName)}')");
        writer.WriteLine($"    DROP PARTITION SCHEME {SchemaUtils.BracketName(psName)};");
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{SchemaUtils.EscapeLiteral(pfName)}')");
        writer.WriteLine($"    DROP PARTITION FUNCTION {SchemaUtils.BracketName(pfName)};");
    }
}
