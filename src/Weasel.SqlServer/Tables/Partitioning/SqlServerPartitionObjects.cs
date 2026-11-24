using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Weasel.Core;

namespace Weasel.SqlServer.Tables.Partitioning;

/// <summary>
///     How Weasel names the partition function and partition scheme behind a partitioned SQL Server table,
///     and how it finds the ones a table is actually on.
/// </summary>
/// <remarks>
///     <para>
///     A partition function and a partition scheme are <b>database</b>-scoped, not schema-scoped. Before
///     10.0 Weasel named them after the table and column alone (<c>pf_{table}_{column}</c>), so two tables of
///     the same name in two schemas of one database shared one function, and a <c>SPLIT RANGE</c> or
///     <c>MERGE RANGE</c> made for one of them changed the other's partitioning too (weasel#920). Weasel now
///     puts the schema into the name: <c>pf_{schema}_{table}_{column}</c>.
///     </para>
///     <para>
///     SQL Server cannot rename a partition function or scheme, and moving a table onto another scheme
///     rewrites every row. So a table that is already partitioned keeps the objects it is on, whatever they
///     are called: delta detection compares boundaries, never names, and every <c>SPLIT</c>, <c>MERGE</c> and
///     <c>NEXT USED</c> Weasel issues targets the function and scheme the table is bound to in the catalog.
///     Only a table that is partitioned from scratch -- a new table, a table moved onto its scheme, or a
///     table rebuilt for some other reason -- gets the schema-qualified names.
///     </para>
/// </remarks>
public static class SqlServerPartitionObjects
{
    /// <summary>
    ///     The longest identifier SQL Server accepts (<c>sysname</c>).
    /// </summary>
    public const int MaxNameLength = 128;

    /// <summary>
    ///     The name Weasel gives the partition function of a newly partitioned table:
    ///     <c>pf_{schema}_{table}_{column}</c>. A name longer than <see cref="MaxNameLength" /> is shortened
    ///     and given a stable hash suffix so that it stays unique.
    /// </summary>
    public static string FunctionNameFor(Table table, string column) => build("pf", table, column);

    /// <summary>
    ///     The name Weasel gives the partition scheme of a newly partitioned table:
    ///     <c>ps_{schema}_{table}_{column}</c>.
    /// </summary>
    public static string SchemeNameFor(Table table, string column) => build("ps", table, column);

    /// <summary>
    ///     The partition function name Weasel used before 10.0, with no schema in it: <c>pf_{table}_{column}</c>.
    ///     A table already partitioned on it keeps it; see the remarks on <see cref="SqlServerPartitionObjects" />.
    /// </summary>
    public static string LegacyFunctionNameFor(Table table, string column)
        => $"pf_{table.Identifier.Name}_{column}";

    /// <summary>
    ///     The partition scheme name Weasel used before 10.0: <c>ps_{table}_{column}</c>.
    /// </summary>
    public static string LegacySchemeNameFor(Table table, string column)
        => $"ps_{table.Identifier.Name}_{column}";

    /// <summary>
    ///     Every partition function in the database that tables in more than one schema are partitioned on.
    ///     Each of those tables gets every boundary any of the others gets, so a function listed here is one
    ///     whose tables cannot keep different boundaries -- per-tenant partitioning, or RANGE partitioning
    ///     with different boundaries per schema -- until all but one of them move off it.
    /// </summary>
    /// <remarks>
    ///     Only a database partitioned by a Weasel older than 10.0 can have one of these by Weasel's doing.
    ///     The partitioning page of the documentation describes how to move a table off a shared function.
    /// </remarks>
    public static async Task<IReadOnlyList<SharedPartitionFunction>> FindSharedAcrossSchemasAsync(
        SqlConnection connection, CancellationToken token = default)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
                          SELECT pf.name, ps.name, OBJECT_SCHEMA_NAME(i.object_id), OBJECT_NAME(i.object_id)
                          FROM sys.indexes i
                          INNER JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
                          INNER JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
                          WHERE i.index_id <= 1
                          ORDER BY pf.name, ps.name, 3, 4;
                          """;

        var rows = new List<(string Function, string Scheme, string Schema, string Table)>();
        await using (var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
            }
        }

        return rows
            .GroupBy(x => x.Function, StringComparer.OrdinalIgnoreCase)
            .Where(g => g.Select(x => x.Schema).Distinct(StringComparer.OrdinalIgnoreCase).Count() > 1)
            .Select(g => new SharedPartitionFunction(
                g.Key,
                g.Select(x => x.Scheme).Distinct(StringComparer.OrdinalIgnoreCase).ToArray(),
                g.Select(x => (DbObjectName)new SqlServerObjectName(x.Schema, x.Table)).ToArray()))
            .ToArray();
    }

    /// <summary>
    ///     The partition function and scheme the table's heap or clustered index is on right now, or null when
    ///     the table does not exist or is not partitioned.
    /// </summary>
    internal static async Task<BoundPartitionObjects?> ReadBoundAsync(SqlConnection connection, Table table,
        CancellationToken token)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
                          SELECT TOP (1) pf.name, ps.name
                          FROM sys.indexes i
                          INNER JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
                          INNER JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
                          WHERE i.object_id = OBJECT_ID(@table) AND i.index_id <= 1;
                          """;
        cmd.Parameters.AddWithValue("@table", table.Identifier.ToString());

        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            return null;
        }

        return new BoundPartitionObjects(reader.GetString(0), reader.GetString(1));
    }

    /// <summary>
    ///     The partition function and scheme to issue <c>SPLIT</c>, <c>MERGE</c> and <c>NEXT USED</c> against
    ///     for <paramref name="table" />: the ones it is bound to when it is partitioned, which may carry a
    ///     pre-10.0 name, or the names <paramref name="partitioning" /> would create otherwise. Logs a warning,
    ///     once per function per <paramref name="warned" /> set, when the bound function is shared with a
    ///     table in another schema.
    /// </summary>
    internal static async Task<BoundPartitionObjects> ResolveAsync(SqlConnection connection, Table table,
        ISqlServerPartitioning partitioning, ILogger logger, ConcurrentDictionary<string, byte> warned,
        CancellationToken token)
    {
        var bound = await ReadBoundAsync(connection, table, token).ConfigureAwait(false);
        if (bound == null)
        {
            return new BoundPartitionObjects(partitioning.PartitionFunctionName(table),
                partitioning.PartitionSchemeName(table));
        }

        if (!bound.FunctionName.Equals(partitioning.PartitionFunctionName(table), StringComparison.OrdinalIgnoreCase)
            && warned.TryAdd($"{connection.DataSource}/{connection.Database}/{bound.FunctionName}", 0))
        {
            var others = await otherSchemasOnFunctionAsync(connection, bound.FunctionName,
                table.Identifier.Schema, token).ConfigureAwait(false);

            if (others.Count > 0)
            {
                logger.LogWarning(
                    "Table {Table} is partitioned on {Function}, a partition function that tables in schema(s) {Schemas} " +
                    "are partitioned on too. Partition functions are database-scoped, so every boundary split into or " +
                    "merged out of it changes all of those tables. Weasel names a new table's function after its schema " +
                    "(weasel#920); move this table off the shared function to give it boundaries of its own",
                    table.Identifier, bound.FunctionName, string.Join(", ", others));
            }
        }

        return bound;
    }

    private static async Task<IReadOnlyList<string>> otherSchemasOnFunctionAsync(SqlConnection connection,
        string functionName, string schema, CancellationToken token)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
                          SELECT DISTINCT OBJECT_SCHEMA_NAME(i.object_id)
                          FROM sys.indexes i
                          INNER JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
                          INNER JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
                          WHERE pf.name = @pf AND i.index_id <= 1 AND OBJECT_SCHEMA_NAME(i.object_id) <> @schema;
                          """;
        cmd.Parameters.AddWithValue("@pf", functionName);
        cmd.Parameters.AddWithValue("@schema", schema);

        var schemas = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            schemas.Add(reader.GetString(0));
        }

        return schemas;
    }

    private static string build(string prefix, Table table, string column)
    {
        var name = $"{prefix}_{table.Identifier.Schema}_{table.Identifier.Name}_{column}";
        if (name.Length <= MaxNameLength)
        {
            return name;
        }

        // Truncating alone could make two long names equal; the hash of the whole name keeps them apart and
        // is the same on every host, so every process agrees on the name.
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(name)))[..8].ToLowerInvariant();
        return $"{name[..(MaxNameLength - hash.Length - 1)]}_{hash}";
    }
}

/// <summary>
///     A partition function that tables in more than one schema are partitioned on. See
///     <see cref="SqlServerPartitionObjects.FindSharedAcrossSchemasAsync" />.
/// </summary>
/// <param name="FunctionName">The partition function.</param>
/// <param name="SchemeNames">The partition schemes built on it that those tables are on.</param>
/// <param name="Tables">Every table partitioned on it.</param>
public sealed record SharedPartitionFunction(
    string FunctionName,
    IReadOnlyList<string> SchemeNames,
    IReadOnlyList<DbObjectName> Tables);

internal sealed record BoundPartitionObjects(string FunctionName, string SchemeName);
