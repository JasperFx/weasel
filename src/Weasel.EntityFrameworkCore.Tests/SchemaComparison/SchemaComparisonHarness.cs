using System.Text;
using System.Text.RegularExpressions;
using JasperFx;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql;
using Weasel.SqlServer;

namespace Weasel.EntityFrameworkCore.Tests.SchemaComparison;

public class SchemaComparisonResult
{
    public required SchemaSnapshot EfSchema { get; init; }
    public required SchemaSnapshot WeaselSchema { get; init; }
    public required IReadOnlyList<SchemaDifference> Differences { get; init; }

    /// <summary>
    ///     What Weasel's own delta detection reports when comparing its mapped
    ///     model against the schema EF Core created. Should be None: Weasel
    ///     must not want to "migrate" a schema EF just created.
    /// </summary>
    public required SchemaPatchDifference DeltaAgainstEfSchema { get; init; }

    /// <summary>The SQL Weasel would run against the EF-created schema (diagnostics)</summary>
    public required string DeltaUpdateSql { get; init; }

    /// <summary>
    ///     Weasel's delta re-run after Weasel itself created the schema.
    ///     Should always be None (self-consistency / idempotency).
    /// </summary>
    public required SchemaPatchDifference DeltaAfterWeaselCreate { get; init; }

    /// <summary>
    ///     Assert full parity: catalog-level parity between the EF-created and
    ///     Weasel-created schemas, plus zero Weasel delta in both directions.
    ///     Difference categories listed in <paramref name="tolerated" /> are
    ///     reported but do not fail the assertion.
    /// </summary>
    public void AssertParity(params DifferenceCategory[] tolerated)
    {
        var failing = Differences.Where(d => !tolerated.Contains(d.Category)).ToList();

        var report = new StringBuilder();
        if (failing.Any())
        {
            report.AppendLine($"Schema comparison found {failing.Count} difference(s) between the EF Core-created and Weasel-created schemas:");
            foreach (var difference in failing)
            {
                report.AppendLine($"  {difference}");
            }
        }

        if (DeltaAgainstEfSchema != SchemaPatchDifference.None)
        {
            report.AppendLine($"Weasel delta against the EF-created schema was {DeltaAgainstEfSchema} (expected None). Weasel would run:");
            report.AppendLine(DeltaUpdateSql);
        }

        if (DeltaAfterWeaselCreate != SchemaPatchDifference.None)
        {
            report.AppendLine($"Weasel delta against its own created schema was {DeltaAfterWeaselCreate} (expected None) - Weasel migrations are not idempotent for this model.");
        }

        if (report.Length > 0)
        {
            throw new SchemaComparisonException(report.ToString());
        }
    }
}

public class SchemaComparisonException : Exception
{
    public SchemaComparisonException(string message) : base(message)
    {
    }
}

/// <summary>
///     The comparison harness. For a DbContext whose model lives entirely in one
///     dedicated schema:
///     1. Drops + recreates the schema with EF Core's own create script (what
///        EF migrations would produce) and snapshots the result from the catalog.
///     2. Runs Weasel's delta detection against that EF-created schema — it
///        should find nothing to do.
///     3. Drops the schema again, lets Weasel create it from the mapped model,
///        snapshots it, and re-runs delta detection (idempotency).
///     4. Compares both snapshots.
/// </summary>
public static class SchemaComparisonHarness
{
    public static async Task<SchemaComparisonResult> RunPostgresqlAsync(
        DbContext context,
        string schemaName,
        Action<ITable[]>? customizeTables = null)
    {
        var migrator = new PostgresqlMigrator();
        var connectionString = context.Database.GetConnectionString()
                               ?? throw new InvalidOperationException("DbContext has no connection string");

        var tables = DbContextExtensions.GetEntityTypesForMigration(context)
            .Select(migrator.MapToTable)
            .ToArray();

        guardSchemaIsolation(tables, schemaName);
        customizeTables?.Invoke(tables);

        var schemaObjects = tables.OfType<ISchemaObject>().ToArray();

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        // ---- Phase 1: EF Core creates the schema --------------------------
        await dropSchemaAsync(conn, schemaName);
        await executeAsync(conn, $"CREATE SCHEMA \"{schemaName}\"");

        var efScript = context.Database.GenerateCreateScript();
        await executeAsync(conn, efScript);

        var efSnapshot = await PostgresqlSchemaIntrospector.SnapshotAsync(conn, schemaName);

        // ---- Phase 2: Weasel delta against the EF-created schema ----------
        var deltaAgainstEf = await SchemaMigration.DetermineAsync(conn, default, schemaObjects);
        var deltaSql = string.Empty;
        if (deltaAgainstEf.Difference != SchemaPatchDifference.None)
        {
            try
            {
                var writer = new StringWriter();
                deltaAgainstEf.WriteAllUpdates(writer, migrator, AutoCreate.CreateOrUpdate);
                deltaSql = writer.ToString();
            }
            catch (Exception e)
            {
                deltaSql = $"<could not generate update SQL: {e.Message}>";
            }
        }

        // ---- Phase 3: Weasel creates the schema from scratch --------------
        await dropSchemaAsync(conn, schemaName);

        var creationMigration = await SchemaMigration.DetermineAsync(conn, default, schemaObjects);
        await migrator.ApplyAllAsync(conn, creationMigration, AutoCreate.CreateOrUpdate);

        var weaselSnapshot = await PostgresqlSchemaIntrospector.SnapshotAsync(conn, schemaName);

        var deltaAfterWeasel = await SchemaMigration.DetermineAsync(conn, default, schemaObjects);

        return new SchemaComparisonResult
        {
            EfSchema = efSnapshot,
            WeaselSchema = weaselSnapshot,
            Differences = SchemaComparer.Compare(efSnapshot, weaselSnapshot),
            DeltaAgainstEfSchema = deltaAgainstEf.Difference,
            DeltaUpdateSql = deltaSql,
            DeltaAfterWeaselCreate = deltaAfterWeasel.Difference
        };
    }

    public static async Task<SchemaComparisonResult> RunSqlServerAsync(
        DbContext context,
        string schemaName,
        Action<ITable[]>? customizeTables = null)
    {
        var migrator = new SqlServerMigrator();
        var connectionString = context.Database.GetConnectionString()
                               ?? throw new InvalidOperationException("DbContext has no connection string");

        var tables = DbContextExtensions.GetEntityTypesForMigration(context)
            .Select(migrator.MapToTable)
            .ToArray();

        guardSchemaIsolation(tables, schemaName);
        customizeTables?.Invoke(tables);

        var schemaObjects = tables.OfType<ISchemaObject>().ToArray();

        await SqlServer.SqlServerDatabaseBootstrap.EnsureDatabaseExistsAsync(connectionString);

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        // ---- Phase 1: EF Core creates the schema --------------------------
        await dropSqlServerSchemaObjectsAsync(conn, schemaName);
        await executeSqlServerAsync(conn, $"IF SCHEMA_ID('{schemaName}') IS NULL EXEC('CREATE SCHEMA [{schemaName}]')");

        // SQL Server create scripts contain GO batch separators
        foreach (var batch in splitSqlServerBatches(context.Database.GenerateCreateScript()))
        {
            await executeSqlServerAsync(conn, batch);
        }

        var efSnapshot = await SqlServerSchemaIntrospector.SnapshotAsync(conn, schemaName);

        // ---- Phase 2: Weasel delta against the EF-created schema ----------
        var deltaAgainstEf = await SchemaMigration.DetermineAsync(conn, default, schemaObjects);
        var deltaSql = string.Empty;
        if (deltaAgainstEf.Difference != SchemaPatchDifference.None)
        {
            try
            {
                var writer = new StringWriter();
                deltaAgainstEf.WriteAllUpdates(writer, migrator, AutoCreate.CreateOrUpdate);
                deltaSql = writer.ToString();
            }
            catch (Exception e)
            {
                deltaSql = $"<could not generate update SQL: {e.Message}>";
            }
        }

        // ---- Phase 3: Weasel creates the schema from scratch --------------
        await dropSqlServerSchemaObjectsAsync(conn, schemaName);

        var creationMigration = await SchemaMigration.DetermineAsync(conn, default, schemaObjects);
        await migrator.ApplyAllAsync(conn, creationMigration, AutoCreate.CreateOrUpdate);

        var weaselSnapshot = await SqlServerSchemaIntrospector.SnapshotAsync(conn, schemaName);

        var deltaAfterWeasel = await SchemaMigration.DetermineAsync(conn, default, schemaObjects);

        return new SchemaComparisonResult
        {
            EfSchema = efSnapshot,
            WeaselSchema = weaselSnapshot,
            Differences = SchemaComparer.Compare(efSnapshot, weaselSnapshot),
            DeltaAgainstEfSchema = deltaAgainstEf.Difference,
            DeltaUpdateSql = deltaSql,
            DeltaAfterWeaselCreate = deltaAfterWeasel.Difference
        };
    }

    /// <summary>
    ///     SQL Server has no DROP SCHEMA CASCADE; drop the schema's inbound
    ///     foreign keys, then its tables, scoped strictly to the given schema.
    /// </summary>
    private static async Task dropSqlServerSchemaObjectsAsync(SqlConnection conn, string schemaName)
    {
        await executeSqlServerAsync(conn, $"""
            declare @sql nvarchar(max) = N'';
            select @sql += N'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name)
                + N' DROP CONSTRAINT ' + QUOTENAME(fk.name) + N';'
            from sys.foreign_keys fk
            join sys.tables t on t.object_id = fk.parent_object_id
            where SCHEMA_NAME(fk.schema_id) = '{schemaName}'
               or SCHEMA_NAME(t.schema_id) = '{schemaName}';
            exec sp_executesql @sql;
            """);

        await executeSqlServerAsync(conn, $"""
            declare @sql nvarchar(max) = N'';
            select @sql += N'DROP TABLE ' + QUOTENAME('{schemaName}') + N'.' + QUOTENAME(name) + N';'
            from sys.tables where SCHEMA_NAME(schema_id) = '{schemaName}';
            exec sp_executesql @sql;
            """);
    }

    private static IEnumerable<string> splitSqlServerBatches(string script)
        => Regex.Split(script, @"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase)
            .Where(batch => !string.IsNullOrWhiteSpace(batch));

    private static async Task executeSqlServerAsync(SqlConnection conn, string sql)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    ///     Every mapped table must live in the dedicated test schema — this
    ///     harness drops that schema with CASCADE, and must never be able to
    ///     touch shared tables in other schemas.
    /// </summary>
    private static void guardSchemaIsolation(ITable[] tables, string schemaName)
    {
        var strays = tables
            .Where(t => !t.Identifier.Schema.Equals(schemaName, StringComparison.OrdinalIgnoreCase))
            .Select(t => t.Identifier.QualifiedName)
            .ToList();

        if (strays.Any())
        {
            throw new InvalidOperationException(
                $"All entity types must be mapped into the dedicated schema '{schemaName}' " +
                $"(use modelBuilder.HasDefaultSchema(...)), but found: {string.Join(", ", strays)}");
        }
    }

    private static async Task dropSchemaAsync(NpgsqlConnection conn, string schemaName)
        => await executeAsync(conn, $"DROP SCHEMA IF EXISTS \"{schemaName}\" CASCADE");

    private static async Task executeAsync(NpgsqlConnection conn, string sql)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }
}
