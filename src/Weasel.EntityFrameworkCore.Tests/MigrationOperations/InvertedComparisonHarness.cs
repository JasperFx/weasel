using System.Reflection;
using JasperFx;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Weasel.Core;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Weasel.Postgresql;

namespace Weasel.EntityFrameworkCore.Tests.MigrationOperations;

/// <summary>
///     The inverse of <see cref="SchemaComparisonHarness" /> (#369): the schema
///     is defined as WEASEL objects, the generated migration files are compiled
///     with Roslyn and applied through the real EF runtime (Migrate()), and the
///     result must satisfy both the catalog-level comparison against a
///     Weasel-created schema and Weasel's own delta detection.
/// </summary>
public static class InvertedComparisonHarness
{
    /// <summary>
    ///     Run the inverted flow on PostgreSQL. <paramref name="models" /> is the
    ///     migration chain: the first entry generates the initial migration, each
    ///     later entry generates an incremental migration via the snapshot diff.
    ///     Each invocation must return a FRESH object graph.
    /// </summary>
    public static async Task<SchemaComparisonResult> RunPostgresqlAsync(
        string schemaName,
        params Func<ISchemaObject[]>[] models)
    {
        if (models.Length == 0)
        {
            throw new ArgumentException("At least one model is required", nameof(models));
        }

        var migrator = new PostgresqlMigrator();
        var options = new MigrationOperationTranslationOptions(EfMigrationProvider.PostgreSql)
        {
            Migrator = migrator, ForceRawSql = CommandLine.EfMigrationGenerator.IsPartitioned
        };

        var contextName = sanitize(schemaName) + "InvCtx";
        var ns = $"Weasel.Generated.{sanitize(schemaName)}";

        // ---- generate the migration chain --------------------------------
        var sources = new List<string>
        {
            EfMigrationFileEmitter.EmitStubContext(EfMigrationProvider.PostgreSql,
                new EfMigrationEmissionOptions(contextName) { Namespace = ns }, schemaName)
        };

        string? lastId = null;
        EfSchemaSnapshot? baseline = null;
        var timestamp = new DateTime(2026, 7, 18, 12, 0, 0, DateTimeKind.Utc);

        for (var i = 0; i < models.Length; i++)
        {
            var objects = models[i]();
            var target = EfSchemaSnapshot.FromSchemaObjects(objects, options);

            var emission = new EfMigrationEmissionOptions(contextName)
            {
                Namespace = ns, TimestampUtc = timestamp, LastMigrationId = lastId
            };

            EfMigrationFile migration;
            if (baseline == null)
            {
                migration = EfMigrationFileEmitter.EmitMigration(
                    $"Step{i}",
                    objects.ToMigrationOperations(options),
                    objects.ToDropMigrationOperations(options),
                    emission);
            }
            else
            {
                var diff = EfSnapshotDiffer.Diff(baseline, target, options);
                migration = EfMigrationFileEmitter.EmitMigration(
                    $"Step{i}", diff.UpOperations, diff.DownOperations, emission);
            }

            sources.Add(migration.Code);
            lastId = migration.MigrationId;
            baseline = target;
        }

        // ---- compile with Roslyn and apply through the EF runtime --------
        var connectionString = Postgresql.PostgresqlDbContext.ConnectionString;
        var assembly = Compile($"{ns}.Generated", sources);

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await executeAsync(conn, $"drop schema if exists \"{schemaName}\" cascade;");

        await MigrateAsync(assembly, contextName, connectionString);

        var finalObjects = models[^1]();
        var efSnapshot = await PostgresqlSchemaIntrospector.SnapshotAsync(conn, schemaName);

        // ---- Weasel's own delta detection must find nothing to do --------
        var deltaAgainstEf = await SchemaMigration.DetermineAsync(conn, default, finalObjects);
        var deltaSql = string.Empty;
        if (deltaAgainstEf.Difference != SchemaPatchDifference.None)
        {
            var writer = new StringWriter();
            deltaAgainstEf.WriteAllUpdates(writer, migrator, AutoCreate.CreateOrUpdate);
            deltaSql = writer.ToString();
        }

        // ---- Weasel creates the same schema; snapshots must match --------
        await executeAsync(conn, $"drop schema if exists \"{schemaName}\" cascade;");
        var creation = await SchemaMigration.DetermineAsync(conn, default, models[^1]());
        await migrator.ApplyAllAsync(conn, creation, AutoCreate.CreateOrUpdate);

        var weaselSnapshot = await PostgresqlSchemaIntrospector.SnapshotAsync(conn, schemaName);
        var deltaAfterWeasel = await SchemaMigration.DetermineAsync(conn, default, models[^1]());

        return new SchemaComparisonResult
        {
            // the EF-migrated catalog also contains the relocated history
            // table; exclude it from the comparison
            EfSchema = withoutHistoryTable(efSnapshot),
            WeaselSchema = weaselSnapshot,
            Differences = SchemaComparer.Compare(withoutHistoryTable(efSnapshot), weaselSnapshot),
            DeltaAgainstEfSchema = deltaAgainstEf.Difference,
            DeltaUpdateSql = deltaSql,
            DeltaAfterWeaselCreate = deltaAfterWeasel.Difference
        };
    }

    /// <summary>
    ///     The SQL Server variant of the inverted flow.
    /// </summary>
    public static async Task<SchemaComparisonResult> RunSqlServerAsync(
        string schemaName,
        params Func<ISchemaObject[]>[] models)
    {
        if (models.Length == 0)
        {
            throw new ArgumentException("At least one model is required", nameof(models));
        }

        var migrator = new Weasel.SqlServer.SqlServerMigrator();
        var options = new MigrationOperationTranslationOptions(EfMigrationProvider.SqlServer)
        {
            Migrator = migrator, ForceRawSql = CommandLine.EfMigrationGenerator.IsPartitioned
        };

        var contextName = sanitize(schemaName) + "InvCtx";
        var ns = $"Weasel.Generated.{sanitize(schemaName)}";

        var sources = new List<string>
        {
            EfMigrationFileEmitter.EmitStubContext(EfMigrationProvider.SqlServer,
                new EfMigrationEmissionOptions(contextName) { Namespace = ns }, schemaName)
        };

        string? lastId = null;
        EfSchemaSnapshot? baseline = null;
        var timestamp = new DateTime(2026, 7, 18, 12, 0, 0, DateTimeKind.Utc);

        for (var i = 0; i < models.Length; i++)
        {
            var objects = models[i]();
            var target = EfSchemaSnapshot.FromSchemaObjects(objects, options);

            var emission = new EfMigrationEmissionOptions(contextName)
            {
                Namespace = ns, TimestampUtc = timestamp, LastMigrationId = lastId
            };

            var migration = baseline == null
                ? EfMigrationFileEmitter.EmitMigration($"Step{i}",
                    objects.ToMigrationOperations(options),
                    objects.ToDropMigrationOperations(options), emission)
                : EfMigrationFileEmitter.EmitMigration($"Step{i}",
                    EfSnapshotDiffer.Diff(baseline, target, options).UpOperations,
                    EfSnapshotDiffer.Diff(baseline, target, options).DownOperations, emission);

            sources.Add(migration.Code);
            lastId = migration.MigrationId;
            baseline = target;
        }

        var connectionString = SqlServer.SqlServerDbContext.ConnectionString;
        await SqlServer.SqlServerDatabaseBootstrap.EnsureDatabaseExistsAsync(connectionString);
        var assembly = Compile($"{ns}.Generated", sources);

        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(connectionString);
        await conn.OpenAsync();
        await dropSqlServerSchemaAsync(conn, schemaName);

        await MigrateAsync(assembly, contextName, connectionString);

        var efSnapshot = await SqlServerSchemaIntrospector.SnapshotAsync(conn, schemaName);

        var deltaAgainstEf = await SchemaMigration.DetermineAsync(conn, default, models[^1]());
        var deltaSql = string.Empty;
        if (deltaAgainstEf.Difference != SchemaPatchDifference.None)
        {
            var writer = new StringWriter();
            deltaAgainstEf.WriteAllUpdates(writer, migrator, AutoCreate.CreateOrUpdate);
            deltaSql = writer.ToString();
        }

        await dropSqlServerSchemaAsync(conn, schemaName);
        var creation = await SchemaMigration.DetermineAsync(conn, default, models[^1]());
        await migrator.ApplyAllAsync(conn, creation, AutoCreate.CreateOrUpdate);

        var weaselSnapshot = await SqlServerSchemaIntrospector.SnapshotAsync(conn, schemaName);
        var deltaAfterWeasel = await SchemaMigration.DetermineAsync(conn, default, models[^1]());

        return new SchemaComparisonResult
        {
            EfSchema = withoutHistoryTable(efSnapshot),
            WeaselSchema = weaselSnapshot,
            Differences = SchemaComparer.Compare(withoutHistoryTable(efSnapshot), weaselSnapshot),
            DeltaAgainstEfSchema = deltaAgainstEf.Difference,
            DeltaUpdateSql = deltaSql,
            DeltaAfterWeaselCreate = deltaAfterWeasel.Difference
        };
    }

    private static async Task dropSqlServerSchemaAsync(Microsoft.Data.SqlClient.SqlConnection conn, string schemaName)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"
IF SCHEMA_ID('{schemaName}') IS NOT NULL
BEGIN
    DECLARE @sql NVARCHAR(MAX) = N'';
    SELECT @sql += N'ALTER TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N' DROP CONSTRAINT ' + QUOTENAME(fk.name) + N';'
    FROM sys.foreign_keys fk
        JOIN sys.tables t ON fk.parent_object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = '{schemaName}';
    SELECT @sql += N'DROP TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N';'
    FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = '{schemaName}';
    SELECT @sql += N'DROP SEQUENCE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(sq.name) + N';'
    FROM sys.sequences sq JOIN sys.schemas s ON sq.schema_id = s.schema_id
    WHERE s.name = '{schemaName}';
    EXEC sp_executesql @sql;
    EXEC('DROP SCHEMA [{schemaName}]');
END";
        await cmd.ExecuteNonQueryAsync();
    }

    // ------------------------------------------------------------------
    // compile + run
    // ------------------------------------------------------------------

    public static Assembly Compile(string assemblyName, IEnumerable<string> sources)
    {
        // the assemblies the generated code needs, referenced explicitly so we
        // don't depend on what happens to be loaded in the test host yet
        var required = new[]
        {
            typeof(object).Assembly,
            typeof(Enumerable).Assembly,
            typeof(DbContext).Assembly,
            typeof(Microsoft.EntityFrameworkCore.Migrations.Migration).Assembly,
            typeof(Microsoft.EntityFrameworkCore.Diagnostics.RelationalEventId).Assembly,
            typeof(NpgsqlConnection).Assembly,
            typeof(NpgsqlDbContextOptionsBuilderExtensions).Assembly,
            typeof(Microsoft.Data.SqlClient.SqlConnection).Assembly,
            typeof(SqlServerDbContextOptionsExtensions).Assembly,
            typeof(Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.NpgsqlValueGenerationStrategy).Assembly
        };

        var locations = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .Select(a => a.Location)
            .Concat(required.Select(a => a.Location))
            .Distinct(StringComparer.OrdinalIgnoreCase);

        var references = locations
            .Select(l => (MetadataReference)MetadataReference.CreateFromFile(l))
            .ToList();

        var compilation = CSharpCompilation.Create(
            assemblyName,
            sources.Select(s => CSharpSyntaxTree.ParseText(s)),
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));

        using var stream = new MemoryStream();
        var result = compilation.Emit(stream);

        if (!result.Success)
        {
            var errors = result.Diagnostics
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .Select(d => d.ToString());
            throw new InvalidOperationException(
                "Generated migration sources failed to compile:\n" + string.Join("\n", errors) +
                "\n\n---- sources ----\n" + string.Join("\n\n", sources));
        }

        stream.Position = 0;
        return Assembly.Load(stream.ToArray());
    }

    public static async Task MigrateAsync(Assembly assembly, string contextName, string connectionString)
    {
        var contextType = assembly.GetTypes()
            .Single(t => typeof(DbContext).IsAssignableFrom(t) && t.Name == contextName);

        contextType.GetProperty("ConnectionString", BindingFlags.Public | BindingFlags.Static)!
            .SetValue(null, connectionString);

        await using var context = (DbContext)Activator.CreateInstance(contextType)!;
        await context.Database.MigrateAsync();
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static SchemaSnapshot withoutHistoryTable(SchemaSnapshot snapshot)
    {
        var filtered = snapshot.Tables
            .Where(t => t.Name != CommandLine.EfMigrationGenerator.HistoryTableName)
            .ToList();
        return new SchemaSnapshot(snapshot.SchemaName, filtered, snapshot.Sequences);
    }

    private static string sanitize(string name)
        => new(name.Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());

    private static async Task executeAsync(NpgsqlConnection conn, string sql)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }
}
