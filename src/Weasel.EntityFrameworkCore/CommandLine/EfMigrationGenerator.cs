using System.Data.Common;
using System.Text.RegularExpressions;
using JasperFx.Core;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.EntityFrameworkCore.CommandLine;

public class EfMigrationGenerationOptions
{
    /// <summary>Directory the migration files, stub context and snapshot are written to</summary>
    public string Directory { get; set; } = "WeaselMigrations";

    public string Namespace { get; set; } = "WeaselMigrations";

    /// <summary>
    ///     Stub context type name. Defaults to a sanitized
    ///     "&lt;DatabaseIdentifier&gt;SchemaDbContext".
    /// </summary>
    public string? ContextTypeName { get; set; }

    /// <summary>
    ///     Schema the __EFMigrationsHistory table is relocated into. Defaults to
    ///     the first non-default schema among the database's objects so it never
    ///     collides with the application's own EF context.
    /// </summary>
    public string? HistorySchema { get; set; }

    /// <summary>Override the provider detection from the database's Migrator type</summary>
    public EfMigrationProvider? Provider { get; set; }

    /// <summary>
    ///     Which objects route through the raw-SQL fallback. Defaults to
    ///     partition-strategy detection over the concrete table types.
    /// </summary>
    public Func<ISchemaObject, bool>? ForceRawSql { get; set; }
}

public record EfMigrationAddResult(
    bool HasChanges,
    string? MigrationId,
    string? MigrationFile,
    string? ContextFile,
    string SnapshotFile);

/// <summary>
///     Orchestrates EF migration generation for an <see cref="IDatabase" /> —
///     the engine behind the db-ef-migration command, kept separate so it is
///     directly testable and usable programmatically.
/// </summary>
public static class EfMigrationGenerator
{
    public const string SnapshotFileName = "weasel-schema-snapshot.json";
    public const string HistoryTableName = "__EFMigrationsHistory";

    private static readonly Regex MigrationFilePattern =
        new(@"^\d{14}_.+\.cs$", RegexOptions.Compiled);

    /// <summary>
    ///     Detect the EF provider from the database's Migrator type. Only
    ///     PostgreSQL and SQL Server are supported for EF migration generation.
    /// </summary>
    public static EfMigrationProvider DetectProvider(IDatabase database)
    {
        var migratorType = database.Migrator.GetType().FullName ?? string.Empty;

        if (migratorType.Contains("Postgresql", StringComparison.OrdinalIgnoreCase))
        {
            return EfMigrationProvider.PostgreSql;
        }

        if (migratorType.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
        {
            return EfMigrationProvider.SqlServer;
        }

        throw new NotSupportedException(
            $"EF migration generation supports PostgreSQL and SQL Server; the database '{database.Identifier}' " +
            $"uses {migratorType}");
    }

    /// <summary>
    ///     Default raw-SQL routing: any concrete table carrying a partitioning
    ///     strategy (detected structurally, so this assembly needs no provider
    ///     references) goes through its own Weasel DDL — EF has no model for
    ///     partitioned tables.
    /// </summary>
    public static bool IsPartitioned(ISchemaObject schemaObject)
    {
        var type = schemaObject.GetType();

        foreach (var propertyName in new[] { "Partitioning", "SqlServerPartitioning" })
        {
            if (type.GetProperty(propertyName)?.GetValue(schemaObject) != null)
            {
                return true;
            }
        }

        var strategy = type.GetProperty("PartitionStrategy")?.GetValue(schemaObject);
        return strategy != null && strategy.ToString() != "None";
    }

    /// <summary>
    ///     Generate the next migration for the database. First run scaffolds the
    ///     stub context, the initial "create everything" migration and the
    ///     snapshot; subsequent runs diff against the snapshot (or the live
    ///     database when <paramref name="againstDatabase" /> is true) and emit an
    ///     incremental migration.
    /// </summary>
    public static async Task<EfMigrationAddResult> AddAsync(
        IDatabase database,
        string name,
        EfMigrationGenerationOptions options,
        bool againstDatabase = false,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("A migration name is required", nameof(name));
        }

        var provider = options.Provider ?? DetectProvider(database);
        var translation = new MigrationOperationTranslationOptions(provider)
        {
            Migrator = database.Migrator, ForceRawSql = options.ForceRawSql ?? IsPartitioned
        };

        var objects = database.AllObjects().ToArray();
        var target = EfSchemaSnapshot.FromSchemaObjects(objects, translation);

        System.IO.Directory.CreateDirectory(options.Directory);
        var snapshotFile = Path.Combine(options.Directory, SnapshotFileName);

        var contextTypeName = options.ContextTypeName ?? defaultContextName(database);
        var emission = new EfMigrationEmissionOptions(contextTypeName) { Namespace = options.Namespace };

        if (!File.Exists(snapshotFile))
        {
            // first run: stub context + initial create-everything migration
            var migration = EfMigrationFileEmitter.EmitMigration(
                name,
                objects.ToMigrationOperations(translation),
                objects.ToDropMigrationOperations(translation),
                emission);

            var historySchema = options.HistorySchema ?? defaultHistorySchema(objects, translation);
            var contextCode = EfMigrationFileEmitter.EmitStubContext(provider, emission, historySchema);
            var contextFile = Path.Combine(options.Directory, $"{contextTypeName}.cs");
            var migrationFile = Path.Combine(options.Directory, migration.FileName);

            await File.WriteAllTextAsync(contextFile, contextCode, ct).ConfigureAwait(false);
            await File.WriteAllTextAsync(migrationFile, migration.Code, ct).ConfigureAwait(false);

            target.MigrationId = migration.MigrationId;
            await File.WriteAllTextAsync(snapshotFile, target.ToJson(), ct).ConfigureAwait(false);

            return new EfMigrationAddResult(true, migration.MigrationId, migrationFile, contextFile, snapshotFile);
        }

        var baseline = EfSchemaSnapshot.FromJson(await File.ReadAllTextAsync(snapshotFile, ct).ConfigureAwait(false));

        var operations = againstDatabase
            ? await EfSnapshotDiffer.DiffAgainstDatabaseAsync(database, ct: ct).ConfigureAwait(false)
            : EfSnapshotDiffer.Diff(baseline, target, translation);

        if (!operations.HasChanges)
        {
            return new EfMigrationAddResult(false, null, null, null, snapshotFile);
        }

        emission.LastMigrationId = baseline.MigrationId;
        var incremental = EfMigrationFileEmitter.EmitMigration(
            name, operations.UpOperations, operations.DownOperations, emission);

        var incrementalFile = Path.Combine(options.Directory, incremental.FileName);
        await File.WriteAllTextAsync(incrementalFile, incremental.Code, ct).ConfigureAwait(false);

        target.MigrationId = incremental.MigrationId;
        await File.WriteAllTextAsync(snapshotFile, target.ToJson(), ct).ConfigureAwait(false);

        return new EfMigrationAddResult(true, incremental.MigrationId, incrementalFile, null, snapshotFile);
    }

    /// <summary>
    ///     Adopt a pre-existing database: insert __EFMigrationsHistory rows for
    ///     every migration file in the output directory without executing them
    ///     (the EF-sanctioned baselining/squashing technique). Returns the
    ///     migration ids that were newly recorded.
    /// </summary>
    public static async Task<IReadOnlyList<string>> BaselineAsync(
        IDatabase database,
        EfMigrationGenerationOptions options,
        CancellationToken ct = default)
    {
        var provider = options.Provider ?? DetectProvider(database);
        var migrationIds = MigrationIdsIn(options.Directory);

        if (!migrationIds.Any())
        {
            return Array.Empty<string>();
        }

        var translation = new MigrationOperationTranslationOptions(provider)
        {
            Migrator = database.Migrator, ForceRawSql = options.ForceRawSql ?? IsPartitioned
        };
        var historySchema = options.HistorySchema ??
                            defaultHistorySchema(database.AllObjects().ToArray(), translation);

        await using var connection = createConnection(database);
        await connection.OpenAsync(ct).ConfigureAwait(false);

        await ensureHistoryTableAsync(connection, provider, historySchema, ct).ConfigureAwait(false);

        var recorded = new List<string>();
        var productVersion = ProductInfo.GetVersion();

        foreach (var migrationId in migrationIds)
        {
            var inserted = await insertHistoryRowAsync(connection, provider, historySchema, migrationId,
                productVersion, ct).ConfigureAwait(false);
            if (inserted)
            {
                recorded.Add(migrationId);
            }
        }

        return recorded;
    }

    /// <summary>The ordered migration ids found as files in the output directory</summary>
    public static IReadOnlyList<string> MigrationIdsIn(string directory)
    {
        if (!System.IO.Directory.Exists(directory))
        {
            return Array.Empty<string>();
        }

        return System.IO.Directory.EnumerateFiles(directory, "*.cs")
            .Select(Path.GetFileName)
            .OfType<string>()
            .Where(x => MigrationFilePattern.IsMatch(x))
            .Select(x => x[..^3])
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();
    }

    // ------------------------------------------------------------------
    // internals
    // ------------------------------------------------------------------

    private static string defaultContextName(IDatabase database)
    {
        var sanitized = new string(database.Identifier
            .Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
        if (sanitized.IsEmpty() || !char.IsLetter(sanitized[0]))
        {
            sanitized = "Weasel" + sanitized;
        }

        // PascalCase-ish: capitalize segments split by underscores
        var parts = sanitized.Split('_', StringSplitOptions.RemoveEmptyEntries)
            .Select(p => char.ToUpperInvariant(p[0]) + p[1..]);
        return $"{string.Join(string.Empty, parts)}SchemaDbContext";
    }

    private static string defaultHistorySchema(
        ISchemaObject[] objects,
        MigrationOperationTranslationOptions translation)
    {
        return objects
                   .Select(x => x.Identifier.Schema)
                   .FirstOrDefault(s => s.IsNotEmpty() && !s.EqualsIgnoreCase(translation.DefaultSchema))
               ?? translation.DefaultSchema;
    }

    private static DbConnection createConnection(IDatabase database)
    {
        // CreateConnection lives on IConnectionSource<TConnection> behind the
        // closed generic IDatabase<TConnection>; resolve it structurally so
        // this assembly stays free of provider references
        var method = database.GetType().GetMethod("CreateConnection", Type.EmptyTypes);

        if (method?.Invoke(database, null) is DbConnection connection)
        {
            return connection;
        }

        throw new NotSupportedException(
            $"Could not create a connection for database '{database.Identifier}' — it does not implement IDatabase<TConnection>");
    }

    private static async Task ensureHistoryTableAsync(
        DbConnection connection,
        EfMigrationProvider provider,
        string historySchema,
        CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = provider == EfMigrationProvider.PostgreSql
            ? $"""
               create schema if not exists "{historySchema}";
               create table if not exists "{historySchema}"."{HistoryTableName}" (
                   "MigrationId" varchar(150) not null primary key,
                   "ProductVersion" varchar(32) not null
               );
               """
            : $"""
               IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{historySchema}')
                   EXEC('CREATE SCHEMA [{historySchema}]');
               IF OBJECT_ID(N'[{historySchema}].[{HistoryTableName}]') IS NULL
                   CREATE TABLE [{historySchema}].[{HistoryTableName}] (
                       [MigrationId] nvarchar(150) NOT NULL PRIMARY KEY,
                       [ProductVersion] nvarchar(32) NOT NULL
                   );
               """;
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static async Task<bool> insertHistoryRowAsync(
        DbConnection connection,
        EfMigrationProvider provider,
        string historySchema,
        string migrationId,
        string productVersion,
        CancellationToken ct)
    {
        await using var command = connection.CreateCommand();

        var idParameter = command.CreateParameter();
        idParameter.ParameterName = "@id";
        idParameter.Value = migrationId;
        var versionParameter = command.CreateParameter();
        versionParameter.ParameterName = "@version";
        versionParameter.Value = productVersion;
        command.Parameters.Add(idParameter);
        command.Parameters.Add(versionParameter);

        command.CommandText = provider == EfMigrationProvider.PostgreSql
            ? $"""
               insert into "{historySchema}"."{HistoryTableName}" ("MigrationId", "ProductVersion")
               values (@id, @version)
               on conflict ("MigrationId") do nothing;
               """
            : $"""
               IF NOT EXISTS (SELECT 1 FROM [{historySchema}].[{HistoryTableName}] WHERE [MigrationId] = @id)
                   INSERT INTO [{historySchema}].[{HistoryTableName}] ([MigrationId], [ProductVersion])
                   VALUES (@id, @version);
               """;

        var affected = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        return affected > 0;
    }
}
