using System.Data.Common;
using JasperFx;
using JasperFx.Descriptors;

namespace Weasel.Core.Migrations;

/// <summary>
///     Marker interface for a known database with expected schema features
///     used to drive the migration process
/// </summary>
public interface IDatabase
{
    DatabaseDescriptor Describe();

    AutoCreate AutoCreate { get; }

    /// <summary>
    ///     The migrator rules for formatting SQL for this database
    /// </summary>
    Migrator Migrator { get; }

    /// <summary>
    ///     Identifying name for Weasel infrastructure and logging
    /// </summary>
    [Obsolete("Try to favor the Id value object going forward")]
    string Identifier { get; }

    DatabaseId Id { get; }

    /// <summary>
    /// In the case of multi-tenancy, this would hold one or more tenant ids
    /// </summary>
    List<string> TenantIds { get; }

    /// <summary>
    ///     Create all known features in order. Dependency relationships are assumed
    ///     to be reflected in the order of the feature array
    /// </summary>
    /// <returns></returns>
    IFeatureSchema[] BuildFeatureSchemas();

    /// <summary>
    ///     All referenced schema names by the known objects in this database
    /// </summary>
    /// <returns></returns>
    string[] AllSchemaNames();

    /// <summary>
    ///     Return an enumerable of all schema objects in dependency order
    /// </summary>
    /// <returns></returns>
    IEnumerable<ISchemaObject> AllObjects();

    /// <summary>
    ///     Determine a migration for a single IFeatureSchema
    /// </summary>
    /// <param name="group"></param>
    /// <param name="ct"> Cancellation token</param>
    /// <returns></returns>
    Task<SchemaMigration> CreateMigrationAsync(IFeatureSchema group, CancellationToken ct = default);

    /// <summary>
    ///     Return the SQL script for the entire database configuration as a single string
    /// </summary>
    /// <returns></returns>
    string ToDatabaseScript();

    /// <summary>
    ///     Write the SQL creation script to the supplied filename
    /// </summary>
    /// <param name="filename"></param>
    /// <param name="ct">Cancellation Token</param>
    /// <returns></returns>
    Task WriteCreationScriptToFileAsync(string filename, CancellationToken ct = default);

    /// <summary>
    ///     Write the SQL creation script by feature type to the supplied directory
    /// </summary>
    /// <param name="directory"></param>
    /// <param name="ct">Cancellation Token</param>
    Task WriteScriptsByTypeAsync(string directory, CancellationToken ct = default);

    /// <summary>
    ///     Determine a migration for the configured database against the actual database
    /// </summary>
    /// <param name="ct">Cancellation Token</param>
    /// <returns></returns>
    Task<SchemaMigration> CreateMigrationAsync(CancellationToken ct = default);

    /// <summary>
    ///     Apply all detected changes between configuration and the actual database to the database
    /// </summary>
    /// <param name="override">If supplied, this overrides the AutoCreate threshold of this database</param>
    /// <param name="reconnectionOptions">Reconnection policy options when the database is unavailable while applying database changes. If not supplied it will use default options.</param>
    /// <param name="ct">Cancellation Token</param>
    /// <returns></returns>
    Task<SchemaPatchDifference> ApplyAllConfiguredChangesToDatabaseAsync(
        AutoCreate? @override = null,
        ReconnectionOptions? reconnectionOptions = null,
        CancellationToken ct = default
    );

    /// <summary>
    ///     Assert that the existing database matches the configured database
    /// </summary>
    /// <param name="ct">Cancellation Token</param>
    /// <returns></returns>
    Task AssertDatabaseMatchesConfigurationAsync(CancellationToken ct = default);

    /// <summary>
    ///     Attempts to connect to the actual database and throws an exception
    ///     if that fails
    /// </summary>
    /// <param name="ct">Cancellation Token</param>
    /// <returns></returns>
    Task AssertConnectivityAsync(CancellationToken ct = default);

    /// <summary>
    ///     Release any pooled connections held for this database without disposing the database itself.
    ///     The default implementation is a no-op. Implementations that pool connections (see
    ///     <c>PostgresqlDatabase</c>) should close their idle connections here.
    ///     <para>
    ///     This exists for one-shot tooling — <c>db-apply</c> in particular — that walks many databases
    ///     sequentially (weasel#356). Each database owns its own pool, and nothing evicts a finished
    ///     database's idle connections until the connection idle lifetime expires, so an apply that only
    ///     ever needs one connection at a time drags a rolling tail of idle pools behind it and can push a
    ///     busy server over <c>max_connections</c>. Calling this after a database is done bounds peak usage
    ///     at the one pool actually in use.
    ///     </para>
    ///     <para>
    ///     This must remain safe to call on a database that will be used again afterward: it releases idle
    ///     connections, it does not tear the database's connectivity down.
    ///     </para>
    /// </summary>
    /// <param name="ct">Cancellation Token</param>
    ValueTask ReleaseConnectionPoolAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
}

public static class DatabaseExtensions
{
    /// <summary>
    ///     Generate a migration, and write the results to the specified file name
    /// </summary>
    /// <param name="database"></param>
    /// <param name="filename"></param>
    public static async Task WriteMigrationFileAsync(
        this IDatabase database,
        string filename,
        CancellationToken ct = default
    )
    {
        var migration = await database.CreateMigrationAsync(ct).ConfigureAwait(false);
        await database.Migrator.WriteMigrationFileAsync(filename, migration, ct).ConfigureAwait(false);
    }

    public static Task<SchemaMigration> CreateMigrationAsync(
        this IDatabase database,
        Type featureType,
        CancellationToken ct = default
    )
    {
        var feature = database.BuildFeatureSchemas().FirstOrDefault(x => x.StorageType == featureType);
        if (feature == null)
        {
            throw new ArgumentOutOfRangeException(nameof(featureType),
                $"Type '{featureType.FullName}' is an unknown storage type");
        }

        return database.CreateMigrationAsync(feature, ct);
    }

    /// <summary>
    ///     Apply all detected changes to the database, retrying a bounded number of times with an
    ///     exponential, jittered backoff when the attempt fails with a transient connection failure as
    ///     judged by <see cref="Migrator.IsTransientConnectionFailure" /> — a server at its connection
    ///     ceiling, most notably.
    ///     <para>
    ///     Intended for one-shot migration tooling against a live server (weasel#356): a connection
    ///     refusal there is near-certainly transient ambient pressure, and failing the whole job on the
    ///     first refusal blocks a deployment for something that would have succeeded a second later. The
    ///     pool is released between attempts so a retry starts from a clean slate rather than re-using a
    ///     pool full of connections the server just refused.
    ///     </para>
    /// </summary>
    /// <param name="database"></param>
    /// <param name="maxAttempts">The total number of attempts, including the first. Default is 3, i.e. two retries.</param>
    /// <param name="baseDelayMs">Delay before the first retry. Doubles per subsequent retry, capped at <see cref="MaxRetryDelayMs" />. Default is 1000ms.</param>
    /// <param name="ct">Cancellation Token</param>
    public static async Task<SchemaPatchDifference> ApplyAllConfiguredChangesWithRetriesAsync(
        this IDatabase database,
        int maxAttempts = 3,
        int baseDelayMs = 1000,
        CancellationToken ct = default
    )
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await database.ApplyAllConfiguredChangesToDatabaseAsync(ct: ct).ConfigureAwait(false);
            }
            catch (Exception e) when (attempt < maxAttempts && database.Migrator.IsTransientConnectionFailure(e))
            {
                // The refused attempt may still have left connections in this database's pool. Drop them
                // before backing off so the retry is not competing against our own idle connections.
                await database.ReleaseConnectionPoolAsync(ct).ConfigureAwait(false);

                await Task.Delay(BackoffDelayMs(attempt, baseDelayMs), ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    ///     Ceiling on a single backoff delay. Doubling is only useful up to a point: a caller who asked for
    ///     many attempts wants to keep trying while a connection storm clears, not to disappear for an hour
    ///     on one sleep.
    /// </summary>
    internal const int MaxRetryDelayMs = 30_000;

    /// <summary>
    ///     Exponential backoff for the given (1-based) attempt, jittered so that several appliers refused at
    ///     the same moment don't march back in lockstep. Computed in <see cref="long" /> and clamped: the
    ///     naive <c>baseDelayMs * 2^(attempt-1)</c> overflows <see cref="int" /> around attempt 32 and goes
    ///     negative, which would throw out of <c>Task.Delay</c> and lose the connection failure being retried.
    ///     Pure so the clamping is unit-testable.
    /// </summary>
    internal static int BackoffDelayMs(int attempt, int baseDelayMs)
    {
        var doublings = Math.Min(attempt - 1, 30);
        var delay = Math.Min((long)baseDelayMs << doublings, MaxRetryDelayMs);

        return (int)delay + Random.Shared.Next(0, 100);
    }
}

public interface IDatabase<T>: IDatabase, IConnectionSource<T> where T : DbConnection
{
}

public interface IMigrationLogger
{
    void SchemaChange(string sql);

    void OnFailure(DbCommand command, Exception ex);
}

public class DefaultMigrationLogger: IMigrationLogger
{
    public void SchemaChange(string sql)
    {
        Console.WriteLine(sql);
    }

    public void OnFailure(DbCommand command, Exception ex)
    {
        throw ex;
    }
}
