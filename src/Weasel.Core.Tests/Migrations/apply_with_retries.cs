using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using JasperFx;
using JasperFx.Descriptors;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Core.Tests.Migrations;

/// <summary>
///     Coverage for <see cref="DatabaseExtensions.ApplyAllConfiguredChangesWithRetriesAsync" /> (weasel#356):
///     a one-shot apply against a server at its connection ceiling should back off and retry rather than
///     fail the whole job on the first refusal, and should not retry a genuine migration failure.
/// </summary>
public class apply_with_retries
{
    private static Task<SchemaPatchDifference> apply(FakeDatabase database) =>
        database.ApplyAllConfiguredChangesWithRetriesAsync(baseDelayMs: 1);

    [Fact]
    public async Task no_retry_when_the_first_attempt_succeeds()
    {
        var database = new FakeDatabase();

        (await apply(database)).ShouldBe(SchemaPatchDifference.Update);

        database.Attempts.ShouldBe(1);
        database.PoolReleases.ShouldBe(0);
    }

    [Fact]
    public async Task retries_a_transient_connection_failure_until_it_succeeds()
    {
        var database = new FakeDatabase { FailuresBeforeSuccess = 2, FailureIsTransient = true };

        (await apply(database)).ShouldBe(SchemaPatchDifference.Update);

        database.Attempts.ShouldBe(3);
    }

    [Fact]
    public async Task releases_the_pool_before_each_retry()
    {
        var database = new FakeDatabase { FailuresBeforeSuccess = 2, FailureIsTransient = true };

        await apply(database);

        // Once per failed attempt -- the refused attempt's connections should not be left in the pool
        // competing with the retry.
        database.PoolReleases.ShouldBe(2);
    }

    [Fact]
    public async Task gives_up_after_the_maximum_number_of_attempts()
    {
        var database = new FakeDatabase { FailuresBeforeSuccess = 99, FailureIsTransient = true };

        await Should.ThrowAsync<TimeoutException>(() =>
            database.ApplyAllConfiguredChangesWithRetriesAsync(maxAttempts: 4, baseDelayMs: 1));

        database.Attempts.ShouldBe(4);
    }

    [Fact]
    public async Task does_not_retry_a_failure_that_is_not_a_transient_connection_failure()
    {
        var database = new FakeDatabase { FailuresBeforeSuccess = 1, FailureIsTransient = false };

        await Should.ThrowAsync<TimeoutException>(() => apply(database));

        // A DDL failure is not transient. Retrying it only delays a failure the operator needs to see.
        database.Attempts.ShouldBe(1);
        database.PoolReleases.ShouldBe(0);
    }

    [Fact]
    public async Task a_failure_to_release_the_pool_does_not_defeat_the_retry()
    {
        // Release does real driver work now that every provider implements it, so it can fail. If that
        // failure escaped, it would replace the transient exception being retried and turn a recoverable
        // refusal into a hard failure.
        var database = new FakeDatabase
        {
            FailuresBeforeSuccess = 1, FailureIsTransient = true, ReleaseThrows = true
        };

        (await apply(database)).ShouldBe(SchemaPatchDifference.Update);

        database.Attempts.ShouldBe(2);
    }

    [Fact]
    public void the_backoff_doubles_per_attempt()
    {
        DatabaseExtensions.BackoffDelayMs(1, 1000).ShouldBeInRange(1000, 1100);
        DatabaseExtensions.BackoffDelayMs(2, 1000).ShouldBeInRange(2000, 2100);
        DatabaseExtensions.BackoffDelayMs(3, 1000).ShouldBeInRange(4000, 4100);
    }

    [Fact]
    public void the_backoff_is_clamped_rather_than_overflowing()
    {
        // Naive `baseDelayMs * 2^(attempt-1)` in an int goes negative around attempt 32 and would throw
        // out of Task.Delay, losing the connection failure being retried.
        foreach (var attempt in new[] { 20, 32, 40, 1000, int.MaxValue })
        {
            DatabaseExtensions.BackoffDelayMs(attempt, 1000)
                .ShouldBeInRange(0, DatabaseExtensions.MaxRetryDelayMs + 100);
        }
    }

    #region Fakes

    internal class FakeDatabase: IDatabase
    {
        public int Attempts { get; private set; }
        public int PoolReleases { get; private set; }
        public int FailuresBeforeSuccess { get; set; }
        public bool FailureIsTransient { get; set; }
        public bool ReleaseThrows { get; set; }

        public Task<SchemaPatchDifference> ApplyAllConfiguredChangesToDatabaseAsync(
            AutoCreate? @override = null,
            ReconnectionOptions? reconnectionOptions = null,
            CancellationToken ct = default
        )
        {
            Attempts++;
            if (Attempts <= FailuresBeforeSuccess)
            {
                // The exception type is irrelevant -- the Migrator decides what is transient.
                throw new TimeoutException("refused");
            }

            return Task.FromResult(SchemaPatchDifference.Update);
        }

        public ValueTask ReleaseConnectionPoolAsync(CancellationToken ct = default)
        {
            PoolReleases++;
            if (ReleaseThrows) throw new InvalidOperationException("the pool could not be cleared");

            return ValueTask.CompletedTask;
        }

        public Migrator Migrator => new FakeMigrator(FailureIsTransient);

        public DatabaseDescriptor Describe() => new();
        public AutoCreate AutoCreate => AutoCreate.CreateOrUpdate;
        public string Identifier => "fake";
        public DatabaseId Id { get; } = new("server", "database");
        public List<string> TenantIds { get; } = new();
        public IFeatureSchema[] BuildFeatureSchemas() => throw new NotSupportedException();
        public string[] AllSchemaNames() => throw new NotSupportedException();
        public IEnumerable<ISchemaObject> AllObjects() => throw new NotSupportedException();

        public Task<SchemaMigration> CreateMigrationAsync(IFeatureSchema group, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public string ToDatabaseScript() => throw new NotSupportedException();

        public Task WriteCreationScriptToFileAsync(string filename, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task WriteScriptsByTypeAsync(string directory, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<SchemaMigration> CreateMigrationAsync(CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task AssertDatabaseMatchesConfigurationAsync(CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task AssertConnectivityAsync(CancellationToken ct = default) => throw new NotSupportedException();
    }

    internal class FakeMigrator: Migrator
    {
        private readonly bool _isTransient;

        public FakeMigrator(bool isTransient): base("fake")
        {
            _isTransient = isTransient;
        }

        public override bool IsTransientConnectionFailure(Exception exception) => _isTransient;

        public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null) =>
            throw new NotSupportedException();

        public override bool MatchesConnection(DbConnection connection) => throw new NotSupportedException();
        public override IDatabaseProvider Provider => throw new NotSupportedException();
        public override ITable CreateTable(DbObjectName identifier) => throw new NotSupportedException();

        public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep) =>
            throw new NotSupportedException();

        public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer) =>
            throw new NotSupportedException();

        public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer) =>
            throw new NotSupportedException();

        protected override Task executeDelta(
            SchemaMigration migration,
            DbConnection conn,
            AutoCreate autoCreate,
            IMigrationLogger logger,
            CancellationToken ct = default
        ) => throw new NotSupportedException();

        public override string ToExecuteScriptLine(string scriptName) => throw new NotSupportedException();
        public override void AssertValidIdentifier(string name) => throw new NotSupportedException();

        public override string GenerateDeleteAllSql(IReadOnlyList<DbObjectName> tables, bool resetIdentity = true) =>
            throw new NotSupportedException();
    }

    #endregion
}
