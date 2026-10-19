using System.Data.Common;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Sqlite;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     A migration's introspection can fail because an applier rewrote the catalog while it was being
///     read — a failure of nothing the reader did, where reading again succeeds. weasel#723.
/// </summary>
/// <remarks>
///     <para>
///         Firebird is the provider this exists for. Its catalog keeps a view's, routine's or trigger's
///         source in a BLOB and a row carries only the BLOB's id, so a concurrent <c>CREATE OR ALTER</c>
///         can collect the blob between the read of the row and the read of the blob: "BLOB not found".
///         It arrived as an intermittent failure of <c>racing_create_or_alter</c> on Firebird 5, and
///         <c>FirebirdMigrator</c> classified it as a real failure, because the error cannot come from a
///         DDL statement and the statement retry is where every other Firebird race is handled.
///     </para>
///     <para>
///         Which provider it is, though, is not what these tests pin. They pin the three things the
///         seam has to get right whatever the provider: a raced read is read again, a read that keeps
///         racing still gives up and reports, and a migrator that has not opted in does not re-read at
///         all. The third matters most — every provider but Firebird answers
///         <c>IsConcurrentIntrospectionFailure</c> false, and must make exactly one attempt as before.
///     </para>
///     <para>
///         Over an in-memory SQLite connection, so this is the real execution path with no containers,
///         following <c>introspection_renders_once</c>.
///     </para>
/// </remarks>
public class introspection_rereads_a_catalog_that_changed_under_it
{
    private static async Task<SqliteConnection> openConnectionAsync()
    {
        var conn = new SqliteConnection("Data Source=:memory:");
        await conn.OpenAsync();
        return conn;
    }

    private static CancellationToken ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task a_read_that_raced_an_applier_is_read_again()
    {
        await using var conn = await openConnectionAsync();
        var thing = new RacingSchemaObject("thing", failures: 1);

        var migration = await SchemaMigration.DetermineAsync(conn, new RetryingMigrator(), ct, thing);

        migration.Deltas.Count.ShouldBe(1);
        thing.Attempts.ShouldBe(2, "the raced read should have been made a second time");
    }

    /// <summary>
    ///     The bound. A catalog that never settles must still report, not spin.
    /// </summary>
    [Fact]
    public async Task a_read_that_keeps_racing_gives_up_and_reports_the_failure()
    {
        await using var conn = await openConnectionAsync();
        var thing = new RacingSchemaObject("thing", failures: int.MaxValue);

        var raced = await Should.ThrowAsync<RacedException>(() =>
            SchemaMigration.DetermineAsync(conn, new RetryingMigrator(), ct, thing));

        raced.ShouldNotBeNull();
        thing.Attempts.ShouldBe(3, "three attempts including the first, then the failure surfaces");
    }

    /// <summary>
    ///     The guard for every other provider: the default answer is "this is a real failure", so
    ///     nothing is read twice and nothing is retried into a slower version of itself.
    /// </summary>
    [Fact]
    public async Task a_migrator_that_has_not_opted_in_reads_once_and_reports()
    {
        await using var conn = await openConnectionAsync();
        var thing = new RacingSchemaObject("thing", failures: 1);

        await Should.ThrowAsync<RacedException>(() =>
            SchemaMigration.DetermineAsync(conn, new SqliteMigrator(), ct, thing));

        thing.Attempts.ShouldBe(1);
    }

    [Fact]
    public void no_provider_but_firebird_treats_an_introspection_failure_as_transient()
    {
        // Any exception at all: the others answer false unconditionally, which is the point.
        var boom = new RacedException();

        new SqliteMigrator().IsConcurrentIntrospectionFailure(boom).ShouldBeFalse();
        new Postgresql.PostgresqlMigrator().IsConcurrentIntrospectionFailure(boom).ShouldBeFalse();
        new SqlServer.SqlServerMigrator().IsConcurrentIntrospectionFailure(boom).ShouldBeFalse();
        new MySql.MySqlMigrator().IsConcurrentIntrospectionFailure(boom).ShouldBeFalse();
        new Oracle.OracleMigrator().IsConcurrentIntrospectionFailure(boom).ShouldBeFalse();

        // Firebird's own answer is false for anything that is not its BLOB error; the error number
        // itself is pinned against a live server in Weasel.Firebird.Tests.
        new Firebird.FirebirdMigrator().IsConcurrentIntrospectionFailure(boom).ShouldBeFalse();
    }

    private class RacedException: Exception
    {
        public RacedException(): base("the catalog changed under the read")
        {
        }
    }

    /// <summary>A SQLite migrator that calls <see cref="RacedException" /> a lost race.</summary>
    private class RetryingMigrator: SqliteMigrator
    {
        public override bool IsConcurrentIntrospectionFailure(Exception exception)
            => ExceptionChain.Flatten(exception).Any(x => x is RacedException);
    }

    /// <summary>
    ///     A real, executable schema object whose first <paramref name="failures" /> reads fail the way
    ///     a raced catalog read does: the query runs, and reading the result throws.
    /// </summary>
    private class RacingSchemaObject(string name, int failures): ISchemaObject
    {
        public int Attempts { get; private set; }

        public DbObjectName Identifier { get; } = new("main", name);

        public void ConfigureQueryCommand(DbCommandBuilder builder)
        {
            var param = builder.AddParameter(Identifier.Name).ParameterName;
            builder.Append($"select count(*) from sqlite_master where name = @{param};");
        }

        public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
        {
            Attempts++;

            await reader.ReadAsync(ct);
            await reader.GetFieldValueAsync<long>(0, ct);

            if (Attempts <= failures)
            {
                throw new RacedException();
            }

            return new SchemaObjectDelta(this, SchemaPatchDifference.None);
        }

        public void WriteCreateStatement(Migrator migrator, TextWriter writer) => throw new NotSupportedException();
        public void WriteDropStatement(Migrator rules, TextWriter writer) => throw new NotSupportedException();

        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;
        }
    }
}
