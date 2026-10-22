using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using Npgsql;
using Nuke.Common;
using Serilog;

// One supervised target per CI test project, the same split the workflows already make: each
// workflow builds, then runs exactly one of these (`./build.sh TestPostgresql --framework net9.0`).
// None depends on Compile -- the workflow's own restore and build steps stay as they were, and the
// target fails loudly if the test host it needs was not built.
//
// Worker counts and isolation, per suite. The committed count is clamped to half the machine's cores
// (2 on a GitHub-hosted runner) unless --test-workers overrides it.
//
//   Postgres    4 workers, a database per lane. Weasel's Postgres tests group classes into xUnit
//               collections by the SCHEMA they reset ([Collection("partitions")] on six classes that
//               all reset the "partitions" schema), and xUnit runs a collection's classes one after
//               another. The supervisor partitions by class, not by collection -- it cannot see
//               collections -- so two classes resetting the same schema can land in two lanes.
//               Per-lane SCHEMAS cannot fix that, because the schema names are the tests' own
//               literals; the database has to be the unit of isolation.
//   SQL Server  4 workers, a database per lane on the one server, for the same reason: every class
//               resets a hard-coded schema. The CI connection string names no catalog (master);
//               each lane gets weasel_w{lane}.
//   Firebird    4 workers, no plumbing. Every test class already creates a database FILE of its own,
//               named for the class (IntegrationContext), and no two classes share a name or a file
//               -- checked when this was written: the only cross-class name, "server_version", is
//               unused. Isolation by class is exactly what the supervisor's partitioning preserves.
//   MySQL       1 worker. A MySQL schema IS a database, a server-scoped name, and the suite's are
//               literals shared across classes; isolating lanes would mean renaming them in the
//               tests. The suite runs in seconds, so a second lane would buy nothing anyway.
//   Oracle      1 worker. A schema is a USER, scoped to the pluggable database, and the tests create
//               and drop them by fixed name; lane isolation would need a PDB per lane.
//   SQLite, Core, EF Core, CommandLine
//               1 worker. Seconds each; parallel lanes would cost more in process start than they save.
//               CommandLine is Postgres-backed despite the name -- it shares Weasel.Postgresql.Tests'
//               ConnectionSource and drops real schemas -- so it runs as a second step of the Postgres
//               workflow rather than the Core one. At one worker it needs no lane isolation; it would,
//               at more than one, because its classes share [Collection("integration")] and reset
//               schemas by literal name, which is the same reason the Postgres suite gets a database
//               per lane.
partial class Build
{
    Target TestCore => _ => _.Executes(() => RunSupervised("Weasel.Core.Tests"));

    Target TestSqlite => _ => _.Executes(() => RunSupervised("Weasel.Sqlite.Tests"));

    Target TestPostgresql => _ => _.Executes(() =>
        RunSupervised("Weasel.Postgresql.Tests", workers: 4, isolation: PostgresLanes));

    Target TestSqlServer => _ => _.Executes(() =>
        RunSupervised("Weasel.SqlServer.Tests", workers: 4, isolation: SqlServerLanes));

    Target TestFirebird => _ => _.Executes(() => RunSupervised("Weasel.Firebird.Tests", workers: 4));

    Target TestMySql => _ => _.Executes(() => RunSupervised("Weasel.MySql.Tests"));

    Target TestOracle => _ => _.Executes(() => RunSupervised("Weasel.Oracle.Tests"));

    Target TestEfCore => _ => _.Executes(() => RunSupervised("Weasel.EntityFrameworkCore.Tests"));

    // weasel#721: run by no workflow at all until now, so its 57 tests could rot unseen.
    Target TestCommandLine => _ => _.Executes(() => RunSupervised("Weasel.CommandLine.Tests"));

    // ─── Postgres ─────────────────────────────────────────────────────────

    const string PostgresVariable = "weasel_postgresql_testing_database";

    /// <summary>
    /// The suite's own default (src/Weasel.Postgresql.Tests/ConnectionSource.cs). weasel_testing, not
    /// marten_testing: lanes are named <c>{database}_w{n}</c>, and on a Postgres shared with Marten
    /// its lanes are marten_testing_w{n} (weasel#733).
    /// </summary>
    const string PostgresDefault =
        "Host=localhost;Port=5432;Database=weasel_testing;Username=postgres;password=postgres;SSL Mode=Disable";

    static string PostgresConnectionString =>
        Environment.GetEnvironmentVariable(PostgresVariable) is { Length: > 0 } configured ? configured : PostgresDefault;

    /// <summary>
    /// The configured database's name with the lane appended -- derived from the configured
    /// connection string rather than written down, so a renamed CI database cannot leave a lane
    /// pointing at a name nothing provisioned.
    /// </summary>
    static string postgresLaneDatabase(int lane)
        => $"{new NpgsqlConnectionStringBuilder(PostgresConnectionString).Database}_w{lane}";

    static string postgresLaneConnectionString(int lane)
        => new NpgsqlConnectionStringBuilder(PostgresConnectionString) { Database = postgresLaneDatabase(lane) }
            .ConnectionString;

    static LaneIsolation PostgresLanes => new(
        "a Postgres database per lane",
        lane =>
        {
            var database = postgresLaneDatabase(lane);

            ensurePostgresDatabase(database);
        },
        lane => new Dictionary<string, string> { [PostgresVariable] = postgresLaneConnectionString(lane) });

    /// <summary>
    /// Creates <paramref name="database"/> on the configured server unless it exists. Connects to the
    /// stock <c>postgres</c> maintenance database rather than the configured one, which may not exist
    /// yet either: on a server another repository started (Marten's, whose only database is
    /// marten_testing) nothing has created weasel_testing.
    /// </summary>
    static void ensurePostgresDatabase(string database)
    {
        // CREATE DATABASE cannot run inside a transaction or on a multiplexed pooled connection.
        using var conn = new NpgsqlConnection(
            new NpgsqlConnectionStringBuilder(PostgresConnectionString) { Database = "postgres", Pooling = false }
                .ConnectionString);
        conn.Open();

        using var check = conn.CreateCommand();
        check.CommandText = "select 1 from pg_database where datname = @name";
        check.Parameters.AddWithValue("name", database);
        if (check.ExecuteScalar() is not null) return;

        Log.Information("  provisioning database {Database}", database);
        using var create = conn.CreateCommand();
        // An identifier, not a value, so it cannot be a parameter. Generated by the build, never input.
        create.CommandText = $"CREATE DATABASE \"{database}\"";
        create.ExecuteNonQuery();
    }

    // ─── SQL Server ───────────────────────────────────────────────────────

    const string SqlServerVariable = "weasel_sqlserver_testing_database";

    /// <summary>The suite's own default (src/Weasel.SqlServer.Tests/ConnectionSource.cs).</summary>
    const string SqlServerDefault =
        "Server=localhost,1435;User Id=sa;Password=P@55w0rd;Timeout=5;MultipleActiveResultSets=True;Initial Catalog=master;Encrypt=False";

    static string SqlServerConnectionString =>
        Environment.GetEnvironmentVariable(SqlServerVariable) is { Length: > 0 } configured ? configured : SqlServerDefault;

    static string sqlServerLaneConnectionString(string server, int lane)
        => new SqlConnectionStringBuilder(server) { InitialCatalog = $"weasel_w{lane}" }.ConnectionString;

    static LaneIsolation SqlServerLanes => sqlServerLanes(SqlServerConnectionString);

    /// <summary>
    /// A database per lane on the SQL Server <paramref name="server"/> names. A parameter rather than
    /// the variable alone because the local gate's optional 2019 and 2022 legs run the same suite
    /// against servers of their own (LocalGate.cs).
    /// </summary>
    static LaneIsolation sqlServerLanes(string server) => new(
        "a SQL Server database per lane",
        lane =>
        {
            var database = $"weasel_w{lane}";
            using var conn = new SqlConnection(
                new SqlConnectionStringBuilder(server) { InitialCatalog = "master" }.ConnectionString);
            conn.Open();

            using var create = conn.CreateCommand();
            // An identifier, not a value, so it cannot be a parameter. Generated above, never input.
            create.CommandText = $"if DB_ID('{database}') is null begin CREATE DATABASE [{database}] end";
            create.ExecuteNonQuery();
        },
        lane => new Dictionary<string, string> { [SqlServerVariable] = sqlServerLaneConnectionString(server, lane) });
}
