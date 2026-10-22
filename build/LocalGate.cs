using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bobcat.Ledger;
using Bobcat.Supervisor;
using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Services;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Nuke.Common;
using Nuke.Common.IO;
using Nuke.Common.Tools.DotNet;
using Oracle.ManagedDataAccess.Client;
using Serilog;
using static Nuke.Common.Tools.DotNet.DotNetTasks;

// The local commit-and-push gate (weasel#733), on Bobcat.Supervisor's Gate (bobcat#729,
// https://github.com/JasperFx/bobcat/blob/main/docs/local-gates.md): every suite the eight
// ci-build-*.yml workflows run, as one command, against whatever containers are already up.
//
//   ./build.sh LocalGate                          newest framework (net10.0), Debug
//   ./build.sh LocalGate --framework all          every framework, the CI matrix
//   ./build.sh LocalGate --gate-workers 6         a smaller worker budget
//   ./build.sh LocalGate --gate-legs sqlserver2019,firebird3
//
// Each (project, framework) is one unit, run by its own supervisor with the same policy its CI target
// uses (applyWeaselPolicy), longest first. A unit holds the TRACKS of the servers it touches, and a
// track is exclusive, so no two suites ever share a server at once: one track per external engine.
// Weasel.CommandLine.Tests is on the Postgres track because it drops real schemas on that server;
// Weasel.EntityFrameworkCore.Tests holds every engine it touches. Core and SQLite need no server and
// are limited by the worker budget alone.
//
// Configuration: Debug, the Nuke default locally. CI builds Core and SQLite in Release and the rest
// in Debug; one gate has one configuration and the suites do not depend on it, so the gate normalises
// to Debug, which is what an IDE has usually built already. --configuration Release overrides it.
//
// The gate never starts or stops a container. `docker compose up -d` (ports overridable through
// WEASEL_*_PORT, see docker-compose.yml) does that; the gate reads the same weasel_*_testing_database
// variables the suites do, and every track's preflight connects to its server before anything builds.
partial class Build
{
    [Parameter("LocalGate: worker processes across every unit at once. Default: half the cores")]
    readonly int? GateWorkers;

    [Parameter("LocalGate: optional CI legs to run after the gate, comma-separated: " +
               "sqlserver2019, sqlserver2022, firebird3, firebird4. Each needs its own container")]
    readonly string GateLegs;

    AbsolutePath DocSamplesPath => RootDirectory / "src" / "DocSamples" / "DocSamples.csproj";

    static AbsolutePath testProject(string name) => RootDirectory / "src" / name / $"{name}.csproj";

    /// <summary>
    /// The docs samples (src/DocSamples, the mdsnippets sources) compile. publish-docs.yml builds them
    /// when the docs are published; the gate builds them with the suites, so a broken sample fails the
    /// push rather than the docs deployment.
    /// </summary>
    Target CompileDocSamples => _ => _
        .Description("Compile src/DocSamples, the docs' code samples, for every framework")
        .Executes(() =>
        {
            DotNetBuild(s => s
                .SetProjectFile(DocSamplesPath)
                .SetConfiguration(Configuration));
        });

    Target LocalGate => _ => _
        .Description("Every CI test suite as one local gate (weasel#733). --framework all, --gate-workers N, --gate-legs ...")
        .Executes(async () =>
        {
            // Never a lingering MSBuild node that outlives the gate, and holds the box after it.
            Environment.SetEnvironmentVariable("MSBUILDDISABLENODEREUSE", "1");

            var frameworks = FrameworkSelection.Parse(Framework);
            var exitCodes = new List<(string Name, int ExitCode)>();

            var gate = new GateDeclaration
            {
                Tracks =
                {
                    postgresTrack(), sqlServerTrack("sqlserver", SqlServerConnectionString), mySqlTrack(),
                    oracleTrack(), firebirdTrack("firebird", FirebirdConnectionString)
                },
                Frameworks = frameworks,
                Configuration = Configuration,
                Build = buildForGate,
                Configure = (unit, supervisor) =>
                    applyWeaselPolicy(supervisor, previousLedger(unit.Project.Name).KnownDurations()),
                Log = message => Log.Information("{Message:l}", message)
            };
            if (GateWorkers is { } budget) gate.WorkerBudget = budget;

            gate.Project(testProject("Weasel.Postgresql.Tests"), workers: postgresLanesAllowed())
                .On("postgres")
                .LaunchingWith(unit => lanesFor(unit, PostgresLanes));

            // Postgres-backed despite the name: it shares Weasel.Postgresql.Tests' ConnectionSource and
            // drops real schemas, in the configured database and in `postgres`. One worker, as in CI.
            gate.Project(testProject("Weasel.CommandLine.Tests")).On("postgres");

            gate.Project(testProject("Weasel.SqlServer.Tests"), workers: 4)
                .On("sqlserver")
                .LaunchingWith(unit => lanesFor(unit, SqlServerLanes));

            // Every test class has a database file of its own; isolation by class needs no plumbing.
            gate.Project(testProject("Weasel.Firebird.Tests"), workers: 4).On("firebird");

            gate.Project(testProject("Weasel.MySql.Tests")).On("mysql");
            gate.Project(testProject("Weasel.Oracle.Tests")).On("oracle");

            // EF Core touches every engine it has a provider for. Its Oracle and MySQL tests are
            // compiled out on net10.0 (no EF Core 10 provider yet), so it holds those two tracks only
            // when the gate runs an earlier framework -- holding them for nothing would serialise it
            // behind the Oracle suite for no reason.
            var efCore = gate.Project(testProject("Weasel.EntityFrameworkCore.Tests"))
                .On("postgres", "sqlserver", "firebird")
                .LaunchingWith(unit => new MtpWorkerFactory(unit.Executable, efCoreEnvironment()));
            if (frameworks.Select(efCore.DeclaredFrameworks).Any(f => f != "net10.0")) efCore.On("mysql", "oracle");

            gate.Project(testProject("Weasel.Core.Tests"));
            gate.Project(testProject("Weasel.Sqlite.Tests"));

            var results = await gate.Run();
            recordGate(results, ledgerJobName);
            exitCodes.Add(("gate", results.ExitCode));

            // The optional legs, each against a container of its own, after the gate and built by it.
            // A leg is another server for a suite the gate already ran, and a declaration holds a
            // project once, so legs run in rounds of at most one leg per project.
            foreach (var round in legRounds())
            {
                var legGate = new GateDeclaration
                {
                    Frameworks = frameworks,
                    Configuration = Configuration,
                    SkipBuild = results.AbortReason is null,
                    Build = buildForGate,
                    DurationsPath = null,
                    Configure = (unit, supervisor) =>
                        applyWeaselPolicy(supervisor, previousLedger(unit.Project.Name).KnownDurations()),
                    Log = message => Log.Information("{Message:l}", message)
                };
                if (GateWorkers is { } legBudget) legGate.WorkerBudget = legBudget;

                foreach (var leg in round)
                {
                    legGate.Tracks.Add(leg.Track());
                    legGate.Project(testProject(leg.Project), leg.Workers).On(leg.Name).LaunchingWith(leg.Factory);
                }

                var name = string.Join("+", round.Select(x => x.Name));
                Log.Information("{Message:l}", $"=== Gate legs: {name} ===");
                var legResults = await legGate.Run();
                recordGate(legResults, $"local-{name}");
                exitCodes.Add((name, legResults.ExitCode));
            }

            if (exitCodes.Count > 1)
            {
                Log.Information("{Message:l}", "Gate and legs: " +
                                               string.Join(", ", exitCodes.Select(x => $"{x.Name} exit {x.ExitCode}")));
            }

            var worst = exitCodes.Max(x => x.ExitCode);
            Assert.True(worst == 0, $"The local gate exited {worst}; the report above names every unit " +
                                    $"(artifacts/test-ledger/{fileNameSafe(ledgerJobName)}.gate.report.txt)");
        });

    /// <summary>
    /// The gate's build step (GateDeclaration.Build), in place of Bobcat's default. The default runs
    /// <c>dotnet build &lt;solution&gt; -f &lt;tfm&gt;</c>, which fails on any project in the solution that does
    /// not target that framework (bobcat#774, found in Marten), and builds every project in the
    /// solution besides. This restores the solution once and then builds exactly the test projects the
    /// gate runs, plus the docs samples, for each framework the gate runs.
    /// </summary>
    Task buildForGate(GateBuild request, CancellationToken ct)
    {
        DotNetRestore(s => s.SetProjectFile(SolutionPath));

        foreach (var framework in request.Frameworks)
        {
            foreach (var project in request.Projects.Select(p => p.Path).Append((string)DocSamplesPath))
            {
                ct.ThrowIfCancellationRequested();
                DotNetBuild(s => s
                    .SetProjectFile(project)
                    .SetFramework(framework)
                    .SetConfiguration(request.Configuration)
                    .EnableNoRestore()
                    .SetVerbosity(DotNetVerbosity.minimal));
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// A worker factory that gives every lane a database of its own, provisioned before the first
    /// worker launches: the same isolation the suite's CI target uses (TestTargets.cs). Provisions
    /// the unit's declared lanes, which is never fewer than the budget lets it run.
    /// </summary>
    static IWorkerFactory lanesFor(GateUnit unit, LaneIsolation isolation)
    {
        for (var lane = 0; lane < unit.Workers; lane++) isolation.Provision(lane);
        return new MtpWorkerFactory(unit.Executable) { EnvironmentFor = context => isolation.EnvironmentFor(context.Lane) };
    }

    /// <summary>
    /// EF Core's SQL Server tests create the database their connection string names, and outside the
    /// gate their default names weasel_testing. A configured SQL Server string usually names master
    /// (the SQL Server suite's own default), so give EF Core the same server with weasel_testing.
    /// </summary>
    static IReadOnlyDictionary<string, string> efCoreEnvironment() => new Dictionary<string, string>
    {
        [SqlServerVariable] = new SqlConnectionStringBuilder(SqlServerConnectionString) { InitialCatalog = "weasel_testing" }
            .ConnectionString
    };

    /// <summary>
    /// Every unit's ledger and run report, as the CI targets keep them, and the gate's own report as
    /// text: all under artifacts/test-ledger/, so the next run balances lanes on this one.
    /// </summary>
    void recordGate(GateResults results, string jobName)
    {
        foreach (var unit in results.Units.Where(u => u.Results is not null))
        {
            var project = unit.Unit.Project.Name;
            var framework = unit.Unit.Framework;
            var ledgerPath = RunArtifactsDirectory / fileNameSafe($"{jobName}.{project}.{framework}.ledger.json");
            var runId = $"local-{DateTimeOffset.UtcNow:yyyyMMddHHmmss}/{jobName}/{project}/{framework}";
            recordLedger(project, framework, unit.Results, runId, DateTimeOffset.UtcNow - unit.Duration,
                previousLedger(project), ledgerPath);

            try
            {
                File.WriteAllText(RunArtifactsDirectory / fileNameSafe($"{jobName}.{project}.{framework}.report.json"),
                    RunReport.ToJson(unit.Results));
            }
            catch (Exception e)
            {
                Log.Warning(e, "Could not write the run report for {Unit}", unit.Unit.Name);
            }
        }

        try
        {
            RunArtifactsDirectory.CreateDirectory();
            File.WriteAllText(RunArtifactsDirectory / fileNameSafe($"{jobName}.gate.report.txt"), results.ToText());
        }
        catch (Exception e)
        {
            Log.Warning(e, "Could not write the gate report");
        }
    }

    // ─── Tracks: one per external server, each preflighted by a real connection ───────────────

    /// <summary>
    /// Also ensures the configured database exists: the suites and CommandLine use it directly, the
    /// lanes are created beside it, and on a server another repository started it may not exist.
    /// </summary>
    static Track postgresTrack()
    {
        var builder = new NpgsqlConnectionStringBuilder(PostgresConnectionString);
        var host = builder.Host?.Split(',')[0].Split(':')[0] ?? "localhost";

        return Track.Listening("postgres", host, builder.Port)
            .Check($"postgres: connect as {builder.Username}, ensure database {builder.Database}", () =>
            {
                ensurePostgresDatabase(builder.Database!);
            });
    }

    /// <summary>
    /// Connections one Postgres lane holds at its peak. Measured for weasel#733 by sampling
    /// pg_stat_activity every 200ms through the gate: the Postgres suite on 4 lanes peaked at 89 client
    /// connections, in its first seconds, against the stock max_connections of 100. A lane is budgeted
    /// 25, and 10 are left for superuser and maintenance connections.
    /// </summary>
    const int PostgresConnectionsPerLane = 25;

    /// <summary>
    /// The Postgres suite's lanes, clamped to what the server's max_connections allows (Marten hit
    /// <c>53300: too many clients</c> sharing a stock server). The Postgres track is exclusive, so
    /// one unit at a time draws on the server, and the Postgres suite is the busiest: 4 lanes need
    /// about 110 connections, so a stock server (100) runs 3 and docker-compose.yml's (200) runs 4.
    /// When the server does not answer, the committed 4, and the track's preflight reports it.
    /// </summary>
    static int postgresLanesAllowed()
    {
        const int committed = 4;
        try
        {
            using var conn = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(PostgresConnectionString)
                { Database = "postgres", Pooling = false, Timeout = 5 }.ConnectionString);
            conn.Open();
            using var command = conn.CreateCommand();
            command.CommandText = "show max_connections";
            var max = int.Parse((string)command.ExecuteScalar()!);

            var lanes = Math.Clamp((max - 10) / PostgresConnectionsPerLane, 1, committed);
            if (lanes < committed)
            {
                Log.Warning("Postgres max_connections is {Max}: running Weasel.Postgresql.Tests on {Lanes} lane(s), not " +
                            "{Committed}, to stay under it (about {PerLane} connections a lane). docker-compose.yml's " +
                            "Postgres allows 200", max, lanes, committed, PostgresConnectionsPerLane);
            }

            return lanes;
        }
        catch (Exception)
        {
            return committed;
        }
    }

    static Track sqlServerTrack(string name, string connectionString)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        var (host, port) = sqlServerEndpoint(builder.DataSource);

        return Track.Listening(name, host, port)
            .Check($"{name}: connect to master and select 1", () =>
            {
                using var conn = new SqlConnection(
                    new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "master", Pooling = false }.ConnectionString);
                conn.Open();
                using var command = conn.CreateCommand();
                command.CommandText = "select 1";
                command.ExecuteScalar();
            });
    }

    static (string Host, int Port) sqlServerEndpoint(string dataSource)
    {
        var source = dataSource.StartsWith("tcp:", StringComparison.OrdinalIgnoreCase) ? dataSource[4..] : dataSource;
        var parts = source.Split(',');
        var host = parts[0].Split('\\')[0];
        return (host is "." or "(local)" ? "localhost" : host, parts.Length > 1 ? int.Parse(parts[1]) : 1433);
    }

    // ─── MySQL, Oracle, Firebird: the suites' own variables and defaults ────────────────────────

    const string MySqlVariable = "weasel_mysql_testing_database";

    /// <summary>The suite's own default (src/Weasel.MySql.Tests/ConnectionSource.cs).</summary>
    const string MySqlDefault = "Server=localhost;Port=3306;Database=weasel_testing;Uid=weasel;Pwd=P@55w0rd;";

    static string MySqlConnectionString =>
        Environment.GetEnvironmentVariable(MySqlVariable) is { Length: > 0 } configured ? configured : MySqlDefault;

    static Track mySqlTrack()
    {
        var builder = new MySqlConnectionStringBuilder(MySqlConnectionString);
        return Track.Listening("mysql", builder.Server, (int)builder.Port)
            .Check($"mysql: connect to {builder.Database} as {builder.UserID}", () =>
            {
                using var conn = new MySqlConnection(new MySqlConnectionStringBuilder(MySqlConnectionString) { Pooling = false }
                    .ConnectionString);
                conn.Open();
                using var command = conn.CreateCommand();
                command.CommandText = "select 1";
                command.ExecuteScalar();
            });
    }

    const string OracleVariable = "weasel_oracle_testing_database";

    /// <summary>The suite's own default (src/Weasel.Oracle.Tests/ConnectionSource.cs).</summary>
    const string OracleDefault = "Data Source=localhost:1521/FREEPDB1;User Id=weasel;Password=P@55w0rd;";

    static string OracleConnectionString =>
        Environment.GetEnvironmentVariable(OracleVariable) is { Length: > 0 } configured ? configured : OracleDefault;

    /// <summary>
    /// The listener accepts connections well before the pluggable database opens -- a freshly started
    /// gvenzl/oracle-free takes a minute or more -- so only a real connection says the suite can run.
    /// </summary>
    static Track oracleTrack()
    {
        var builder = new OracleConnectionStringBuilder(OracleConnectionString);
        var address = builder.DataSource.Split('/')[0].Split(':');
        var port = address.Length > 1 ? int.Parse(address[1]) : 1521;

        return Track.Listening("oracle", address[0], port)
            .Check($"oracle: connect to {builder.DataSource} as {builder.UserID} (the PDB is open)", () =>
            {
                using var conn = new OracleConnection(
                    new OracleConnectionStringBuilder(OracleConnectionString) { Pooling = false }.ConnectionString);
                conn.Open();
                using var command = conn.CreateCommand();
                command.CommandText = "select 1 from dual";
                command.ExecuteScalar();
            });
    }

    const string FirebirdVariable = "weasel_firebird_testing_database";

    /// <summary>The suite's own default (src/Weasel.Firebird.Tests/ConnectionSource.cs).</summary>
    const string FirebirdDefault =
        "DataSource=localhost;Port=3065;Database=/var/lib/firebird/data/weasel_testing.fdb;User=SYSDBA;Password=P@55w0rd;Charset=UTF8";

    static string FirebirdConnectionString =>
        Environment.GetEnvironmentVariable(FirebirdVariable) is { Length: > 0 } configured ? configured : FirebirdDefault;

    /// <summary>
    /// The configured database file never exists -- each test class creates one beside it -- so the
    /// check asks the service manager, the same thing the compose healthcheck asks. The first
    /// connection right after the container starts fails for about half a second.
    /// </summary>
    static Track firebirdTrack(string name, string connectionString)
    {
        var builder = new FbConnectionStringBuilder(connectionString);
        return Track.Listening(name, builder.DataSource, builder.Port)
            .Check($"{name}: the service manager answers", () =>
            {
                var version = new FbServerProperties(connectionString).GetServerVersion();
                if (string.IsNullOrEmpty(version)) throw new InvalidOperationException("No server version came back.");
            });
    }

    // ─── Optional legs: the CI matrix's other server versions ──────────────────────────────────

    /// <summary>
    /// One optional leg: a suite the gate already runs, against a CI server version the default
    /// containers do not provide. Each reads a connection string of its own and defaults to the port
    /// docker-compose.yml publishes for it.
    /// </summary>
    sealed record GateLeg(string Name, string Project, int Workers, string Variable, string Default, bool SqlServer)
    {
        public string ConnectionString =>
            Environment.GetEnvironmentVariable(Variable) is { Length: > 0 } configured ? configured : Default;

        public Track Track() => SqlServer ? sqlServerTrack(Name, ConnectionString) : firebirdTrack(Name, ConnectionString);

        public IWorkerFactory Factory(GateUnit unit) => SqlServer
            ? lanesFor(unit, sqlServerLanes(ConnectionString))
            : new MtpWorkerFactory(unit.Executable,
                new Dictionary<string, string> { [FirebirdVariable] = ConnectionString });
    }

    static readonly GateLeg[] KnownLegs =
    [
        new("sqlserver2019", "Weasel.SqlServer.Tests", 4, "weasel_sqlserver2019_testing_database",
            "Server=localhost,1436;User Id=sa;Password=P@55w0rd;Timeout=5;MultipleActiveResultSets=True;Initial Catalog=master;Encrypt=False",
            SqlServer: true),
        new("sqlserver2022", "Weasel.SqlServer.Tests", 4, "weasel_sqlserver2022_testing_database",
            "Server=localhost,1437;User Id=sa;Password=P@55w0rd;Timeout=5;MultipleActiveResultSets=True;Initial Catalog=master;Encrypt=False",
            SqlServer: true),
        new("firebird3", "Weasel.Firebird.Tests", 4, "weasel_firebird3_testing_database",
            "DataSource=localhost;Port=3063;Database=/var/lib/firebird/data/weasel_testing.fdb;User=SYSDBA;Password=P@55w0rd;Charset=UTF8",
            SqlServer: false),
        new("firebird4", "Weasel.Firebird.Tests", 4, "weasel_firebird4_testing_database",
            "DataSource=localhost;Port=3064;Database=/var/lib/firebird/data/weasel_testing.fdb;User=SYSDBA;Password=P@55w0rd;Charset=UTF8",
            SqlServer: false)
    ];

    /// <summary>The requested legs, in rounds of at most one leg per project.</summary>
    IReadOnlyList<IReadOnlyList<GateLeg>> legRounds()
    {
        if (string.IsNullOrWhiteSpace(GateLegs)) return [];

        var requested = GateLegs.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(name => KnownLegs.FirstOrDefault(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                            ?? throw new ArgumentException(
                                $"Unknown gate leg '{name}'. Known: {string.Join(", ", KnownLegs.Select(x => x.Name))}."));

        var rounds = new List<List<GateLeg>>();
        foreach (var leg in requested)
        {
            var round = rounds.FirstOrDefault(r => r.All(x => x.Project != leg.Project));
            if (round is null) rounds.Add(round = []);
            round.Add(leg);
        }

        return rounds;
    }
}
