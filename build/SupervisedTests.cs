using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Bobcat.Ledger;
using Bobcat.Resilience;
using Bobcat.Supervisor;
using Nuke.Common;
using Nuke.Common.IO;
using Serilog;

// The supervised test runner (JasperFx/bobcat#565): every CI test project runs through Bobcat's
// Supervisor (https://github.com/JasperFx/bobcat) instead of `dotnet test`. Copied in shape from
// Wolverine's build/SupervisedTests.cs, whose rollout is the lessons-of-record document
// (bobcat design/wolverine-ci-rollout.md). What it buys over a bare `dotnet test`:
//
//   - Worker processes. Each test project is an xUnit v3 Microsoft.Testing.Platform host (see
//     UseMicrosoftTestingPlatformRunner in Tests.Build.props), driven over the platform's wire
//     protocol, so one suite can be split across several processes. The split is by test CLASS,
//     never by test: a class's fixtures and statics assume one process.
//   - Per-lane isolation, where a suite needs it. See TestTargets.cs for which suites get what, and
//     why: Postgres and SQL Server get a database per lane, Firebird already isolates by class.
//   - Honest reporting. A crashed worker is Indeterminate with its exit code and stderr, never a
//     silent pass or an ordinary "failed"; a test in flight past the stall threshold is named while
//     the run is still going; the job summary carries the supervisor's own report.
//   - The committed ledger (build/test-ledger/). Every run's per-test durations and outcomes, folded
//     into one file per project, so lanes are balanced by measured duration from the first pass
//     instead of by test count.
//
// Retries are OPT-IN ONLY. `dotnet test` gave every test exactly one attempt, so that is the parity
// ceiling (lesson 8 of the Wolverine rollout: match the old harness's REAL retry ceiling). The budget
// below allows a retry only for a test that asks for one with [Trait("Retry", "N")], and no Weasel
// test does today -- so nothing is retried, and a failure is a failure. Never add a blanket retry
// policy here to turn a red build green: that is exactly how flakiness gets laundered into a pass.
partial class Build
{
    /// <summary>
    /// The target framework to run, e.g. <c>--framework net9.0</c>. Every CI job names one. When
    /// omitted, every framework the project has been built for runs, one after the other -- never
    /// concurrently, because the database-backed suites share schemas between frameworks.
    /// </summary>
    [Parameter("Target framework to run the tests under, e.g. net9.0. Default: every framework built")]
    readonly string Framework;

    /// <summary>
    /// Overrides every suite's worker count, and bypasses the per-machine clamp. For measuring.
    /// </summary>
    [Parameter("Worker processes per suite, overriding the committed count and the per-machine clamp")]
    readonly int? TestWorkers;

    /// <summary>
    /// Whether to fold this run into the committed ledger under build/test-ledger/. Defaults to on
    /// locally and off on CI, where a run's ledger goes to artifacts/test-ledger/ instead, to be
    /// folded in by <see cref="FoldLedgers"/>: a CI checkout's edits are thrown away with it.
    /// </summary>
    [Parameter("Fold this run into the committed ledger (default: on locally, off on CI)")]
    readonly bool? RecordLedger;

    /// <summary>
    /// A directory of per-run ledgers (the CI artifacts) for <see cref="FoldLedgers"/>.
    /// </summary>
    [Parameter("Directory of downloaded test-ledger artifacts to fold into build/test-ledger/")]
    readonly AbsolutePath LedgerSource;

    /// <summary>
    /// The committed ledgers, one per test project. Every framework and every CI matrix leg folds
    /// into the same file -- each run is its own observation, so they never overwrite each other.
    /// </summary>
    static AbsolutePath CommittedLedgerDirectory => RootDirectory / "build" / "test-ledger";

    /// <summary>
    /// This run's own ledger and report, for the CI artifact upload. Under artifacts/, which is
    /// gitignored.
    /// </summary>
    static AbsolutePath RunArtifactsDirectory => RootDirectory / "artifacts" / "test-ledger";

    /// <summary>
    /// Observations kept per test in a new ledger. The design default is 20; every CI leg of a
    /// project (four for Postgres, four for SQL Server) is its own observation, and each costs a few
    /// hundred bytes per test in a committed file, so 20 would make the Postgres ledger alone several
    /// megabytes. Five is enough for a median. An existing file keeps the retention it was written with.
    /// </summary>
    const int LedgerRunsPerTest = 5;

    /// <summary>
    /// A test in flight this long is named while the run is still going. Report-only -- nothing is
    /// killed. Weasel's slowest tests take seconds (the committed ledger has the numbers), so two
    /// minutes is far past anything healthy and still well inside every job's 15-45 minute cap: a
    /// wedged job says which test it was wedged on before the cap destroys the log.
    /// </summary>
    static readonly TimeSpan StallThreshold = TimeSpan.FromMinutes(2);

    /// <summary>
    /// The ceiling on retries. Only a test carrying a <c>Retry</c> trait can spend it (see the note
    /// at the top of this file), and none does.
    /// </summary>
    static readonly RetryBudget OptInRetries = new() { MaxAttemptsPerTest = 3, MaxRetriesPerRun = 10 };

    /// <summary>
    /// Folds downloaded CI ledgers into the committed ones: <c>./build.sh FoldLedgers --ledger-source
    /// &lt;dir&gt;</c>, after <c>gh run download &lt;run&gt; -p 'test-ledger-*' -D &lt;dir&gt;</c>. The
    /// fold is a set union, so folding the same artifact twice, or in any order, changes nothing.
    /// </summary>
    Target FoldLedgers => _ => _
        .Requires(() => LedgerSource)
        .Executes(() =>
        {
            var files = Directory.EnumerateFiles(LedgerSource, "*.ledger.json", SearchOption.AllDirectories).ToList();
            Assert.True(files.Count > 0, $"No *.ledger.json files under {LedgerSource}");

            foreach (var group in files.GroupBy(projectOfLedgerFile))
            {
                var path = CommittedLedgerDirectory / $"{group.Key}.json";
                var ledger = group.Aggregate(TestLedger.Load(path), (folded, file) => folded.Merge(TestLedger.Load(file)));
                ledger.Save(path);
                Log.Information("Folded {Count} run ledger(s) into {Path}", group.Count(), path);
            }
        });

    // A run ledger is named {job}.{project}.{framework}.ledger.json; the project is the segment that
    // names one of the test projects, so a job name with dots in it cannot confuse the split.
    static string projectOfLedgerFile(string file)
    {
        var name = Path.GetFileName(file);
        var project = Directory.GetDirectories(RootDirectory / "src", "*.Tests")
            .Select(Path.GetFileName)
            .OrderByDescending(x => x.Length)
            .FirstOrDefault(x => name.Contains($".{x}.", StringComparison.Ordinal));

        return project ?? throw new InvalidOperationException($"Cannot tell which test project {file} belongs to");
    }

    /// <summary>
    /// How one suite's worker processes are kept apart, when it needs anything at all. Provision runs
    /// once, before the first worker launches; EnvironmentFor hands each lane its own whole connection
    /// string -- never a fragment for the suite to stitch together.
    /// </summary>
    record LaneIsolation(string Description, Action<int> Provision, Func<int, IReadOnlyDictionary<string, string>> EnvironmentFor);

    /// <summary>
    /// Runs one test project through the supervisor, for every framework this invocation covers.
    /// </summary>
    /// <param name="workers">
    /// The committed worker count. Clamped to half the machine's cores unless --test-workers says
    /// otherwise (lesson 3: a count tuned on a developer machine killed a 4-vCPU hosted runner).
    /// </param>
    /// <param name="isolation">Per-lane isolation; null when the suite needs none at this worker count.</param>
    void RunSupervised(string projectName, int workers = 1, LaneIsolation isolation = null)
    {
        var projectPath = RootDirectory / "src" / projectName / $"{projectName}.csproj";
        var failed = new List<string>();

        foreach (var framework in frameworksFor(projectPath))
        {
            if (!runSupervised(projectName, projectPath, framework, workers, isolation))
                failed.Add($"{projectName} ({framework})");
        }

        Assert.True(failed.Count == 0, $"Tests failed: {string.Join(", ", failed)}");
    }

    bool runSupervised(string projectName, AbsolutePath projectPath, string framework, int workers,
        LaneIsolation isolation)
    {
        var ceiling = Math.Max(1, Environment.ProcessorCount / 2);
        if (TestWorkers is { } requested)
        {
            workers = requested;
        }
        else if (workers > ceiling)
        {
            Log.Information("Clamping {Asked} workers to {Ceiling} for this machine's {Cores} core(s)",
                workers, ceiling, Environment.ProcessorCount);
            workers = ceiling;
        }

        // A suite that shares one database between its lanes would collide the moment two lanes run.
        Assert.True(workers == 1 || isolation is not null || IsolatedByClass.Contains(projectName),
            $"{projectName} has no per-lane isolation and cannot run on {workers} workers");

        var executable = testHostFor(projectPath, framework);
        Log.Information("=== {Project} ({Framework}): supervised run, {Workers} worker(s){Isolation} ===",
            projectName, framework, workers,
            workers > 1 && isolation is not null ? $", {isolation.Description}" : "");

        if (workers > 1 && isolation is not null)
        {
            for (var lane = 0; lane < workers; lane++) isolation.Provision(lane);
        }

        var factory = new MtpWorkerFactory(executable)
        {
            // Discovery, isolated and recycled workers all report lane 0, so the databases
            // provisioned equal the workers asked for, not the processes launched.
            EnvironmentFor = workers > 1 && isolation is not null
                ? context => isolation.EnvironmentFor(context.Lane)
                : null
        };

        var ledgerPath = CommittedLedgerDirectory / $"{projectName}.json";
        var ledger = TestLedger.Load(ledgerPath, LedgerRunsPerTest);
        var known = ledger.KnownDurations();

        var supervisor = new Supervisor(factory)
        {
            MaxParallelWorkers = workers,
            RetryBudget = OptInRetries,
            // Lesson 4: lanes sitting idle at peak working set through a fresh-process retry is
            // workers + 1 hosts resident at once. Cheap insurance even with retries opt-in.
            ReleaseIdleLanes = true,
            StallThreshold = StallThreshold,
            StallAction = StallAction.Report,
            HeartbeatInterval = TimeSpan.FromSeconds(30),
            ResourceSampleInterval = TimeSpan.FromSeconds(15),
            KnownTestDurations = known.Count > 0 ? known : null,
            // Announces the run to a Bobcat monitor when one answers at BOBCAT_MONITOR_URL. On a CI
            // runner nothing listens: one refused local connection, nothing published.
            // BOBCAT_MONITOR=0 turns it off.
            PublishToMonitor = true,
            Log = message => Log.Information("  {Message}", message)
        };

        if (known.Count > 0)
            Log.Information("  balancing lanes with {Count} measured duration(s) from {Ledger}", known.Count,
                RootDirectory.GetRelativePathTo(ledgerPath));

        var runId = ledgerRunId(projectName, framework);
        var startedAt = DateTimeOffset.UtcNow;

        // A cancelled job (the workflow's timeout, or a superseding push) is SIGTERMed with a grace
        // period. Snapshot the run inside it, so the summary still says what ran, what was in
        // flight and what never started -- a capped job otherwise reports nothing at all.
        using var cancellation = PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ =>
        {
            try
            {
                report(projectName, framework, supervisor.Snapshot(), runId, startedAt, ledger, ledgerPath);
            }
            catch (Exception e)
            {
                Log.Warning(e, "Could not snapshot the cancelled run of {Project}", projectName);
            }
        });

        var rows = new TheoryRowGuard();
        supervisor.AddObserver(rows);

        var results = supervisor.Run().GetAwaiter().GetResult();
        var reported = report(projectName, framework, results, runId, startedAt, ledger, ledgerPath);
        return rows.Check(projectName, framework, results) && reported;
    }

    /// <summary>
    /// Catches the failures the supervisor's verdict would otherwise lose.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A theory whose data xUnit cannot serialize (a <c>TableColumn</c>, an <c>Action&lt;Table&gt;</c>,
    /// an <c>IEnumerable&lt;Guid&gt;</c>) is not enumerated at discovery: it is ONE test, with one uid,
    /// that runs all its rows when executed. Over Microsoft.Testing.Platform every row then reports a
    /// verdict under that same uid, and Bobcat.Supervisor 0.60.4 keeps the LAST terminal update per
    /// uid. So a failing row followed by a passing one is a pass. Measured on this branch: a forced
    /// failure in the first row of <c>TableColumnTests.can_add</c> failed the in-process host, and the
    /// supervised run reported "1056 passed".
    /// </para>
    /// <para>
    /// The observer sees every update as it arrives, so it keeps what the verdict throws away: every
    /// row that failed. A test whose final verdict is a pass after one of its rows failed fails the
    /// job, naming the row. Remove this when the supervisor folds a uid's rows itself.
    /// </para>
    /// </remarks>
    class TheoryRowGuard : ISupervisorObserver
    {
        readonly object _lock = new();
        readonly Dictionary<string, int> _terminalUpdates = new(StringComparer.Ordinal);
        readonly Dictionary<string, List<string>> _failedRows = new(StringComparer.Ordinal);

        public void TestUpdated(WorkerLaunchContext worker, WorkerTestUpdate update)
        {
            if (update.State is not { } state) return;

            lock (_lock)
            {
                _terminalUpdates[update.Uid] = _terminalUpdates.GetValueOrDefault(update.Uid) + 1;

                if (state is WorkerTestState.Failed or WorkerTestState.Error or WorkerTestState.Timeout)
                {
                    if (!_failedRows.TryGetValue(update.Uid, out var rows)) _failedRows[update.Uid] = rows = [];
                    rows.Add(update.DisplayName);
                }
            }
        }

        public bool Check(string projectName, string framework, SupervisorResults results)
        {
            lock (_lock)
            {
                var multiRow = _terminalUpdates.Where(x => x.Value > 1).ToList();
                if (multiRow.Count > 0)
                    Log.Information("  {Tests} test(s) reported {Rows} theory rows under one uid each; " +
                                    "the supervisor counts each as one test", multiRow.Count, multiRow.Sum(x => x.Value));

                if (multiRow.Count > 0 && Environment.GetEnvironmentVariable("GITHUB_STEP_SUMMARY") is { Length: > 0 } summary)
                {
                    try
                    {
                        File.AppendAllText(summary,
                            $"> {multiRow.Count} theory test(s) ran {multiRow.Sum(x => x.Value)} rows under one uid each " +
                            "(data xUnit cannot serialize); the counts above show each as one test. Every row's " +
                            "verdict was checked: a failing row fails the job.\n\n");
                    }
                    catch (Exception e)
                    {
                        Log.Warning(e, "Could not append to $GITHUB_STEP_SUMMARY");
                    }
                }

                // A retried test's earlier failures are the retry policy's business, not a lost row.
                var lost = results.Tests
                    .Where(t => t.Final.Succeeded && t.AttemptCount == 1 && _failedRows.ContainsKey(t.Uid))
                    .ToList();

                foreach (var test in lost)
                {
                    foreach (var row in _failedRows[test.Uid])
                    {
                        Log.Error("  [FAILED ROW] {Row} — a failing theory row the supervisor's verdict reported as passed", row);
                        if (Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true")
                            Console.WriteLine($"::error title={projectName} ({framework}) failed theory row::{row}");
                    }
                }

                return lost.Count == 0;
            }
        }
    }

    /// <summary>
    /// Suites whose tests already keep out of each other's way across processes, so more than one
    /// worker needs no plumbing. See TestTargets.cs.
    /// </summary>
    static readonly HashSet<string> IsolatedByClass = ["Weasel.Firebird.Tests"];

    bool report(string projectName, string framework, SupervisorResults results, string runId,
        DateTimeOffset startedAt, TestLedger ledger, AbsolutePath ledgerPath)
    {
        recordLedger(projectName, framework, results, runId, startedAt, ledger, ledgerPath);
        writeRunReport(projectName, framework, results);
        appendStepSummary(projectName, framework, results);
        annotate(projectName, framework, results);

        if (results.AbortReason is not null)
        {
            Log.Error("=== {Project} ({Framework}): run ABORTED — {Reason} ===", projectName, framework, results.AbortReason);
            return false;
        }

        // The inversion of GuardAgainstAnUnfilteredRun: no CI job here filters, so a run that found
        // nothing is a broken host or a broken discovery, never a green job.
        if (results.Tests.Count == 0)
        {
            Log.Error("=== {Project} ({Framework}): NO tests were discovered ===", projectName, framework);
            return false;
        }

        Log.Information("=== {Project} ({Framework}): {Summary}; {Skipped} of those passes were skips ===",
            projectName, framework, results.Summarize(), skippedCount(results));

        foreach (var flaky in results.PassedOnRetry)
            Log.Warning("  [FLAKY] {Test} — passed on attempt {Attempts}", flaky.DisplayName, flaky.AttemptCount);

        foreach (var fault in results.WorkerFaults)
            Log.Error("  [WORKER FAULT] {Fault}", fault);

        foreach (var stalled in results.StalledTests)
            Log.Warning("  [STALLED] {Test} — {Seconds}s in flight on lane {Lane}",
                stalled.DisplayName, (int)stalled.InFlight.TotalSeconds, stalled.Worker.Lane);

        var memory = RunResources.For(results);
        if (memory.IsMeasured)
            Log.Information("  peak worker RSS {Peak}", RunResources.Humanize(memory.PeakBytes!.Value));

        foreach (var test in results.Indeterminate)
            Log.Error("  [INDETERMINATE] {Test} — {Error}", test.DisplayName, test.Final.Outcome.ErrorMessage);

        foreach (var test in results.Failed.Where(x => !x.IsIndeterminate))
            Log.Error("  [FAILED] {Test} — {Error}", test.DisplayName, test.Final.Outcome.ErrorMessage);

        return results.ExitCode == 0;
    }

    static int skippedCount(SupervisorResults results)
        => results.Tests.Count(t => t.AttemptCount == 1 && t.Final.Outcome.State == WorkerTestState.Skipped);

    // ─── The ledger ───────────────────────────────────────────────────────

    /// <summary>
    /// The run's own ledger always goes to artifacts/test-ledger/ (the CI upload); locally it is also
    /// folded into the committed one. Reporting about the tests must never be what fails the tests.
    /// </summary>
    void recordLedger(string projectName, string framework, SupervisorResults results, string runId,
        DateTimeOffset startedAt, TestLedger ledger, AbsolutePath ledgerPath)
    {
        try
        {
            var observations = SupervisorLedger.From(results, runId, startedAt);

            RunArtifactsDirectory.CreateDirectory();
            TestLedger.Empty(LedgerRunsPerTest).Record(observations)
                .Save(RunArtifactsDirectory / fileNameSafe($"{ledgerJobName}.{projectName}.{framework}.ledger.json"));

            if (RecordLedger ?? IsLocalBuild)
            {
                // A partial (cancelled) run is the run so far, not a verdict: keep it out of the
                // committed history.
                if (results.IsPartial) return;

                ledger.Record(observations).Save(ledgerPath);
                Log.Information("  folded {Count} observation(s) into {Ledger}", observations.Count,
                    RootDirectory.GetRelativePathTo(ledgerPath));
            }
        }
        catch (Exception e)
        {
            Log.Warning(e, "Could not record the ledger for {Project}", projectName);
        }
    }

    void writeRunReport(string projectName, string framework, SupervisorResults results)
    {
        try
        {
            RunArtifactsDirectory.CreateDirectory();
            File.WriteAllText(
                RunArtifactsDirectory / fileNameSafe($"{ledgerJobName}.{projectName}.{framework}.report.json"),
                RunReport.ToJson(results));
        }
        catch (Exception e)
        {
            Log.Warning(e, "Could not write the run report for {Project}", projectName);
        }
    }

    /// <summary>
    /// The CI job this run belongs to, set by each workflow from its matrix leg. GITHUB_JOB is "build"
    /// for every leg of every workflow here, so it cannot tell two runs apart.
    /// </summary>
    static string ledgerJobName => Environment.GetEnvironmentVariable("CI_JOB_NAME") is { Length: > 0 } name
        ? name
        : "local";

    /// <summary>
    /// One run id per job, project and framework. The ledger unions observations by (test, run id), so
    /// every leg that feeds one project's ledger must say which run it was.
    /// </summary>
    static string ledgerRunId(string projectName, string framework)
    {
        var github = Environment.GetEnvironmentVariable("GITHUB_RUN_ID");
        var prefix = string.IsNullOrEmpty(github)
            ? $"local-{DateTimeOffset.UtcNow:yyyyMMddHHmmss}"
            : $"gh-{github}-{Environment.GetEnvironmentVariable("GITHUB_RUN_ATTEMPT") ?? "1"}";

        return $"{prefix}/{ledgerJobName}/{projectName}/{framework}";
    }

    static string fileNameSafe(string name)
    {
        foreach (var invalid in Path.GetInvalidFileNameChars()) name = name.Replace(invalid, '-');
        return name.Replace(' ', '-');
    }

    // ─── The job summary ──────────────────────────────────────────────────

    static void appendStepSummary(string projectName, string framework, SupervisorResults results)
    {
        var summaryFile = Environment.GetEnvironmentVariable("GITHUB_STEP_SUMMARY");
        if (string.IsNullOrEmpty(summaryFile)) return;

        try
        {
            var builder = new StringBuilder();
            builder.AppendLine($"### {projectName} ({framework}) — supervised");
            builder.AppendLine();
            if (results.IsPartial)
            {
                builder.AppendLine("> **PARTIAL** — the job was cancelled mid-run. Tests without a verdict are " +
                                   "counted Indeterminate, not failed.");
                builder.AppendLine();
            }

            // Bobcat counts a skipped test as a clean pass (Summarize, RunReport's cleanPass), so the
            // skips are split out here: "passed" should mean something ran.
            var skipped = skippedCount(results);
            builder.AppendLine("| tests | passed | skipped | passed on retry | failed | indeterminate | stalled | worker faults | workers | wall clock |");
            builder.AppendLine("|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|");
            builder.AppendLine(
                $"| {results.Tests.Count} | {results.CleanPasses.Count - skipped} | {skipped} | {mark(results.PassedOnRetry.Count)} | " +
                $"{mark(results.Failed.Count(x => !x.IsIndeterminate))} | {mark(results.Indeterminate.Count)} | " +
                $"{mark(results.StalledTests.Count)} | {mark(results.WorkerFaults.Count)} | {results.WorkersLaunched} launched | " +
                $"{results.Duration.TotalSeconds:F0}s |");
            builder.AppendLine();
            builder.AppendLine("<details><summary>Supervisor report</summary>");
            builder.AppendLine();
            builder.AppendLine("```text");
            builder.AppendLine(RunReport.ToText(results).TrimEnd());
            builder.AppendLine("```");
            builder.AppendLine();
            builder.AppendLine("</details>");
            builder.AppendLine();

            File.AppendAllText(summaryFile, builder.ToString());
        }
        catch (Exception e)
        {
            Log.Warning(e, "Could not append to $GITHUB_STEP_SUMMARY for {Project}", projectName);
        }

        static string mark(int count) => count == 0 ? "0" : $"**{count}**";
    }

    /// <summary>
    /// GitHub annotations: Serilog's warnings are not annotations, so these are the literal workflow
    /// commands. One line each -- workflow commands are newline-delimited.
    /// </summary>
    static void annotate(string projectName, string framework, SupervisorResults results)
    {
        if (Environment.GetEnvironmentVariable("GITHUB_ACTIONS") != "true") return;

        foreach (var test in results.Failed.Take(20))
            Console.WriteLine($"::error title={projectName} ({framework}) {(test.IsIndeterminate ? "indeterminate" : "failed")}::" +
                              $"{test.DisplayName}: {oneLine(test.Final.Outcome.ErrorMessage)}");

        foreach (var test in results.PassedOnRetry)
            Console.WriteLine($"::warning title={projectName} ({framework}) flaky::{test.DisplayName} passed only on attempt {test.AttemptCount}");

        foreach (var stalled in results.StalledTests)
            Console.WriteLine($"::warning title={projectName} ({framework}) stalled::{stalled.DisplayName} was in flight " +
                              $"{(int)stalled.InFlight.TotalSeconds}s, past the {StallThreshold.TotalMinutes:F0}-minute threshold");

        if (results.IsPartial)
            Console.WriteLine($"::warning title={projectName} ({framework}) partial::cancelled mid-run; the summary holds the run so far");

        static string oneLine(string text)
        {
            if (string.IsNullOrWhiteSpace(text)) return "no message";
            var flat = string.Join(" ", text.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()));
            return flat.Length <= 400 ? flat : flat[..400] + "…";
        }
    }

    // ─── Test host resolution ─────────────────────────────────────────────

    IReadOnlyList<string> frameworksFor(AbsolutePath projectPath)
    {
        if (!string.IsNullOrEmpty(Framework)) return [Framework];

        var binDir = projectPath.Parent / "bin" / Configuration;
        var built = Directory.Exists(binDir)
            ? Directory.GetDirectories(binDir).Select(Path.GetFileName)
                .Where(tfm => File.Exists(testHostPathFor(projectPath, tfm))).OrderBy(x => x).ToList()
            : [];

        Assert.True(built.Count > 0,
            $"No test host under {binDir}. Build {projectPath.Name} ({Configuration}) first.");
        return built;
    }

    string testHostFor(AbsolutePath projectPath, string framework)
    {
        var executable = testHostPathFor(projectPath, framework);
        Assert.True(File.Exists(executable),
            $"{executable} does not exist. Build {projectPath.Name} for {framework} ({Configuration}) first; the " +
            "executable comes from UseMicrosoftTestingPlatformRunner in Tests.Build.props.");
        return executable;
    }

    string testHostPathFor(AbsolutePath projectPath, string framework)
    {
        var host = projectPath.Parent / "bin" / Configuration / framework / projectPath.NameWithoutExtension;
        return OperatingSystem.IsWindows() ? host + ".exe" : host;
    }
}
