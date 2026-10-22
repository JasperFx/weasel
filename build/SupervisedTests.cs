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
//   - The ledger, carried between CI runs as an artifact (artifacts/test-ledger/). Every run's
//     per-test durations and outcomes, so lanes are balanced by measured duration from the first
//     pass instead of by test count.
//   - Collapsed theories judged worst-first. A theory whose data xUnit cannot serialize is one test
//     under one uid that reports every row; Bobcat 1.0 keeps the worst row's verdict and names the
//     collapse in its report (bobcat#649), so a failing row can no longer hide behind a passing one.
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
    /// The ledger lives here, beside the run's report, and is carried from run to run as a CI
    /// artifact (bobcat#730's default): each workflow leg downloads the newest successful master run's
    /// <c>test-ledger-{CI_JOB_NAME}</c> artifact into this directory before the Test step, the run
    /// balances lanes on it, folds its own observations in and saves it back, and the upload step
    /// sends it on. Nothing is committed. Under artifacts/, which is gitignored.
    /// </summary>
    /// <remarks>
    /// Locally the same directory carries a laptop's own runs forward, and
    /// <c>gh run download &lt;run&gt; -n test-ledger-postgres-net9.0-cstrue -D artifacts/test-ledger</c>
    /// seeds it from CI. Tests are keyed by display name since Bobcat 1.0, so a CI ledger matches a
    /// local run and either framework; the uid it used to be keyed by matched nothing off the machine
    /// that recorded it, which is why the ledger used to be committed and folded by hand.
    /// </remarks>
    static AbsolutePath RunArtifactsDirectory => RootDirectory / "artifacts" / "test-ledger";

    /// <summary>
    /// A test the ledger has not seen run for this long is dropped from it: deleted, renamed, or (for
    /// a theory that collapses under one uid) recorded under a row's display name that has changed.
    /// </summary>
    static readonly TimeSpan LedgerForgetsAfter = TimeSpan.FromDays(30);

    /// <summary>
    /// A test in flight this long is named while the run is still going. Report-only -- nothing is
    /// killed. Weasel's slowest tests take seconds (the ledger has the numbers), so two
    /// minutes is far past anything healthy and still well inside every job's 15-45 minute cap: a
    /// wedged job says which test it was wedged on before the cap destroys the log.
    /// </summary>
    static readonly TimeSpan StallThreshold = TimeSpan.FromMinutes(2);

    /// <summary>
    /// The ceiling on retries. Only a test carrying a <c>Retry</c> trait can spend it (see the note
    /// at the top of this file), and none does. Since Bobcat 1.0 such a retry runs alone in a fresh
    /// process (bobcat#661); <c>[Trait("RetryWarm", "true")]</c> is the opt-in for a same-process one.
    /// </summary>
    static readonly RetryBudget OptInRetries = new() { MaxAttemptsPerTest = 3, MaxRetriesPerRun = 10 };

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

        var ledgerPath = RunArtifactsDirectory / fileNameSafe($"{ledgerJobName}.{projectName}.{framework}.ledger.json");
        var ledger = previousLedger(projectName);
        var known = ledger.KnownDurations();

        var supervisor = new Supervisor(factory)
        {
            MaxParallelWorkers = workers,
            // Announces the run to a Bobcat monitor when one answers at BOBCAT_MONITOR_URL. On a CI
            // runner nothing listens: one refused local connection, nothing published.
            // BOBCAT_MONITOR=0 turns it off.
            PublishToMonitor = true,
            Log = message => Log.Information("  {Message}", message)
        };
        applyWeaselPolicy(supervisor, known);

        if (known.Count > 0)
            Log.Information("  balancing lanes with {Count} measured duration(s) from {Ledger}", known.Count,
                RootDirectory.GetRelativePathTo(RunArtifactsDirectory));

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

        var results = supervisor.Run().GetAwaiter().GetResult();
        return report(projectName, framework, results, runId, startedAt, ledger, ledgerPath);
    }

    /// <summary>
    /// The supervisor settings every Weasel suite runs under, in CI (<see cref="RunSupervised"/>) and
    /// in the local gate (LocalGate.cs) alike, so the gate judges a suite exactly as its workflow does.
    /// </summary>
    static void applyWeaselPolicy(Supervisor supervisor, IReadOnlyDictionary<string, TimeSpan> known)
    {
        supervisor.RetryBudget = OptInRetries;
        // Lesson 4: lanes sitting idle at peak working set through a fresh-process retry is
        // workers + 1 hosts resident at once. Cheap insurance even with retries opt-in.
        supervisor.ReleaseIdleLanes = true;
        supervisor.StallThreshold = StallThreshold;
        supervisor.StallAction = StallAction.Report;
        supervisor.HeartbeatInterval = TimeSpan.FromSeconds(30);
        supervisor.ResourceSampleInterval = TimeSpan.FromSeconds(15);
        supervisor.KnownTestDurations = known.Count > 0 ? known : null;
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

        Log.Information("=== {Project} ({Framework}): {Summary} ===", projectName, framework, results.Summarize());

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

    // ─── The ledger ───────────────────────────────────────────────────────

    /// <summary>
    /// Every ledger already in artifacts/test-ledger/ for this project -- on CI the one the leg
    /// downloaded from master, locally whatever earlier runs and downloads left there -- merged into
    /// one. Ledgers written by Bobcat 0.60 (keyed by uid) are re-keyed by display name as they load.
    /// None at all is an empty ledger, and lanes balance by test count.
    /// </summary>
    static TestLedger previousLedger(string projectName)
    {
        var ledger = TestLedger.Empty();
        if (!Directory.Exists(RunArtifactsDirectory)) return ledger;

        foreach (var file in Directory.EnumerateFiles(RunArtifactsDirectory, "*.ledger.json")
                     .Where(x => Path.GetFileName(x).Contains($".{projectName}.", StringComparison.Ordinal)))
        {
            try
            {
                ledger = ledger.Merge(TestLedger.Load(file));
            }
            catch (Exception e)
            {
                // A ledger only balances lanes. One that will not load must never fail the tests.
                Log.Warning(e, "Ignoring the unreadable ledger {File}", file);
            }
        }

        return ledger;
    }

    /// <summary>
    /// Folds this run into the previous ledger and saves it under this leg's name, for the CI upload
    /// to carry to the next run. Reporting about the tests must never be what fails the tests.
    /// </summary>
    void recordLedger(string projectName, string framework, SupervisorResults results, string runId,
        DateTimeOffset startedAt, TestLedger ledger, AbsolutePath ledgerPath)
    {
        try
        {
            // A partial (cancelled) run is the run so far, not a verdict: carry the history on
            // without it.
            var folded = results.IsPartial ? ledger : ledger.Record(SupervisorLedger.From(results, runId, startedAt));

            // The artifact is carried forward indefinitely, so a deleted or renamed test would stay in
            // it for good without a cutoff.
            folded.PruneTestsNotSeenSince(startedAt - LedgerForgetsAfter).Save(ledgerPath);
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

            builder.AppendLine("| tests | passed | skipped | passed on retry | failed | indeterminate | stalled | worker faults | workers | wall clock |");
            builder.AppendLine("|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|");
            builder.AppendLine(
                $"| {results.Tests.Count} | {results.CleanPasses.Count} | {results.Skipped.Count} | {mark(results.PassedOnRetry.Count)} | " +
                $"{mark(results.Failed.Count(x => !x.IsIndeterminate))} | {mark(results.Indeterminate.Count)} | " +
                $"{mark(results.StalledTests.Count)} | {mark(results.WorkerFaults.Count)} | {results.WorkersLaunched} launched | " +
                $"{results.Duration.TotalSeconds:F0}s |");
            builder.AppendLine();
            if (results.Collapsed.Count > 0)
            {
                builder.AppendLine($"> {results.Collapsed.Count} theory test(s) ran several rows under one uid each (data " +
                                   "xUnit cannot serialize), so the counts above show each as one test, judged by its " +
                                   "worst row. The report below names the rows.");
                builder.AppendLine();
            }

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
