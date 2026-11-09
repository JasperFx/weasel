using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Nuke.Common;
using Nuke.Common.IO;
using Nuke.Common.Tooling;
using Nuke.Common.Tools.DotNet;
using static Nuke.Common.Tools.DotNet.DotNetTasks;

// Targets for building the Critter Stack release train against local NuGet packs, before the nuget.org
// replay (JasperFx/weasel#735): Pack, AotSmoke and TakeLocalUpstream.
partial class Build
{
    [Parameter("Package version for Pack, overriding <Version> in Directory.Build.props (e.g. 10.0.0-local.1)")]
    readonly string Version;

    [Parameter("Folder Pack copies the .nupkg files into, and TakeLocalUpstream adds as a NuGet source")]
    readonly string Feed;

    [Parameter("Version of the local JasperFx packages TakeLocalUpstream pins (e.g. 3.0.0-local.1)")]
    readonly string UpstreamVersion;

    [Parameter("Target framework AotSmoke publishes for - Default is 'net10.0'")]
    readonly string SingleFramework = "net10.0";

    // Every shipped package; publish_nuget.yml packs the same nine.
    static readonly string[] PackableProjects =
    [
        "Weasel.Core", "Weasel.Storage", "Weasel.Postgresql", "Weasel.SqlServer", "Weasel.MySql",
        "Weasel.Oracle", "Weasel.Sqlite", "Weasel.Firebird", "Weasel.EntityFrameworkCore"
    ];

    // The JasperFx packages Directory.Packages.props pins.
    static readonly string[] UpstreamPackages = ["JasperFx", "JasperFx.Events", "JasperFx.SourceGenerator"];

    AbsolutePath ArtifactsDirectory => RootDirectory / "artifacts";

    Target Pack => _ => _
        .DependsOn(Restore)
        .Executes(() =>
        {
            foreach (var name in PackableProjects)
            {
                DotNetPack(s => s
                    .SetProject(RootDirectory / "src" / name / $"{name}.csproj")
                    .SetOutputDirectory(ArtifactsDirectory)
                    .SetConfiguration(Configuration.Release)
                    .When(_ => !string.IsNullOrEmpty(Version), x => x.SetProperty("Version", Version)));
            }

            if (!string.IsNullOrEmpty(Feed))
            {
                Directory.CreateDirectory(Feed);
                var pattern = string.IsNullOrEmpty(Version) ? "*.nupkg" : $"*.{Version}.nupkg";
                foreach (var package in ArtifactsDirectory.GlobFiles(pattern))
                {
                    File.Copy(package, Path.Combine(Feed, package.Name), true);
                }

                Serilog.Log.Information("Copied packed packages into the local feed {Feed}", Feed);
            }
        });

    // The analyzer in Weasel.Core.AotSmoke proves the trim/AOT warnings stay clean, but not the runtime half
    // (see CLAUDE.md): publish natively and run the binary.
    Target AotSmoke => _ => _
        .DependsOn(Restore)
        .Executes(() =>
        {
            var project = RootDirectory / "src" / "Weasel.Core.AotSmoke" / "Weasel.Core.AotSmoke.csproj";
            var output = RootDirectory / "artifacts" / "aot-smoke";
            DotNetPublish(s => s
                .SetProject(project)
                .SetConfiguration(Configuration.Release)
                .SetFramework(SingleFramework)
                .SetOutput(output)
                .SetProperty("PublishAot", "true"));

            var binary = output / (OperatingSystem.IsWindows() ? "Weasel.Core.AotSmoke.exe" : "Weasel.Core.AotSmoke");
            Assert.FileExists(binary, "The native AOT publish produced no executable");
            var process = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo(binary) { UseShellExecute = false })!;
            process.WaitForExit();
            Assert.True(process.ExitCode == 0, $"The native AOT smoke binary exited with {process.ExitCode}");
        });

    // Pin the JasperFx packages in Directory.Packages.props to a locally packed version, and register the
    // local feed as a NuGet source. Run it again with the released version to put the pins back.
    Target TakeLocalUpstream => _ => _
        .Requires(() => UpstreamVersion)
        .Executes(() =>
        {
            var path = RootDirectory / "Directory.Packages.props";
            var text = File.ReadAllText(path);
            foreach (var package in UpstreamPackages)
            {
                var pattern = $"(<PackageVersion Include=\"{Regex.Escape(package)}\" Version=\")[^\"]+(\")";
                Assert.True(Regex.IsMatch(text, pattern), $"Directory.Packages.props does not pin {package}");
                text = Regex.Replace(text, pattern, $"${{1}}{UpstreamVersion}$2");
            }

            File.WriteAllText(path, text);
            Serilog.Log.Information("Pinned {Packages} to {Version}", string.Join(", ", UpstreamPackages), UpstreamVersion);

            if (!string.IsNullOrEmpty(Feed))
            {
                // nuget.config is gitignored: the local feed is machine state, not something to commit.
                var config = RootDirectory / "nuget.config";
                var feed = Path.GetFullPath(Feed);
                if (!File.Exists(config) || !File.ReadAllText(config).Contains(feed))
                {
                    if (File.Exists(config)) DotNet($"nuget remove source critter-local --configfile {config}");
                    else File.WriteAllText(config, "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<configuration />\n");
                    DotNet($"nuget add source {feed} --name critter-local --configfile {config}");
                }
            }
        });
}
