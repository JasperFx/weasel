using System.Collections.Generic;
using System.IO;
using System.Linq;
using Nuke.Common;
using Nuke.Common.ProjectModel;
using Nuke.Common.Tools.DotNet;
using static Nuke.Common.Tools.DotNet.DotNetTasks;

class Build: NukeBuild
{
    [Parameter("Configuration to build - Default is 'Debug' (local) or 'Release' (server)")]
    readonly Configuration Configuration = IsLocalBuild ? Configuration.Debug : Configuration.Release;

    [Solution(SuppressBuildProjectCheck = true)] readonly Solution Solution;

    // Direct paths — Nuke 9.0.4's SolutionSerializer cannot load .slnx,
    // so Solution.Path / Solution.GetProject(...) are unavailable.
    string SolutionPath => RootDirectory / "Weasel.slnx";
    string WeaselCorePath => RootDirectory / "src" / "Weasel.Core" / "Weasel.Core.csproj";

    Target Clean => _ => _
        .Before(Restore)
        .Executes(() =>
        {
        });

    Target Restore => _ => _
        .Executes(() =>
        {
            DotNetRestore();
        });

    Target Compile => _ => _
        .DependsOn(Restore)
        .Executes(() =>
        {
            DotNetBuild(s => s
                .SetProjectFile(SolutionPath)
                .EnableNoRestore());
        });

    private Dictionary<string, string[]> ReferencedProjects = new()
    {
        { "jasperfx", ["JasperFx"] }
    };

    string[] Nugets = ["JasperFx"];


    Target Attach => _ => _.Executes(() =>
    {
        foreach (var pair in ReferencedProjects)
        {
            foreach (var projectName in pair.Value)
            {
                addProject(pair.Key, projectName);
            }
        }

        foreach (var nuget in Nugets)
        {
            if (HasPackageReference(WeaselCorePath, nuget))
            {
                DotNet($"remove {WeaselCorePath} package {nuget}");
            }
            else
            {
                Serilog.Log.Information($"Package {nuget} already removed from Weasel.Core; skipping.");
            }
        }
    });

    Target Detach => _ => _.Executes(() =>
    {
        foreach (var pair in ReferencedProjects)
        {
            foreach (var projectName in pair.Value)
            {
                removeProject(pair.Key, projectName);
            }

            // Also remove any transitively-added projects from the same external
            // repository (e.g. dotnet sln add pulls in <ProjectReference> dependencies
            // like JasperFx.SourceGeneration alongside JasperFx).
            removeOrphanedExternalProjects(pair.Key);
        }

        foreach (var nuget in Nugets)
        {
            if (HasPackageReference(WeaselCorePath, nuget))
            {
                Serilog.Log.Information($"Package {nuget} already present on Weasel.Core; skipping.");
            }
            else
            {
                DotNet($"add {WeaselCorePath} package {nuget} --prerelease");
            }
        }
    });

    private void addProject(string repository, string projectName)
    {
        var path = Path.GetFullPath((string)(RootDirectory / ".." / repository / "src" / projectName / $"{projectName}.csproj"));
        if (!File.Exists(path))
        {
            Assert.Fail($"Cannot attach: project '{path}' does not exist. Ensure ../{repository} is cloned next to this repo.");
        }

        if (!SolutionContainsProject(SolutionPath, path))
        {
            DotNet($"sln {SolutionPath} add {path} --solution-folder Attached");
        }
        else
        {
            Serilog.Log.Information($"Solution already contains {projectName}; skipping sln add.");
        }

        if (Nugets.Contains(projectName))
        {
            if (!HasProjectReference(WeaselCorePath, path))
            {
                DotNet($"add {WeaselCorePath} reference {path}");
            }
            else
            {
                Serilog.Log.Information($"Weasel.Core already references {projectName}; skipping reference add.");
            }
        }
    }

    private void removeProject(string repository, string projectName)
    {
        var path = Path.GetFullPath((string)(RootDirectory / ".." / repository / "src" / projectName / $"{projectName}.csproj"));

        if (Nugets.Contains(projectName))
        {
            if (HasProjectReference(WeaselCorePath, path))
            {
                DotNet($"remove {WeaselCorePath} reference {path}");
            }
            else
            {
                Serilog.Log.Information($"Weasel.Core does not reference {projectName}; skipping reference remove.");
            }
        }

        if (SolutionContainsProject(SolutionPath, path))
        {
            DotNet($"sln {SolutionPath} remove {path}");
        }
        else
        {
            Serilog.Log.Information($"Solution does not contain {projectName}; skipping sln remove.");
        }
    }

    /// <summary>
    /// Find any projects in the solution whose path points into ../{repository}/ and
    /// remove them. Catches dependencies that 'dotnet sln add' pulled in transitively
    /// when the primary project was added (e.g. JasperFx.SourceGeneration).
    /// </summary>
    private void removeOrphanedExternalProjects(string repository)
    {
        if (!File.Exists(SolutionPath)) return;

        var content = File.ReadAllText(SolutionPath);
        var slnDir = Path.GetDirectoryName(SolutionPath)!;

        // slnx uses <Project Path="..\repo\..." /> entries; legacy sln has Project(...) lines.
        // Match either by extracting any project path token containing /{repository}/ or \{repository}\.
        var orphans = new List<string>();
        var matches = System.Text.RegularExpressions.Regex.Matches(
            content, @"(?:Project\([^)]+\)\s*=\s*""[^""]+"",\s*""([^""]+)""|<Project\s+Path=""([^""]+)"")");
        foreach (System.Text.RegularExpressions.Match m in matches)
        {
            var relPath = m.Groups[1].Success ? m.Groups[1].Value : m.Groups[2].Value;
            var normalized = relPath.Replace('\\', '/');
            if (normalized.Contains($"/{repository}/", System.StringComparison.OrdinalIgnoreCase))
            {
                var fullPath = Path.GetFullPath(Path.Combine(slnDir, normalized));
                orphans.Add(fullPath);
            }
        }

        foreach (var orphan in orphans)
        {
            Serilog.Log.Information($"Removing orphaned external project from solution: {Path.GetFileName(orphan)}");
            DotNet($"sln {SolutionPath} remove {orphan}");
        }
    }

    private static bool SolutionContainsProject(string slnPath, string projectPath)
    {
        if (!File.Exists(slnPath)) return false;
        var fileName = Path.GetFileName(projectPath);
        return File.ReadAllText(slnPath).IndexOf(fileName, System.StringComparison.OrdinalIgnoreCase) >= 0;
    }

    private static bool HasProjectReference(string projectPath, string referencePath)
    {
        if (!File.Exists(projectPath)) return false;
        var fileName = Path.GetFileName(referencePath);
        return File.ReadAllText(projectPath).Contains(fileName);
    }

    private static bool HasPackageReference(string projectPath, string packageName)
    {
        if (!File.Exists(projectPath)) return false;
        var content = File.ReadAllText(projectPath);
        return content.Contains($"<PackageReference Include=\"{packageName}\"")
            || content.Contains($"<PackageReference Include='{packageName}'");
    }


    /// Support plugins are available for:
    /// - JetBrains ReSharper        https://nuke.build/resharper
    /// - JetBrains Rider            https://nuke.build/rider
    /// - Microsoft VisualStudio     https://nuke.build/visualstudio
    /// - Microsoft VSCode           https://nuke.build/vscode
    public static int Main() => Execute<Build>(x => x.Compile);
}
