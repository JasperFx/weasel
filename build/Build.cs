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

    [Solution] readonly Solution Solution;

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
                .SetProjectFile(Solution)
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

        var weaselCore = Solution.GetProject("Weasel.Core").Path;
        foreach (var nuget in Nugets)
        {
            if (HasPackageReference(weaselCore, nuget))
            {
                DotNet($"remove {weaselCore} package {nuget}");
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

        var weaselCore = Solution.GetProject("Weasel.Core").Path;
        foreach (var nuget in Nugets)
        {
            if (HasPackageReference(weaselCore, nuget))
            {
                Serilog.Log.Information($"Package {nuget} already present on Weasel.Core; skipping.");
            }
            else
            {
                DotNet($"add {weaselCore} package {nuget} --prerelease");
            }
        }
    });

    private void addProject(string repository, string projectName)
    {
        var path = Path.GetFullPath($"../{repository}/src/{projectName}/{projectName}.csproj");
        if (!File.Exists(path))
        {
            Assert.Fail($"Cannot attach: project '{path}' does not exist. Ensure ../{repository} is cloned next to this repo.");
        }

        var slnPath = Solution.Path;
        if (!SolutionContainsProject(slnPath, path))
        {
            DotNet($"sln {slnPath} add {path} --solution-folder Attached");
        }
        else
        {
            Serilog.Log.Information($"Solution already contains {projectName}; skipping sln add.");
        }

        if (Nugets.Contains(projectName))
        {
            var weaselCore = Solution.GetProject("Weasel.Core").Path;
            if (!HasProjectReference(weaselCore, path))
            {
                DotNet($"add {weaselCore} reference {path}");
            }
            else
            {
                Serilog.Log.Information($"Weasel.Core already references {projectName}; skipping reference add.");
            }
        }
    }

    private void removeProject(string repository, string projectName)
    {
        var path = Path.GetFullPath($"../{repository}/src/{projectName}/{projectName}.csproj");

        if (Nugets.Contains(projectName))
        {
            var weaselCore = Solution.GetProject("Weasel.Core").Path;
            if (HasProjectReference(weaselCore, path))
            {
                DotNet($"remove {weaselCore} reference {path}");
            }
            else
            {
                Serilog.Log.Information($"Weasel.Core does not reference {projectName}; skipping reference remove.");
            }
        }

        var slnPath = Solution.Path;
        if (SolutionContainsProject(slnPath, path))
        {
            DotNet($"sln {slnPath} remove {path}");
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
        var slnPath = Solution.Path;
        if (!File.Exists(slnPath)) return;

        var content = File.ReadAllText(slnPath);
        var token = $"..\\{repository}\\";
        var altToken = $"../{repository}/";

        // Pull project lines and look for paths matching the external repo
        var lines = content.Split('\n');
        var orphans = new List<string>();
        foreach (var line in lines)
        {
            if (!line.StartsWith("Project(", System.StringComparison.Ordinal)) continue;
            if (line.IndexOf(token, System.StringComparison.Ordinal) < 0
                && line.IndexOf(altToken, System.StringComparison.Ordinal) < 0)
                continue;

            // Extract the path between the second and third quoted segments
            var quoteSegments = line.Split('"');
            if (quoteSegments.Length < 6) continue;
            var relPath = quoteSegments[5];
            var fullPath = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(slnPath)!, relPath.Replace('\\', '/')));
            orphans.Add(fullPath);
        }

        foreach (var orphan in orphans)
        {
            Serilog.Log.Information($"Removing orphaned external project from solution: {Path.GetFileName(orphan)}");
            DotNet($"sln {slnPath} remove {orphan}");
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
