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
            DotNet($"remove {weaselCore} package {nuget}");
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
        }

        var weaselCore = Solution.GetProject("Weasel.Core").Path;
        foreach (var nuget in Nugets)
        {
            DotNet($"add {weaselCore} package {nuget} --prerelease");
        }
    });

    private void addProject(string repository, string projectName)
    {
        var path =  Path.GetFullPath($"../{repository}/src/{projectName}/{projectName}.csproj");;
        var slnPath = Solution.Path;
        DotNet($"sln {slnPath} add {path} --solution-folder Attached");

        if (Nugets.Contains(projectName))
        {
            var weaselCore = Solution.GetProject("Weasel.Core").Path;
            DotNet($"add {weaselCore} reference {path}");
        }
    }

    private void removeProject(string repository, string projectName)
    {
        var path =  Path.GetFullPath($"../{repository}/src/{projectName}/{projectName}.csproj");

        if (Nugets.Contains(projectName))
        {
            var marten = Solution.GetProject("Marten").Path;
            DotNet($"remove {marten} reference {path}");
        }

        var slnPath = Solution.Path;
        DotNet($"sln {slnPath} remove {path}");


    }


    /// Support plugins are available for:
    /// - JetBrains ReSharper        https://nuke.build/resharper
    /// - JetBrains Rider            https://nuke.build/rider
    /// - Microsoft VisualStudio     https://nuke.build/visualstudio
    /// - Microsoft VSCode           https://nuke.build/vscode
    public static int Main() => Execute<Build>(x => x.Compile);
}
