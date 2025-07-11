using JasperFx.CommandLine;
using JasperFx.Core;
using Spectre.Console;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

public class DumpInput: WeaselInput
{
    [Description("File (or folder) location to write the SQL file")]
    public string Path { get; set; } = string.Empty;

    [Description("Opt into writing the SQL split out by features into separate files")]
    [FlagAlias("by-feature", 'f')]
    public bool ByFeatureFlag { get; set; } = false;

    [Description("Option to create scripts as transactional script")]
    [FlagAlias("transactional-script")]
    public bool TransactionalScriptFlag { get; set; } = false;
}

[Description("Dumps the entire DDL for the configured Marten database", Name = "db-dump")]
public class DumpCommand: JasperFxAsyncCommand<DumpInput>
{
    public DumpCommand()
    {
        Usage("Writes the complete DDL for the entire Marten configuration to the named file")
            .Arguments(x => x.Path);
    }

    public override async Task<bool> Execute(DumpInput input)
    {
        JasperFxEnvironment.RunQuiet = true;

        AnsiConsole.Write(
            new FigletText("Weasel"){Justification = Justify.Left});

        using var host = input.BuildHost();

        var (found, database) = await input.TryChooseSingleDatabase(host).ConfigureAwait(false);
        if (!found) return false;

        // This can only override to true
        if (input.TransactionalScriptFlag)
        {
            database!.Migrator.IsTransactional = true;
        }

        if (input.ByFeatureFlag)
        {
            await writeByType(input, database).ConfigureAwait(false);
        }
        else
        {
            AnsiConsole.MarkupLine("Writing SQL file to " + input.Path);

            await database!.WriteCreationScriptToFileAsync(input.Path).ConfigureAwait(false);
        }

        return true;
    }

    private static Task writeByType(DumpInput input, IDatabase? database, CancellationToken ct = default)
    {
        // You only need to clean out the existing folder when dumping
        // by type
        try
        {
            if (Directory.Exists(input.Path))
            {
                FileSystem.CleanDirectory(input.Path);
            }
        }
        catch (Exception)
        {
            AnsiConsole.Write($"[yellow]Unable to clean the directory at {input.Path} before writing new files[/]");
        }

        AnsiConsole.WriteLine("Writing SQL files to " + input.Path);
        return database.WriteScriptsByTypeAsync(input.Path, ct);
    }
}
