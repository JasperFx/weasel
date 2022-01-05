using Baseline;
using Oakton;
using Spectre.Console;
using Weasel.Core.Migrations;

namespace Weasel.CommandLine;

public class DumpInput: WeaselInput
{
    [Description("File (or folder) location to write the SQL file")]
    public string Path { get; set; }

    [Description("Opt into writing the SQL split out by features into separate files")]
    [FlagAlias("by-feature", 'f')]
    public bool ByFeatureFlag { get; set; }

    [Description("Option to create scripts as transactional script")]
    [FlagAlias("transactional-script")]
    public bool TransactionalScriptFlag { get; set; }



}

[Description("Dumps the entire DDL for the configured Marten database", Name = "db-dump")]
public class DumpCommand: OaktonCommand<DumpInput>
{
    public DumpCommand()
    {
        Usage("Writes the complete DDL for the entire Marten configuration to the named file")
            .Arguments(x => x.Path);
    }

    public override bool Execute(DumpInput input)
    {
        using var host = input.BuildHost();

        if (!input.TryChooseSingleDatabase(host, out var database))
        {
            return false;
        }

        // This can only override to true
        if (input.TransactionalScriptFlag)
        {
            database.Migrator.IsTransactional = true;
        }

        if (input.ByFeatureFlag)
        {
            writeByType(input, database);
        }
        else
        {
            AnsiConsole.MarkupLine("Writing SQL file to " + input.Path);

            database.WriteCreationScriptToFile(input.Path);
        }

        return true;

    }

    private static void writeByType(DumpInput input, IDatabase? database)
    {
        // You only need to clean out the existing folder when dumping
        // by type
        try
        {
            if (Directory.Exists(input.Path))
            {
                new FileSystem().CleanDirectory(input.Path);
            }
        }
        catch (Exception)
        {
            AnsiConsole.Write($"[yellow]Unable to clean the directory at {input.Path} before writing new files[/]");
        }

        AnsiConsole.WriteLine("Writing SQL files to " + input.Path);
        database.WriteDatabaseCreationScriptByType(input.Path);


    }
}
