using Baseline;
using Oakton;
using Spectre.Console;
using Weasel.Core;

namespace Weasel.CommandLine;

[Description(
    "Evaluates the current configuration against the database and writes a patch and drop file if there are any differences", Name = "db-patch"
)]
public class PatchCommand : OaktonAsyncCommand<PatchInput>
{
    public PatchCommand()
    {
        Usage("Write the patch and matching drop file").Arguments(x => x.FileName);
    }

    public override async Task<bool> Execute(PatchInput input)
    {
        using var host = input.BuildHost();

        if (!input.TryChooseSingleDatabase(host, out var database))
        {
            return false;
        }

        var migration = await database.CreateMigrationAsync();
        if (migration.Difference == SchemaPatchDifference.None)
        {
            AnsiConsole.MarkupLine("[green]No differences were detected between the configuration and the actual database[/]");
            return true;
        }

        migration.AssertPatchingIsValid(input.AutoCreateFlag);

        if (input.TransactionalScriptFlag)
        {
            database.Migrator.IsTransactional = true;
        }

        await database.Migrator.WriteMigrationFile(input.FileName, migration);
        AnsiConsole.MarkupLine($"[green]Wrote migration file to {input.FileName.ToFullPath()}[/]");

        return true;
    }
}

public class PatchInput: WeaselInput
{
    [Description("File path where the patch should be written")]
    public string FileName { get; set; } = string.Empty;


    [Description("Override the AutoCreate behavior. Default is CreateOrUpdate")]
    public AutoCreate AutoCreateFlag { get; set; } = AutoCreate.CreateOrUpdate;

    [Description("Option to create scripts as transactional script")]
    [FlagAlias("transactional-script")]
    public bool TransactionalScriptFlag { get; set; } = false;
}
