using JasperFx;
using JasperFx.CommandLine;
using JasperFx.Core;
using Spectre.Console;

namespace Weasel.Core.CommandLine;

[Description(
    "Evaluates the current configuration against the database and writes a patch and drop file if there are any differences",
    Name = "db-patch"
)]
public class PatchCommand: JasperFxAsyncCommand<PatchInput>
{
    public PatchCommand()
    {
        Usage("Write the patch and matching drop file").Arguments(x => x.FileName);
    }

    public override async Task<bool> Execute(PatchInput input)
    {
        using var host = input.BuildHost();

        var (found, database) = await input.TryChooseSingleDatabase(host).ConfigureAwait(false);
        if (!found) return false;

        var migration = await database!.CreateMigrationAsync().ConfigureAwait(false);
        if (migration.Difference == SchemaPatchDifference.None)
        {
            AnsiConsole.MarkupLine(
                "[green]No differences were detected between the configuration and the actual database[/]");
            return true;
        }

        migration.AssertPatchingIsValid(input.AutoCreateFlag);

        if (input.TransactionalScriptFlag)
        {
            database.Migrator.IsTransactional = true;
        }

        var fullPathToFile = input.FileName.ToFullPath();
        await database!.Migrator.WriteMigrationFileAsync(fullPathToFile, migration).ConfigureAwait(false);
        AnsiConsole.MarkupLine($"[green]Wrote migration file to {fullPathToFile}[/]");

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
