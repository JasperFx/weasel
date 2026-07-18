using JasperFx;
using JasperFx.CommandLine;
using JasperFx.Core;
using Spectre.Console;
using Weasel.Core.CommandLine;

namespace Weasel.EntityFrameworkCore.CommandLine;

public class EfMigrationInput : WeaselInput
{
    [Description("What to do: add | script | baseline")]
    public string Action { get; set; } = string.Empty;

    [Description("The migration name (required for add)")]
    public string? Name { get; set; }

    [Description("Output directory for the generated files. Default is ./WeaselMigrations")]
    [FlagAlias("output", 'o')]
    public string? OutputFlag { get; set; }

    [Description("Namespace for the generated files. Default is WeaselMigrations")]
    [FlagAlias("namespace")]
    public string? NamespaceFlag { get; set; }

    [Description("Stub DbContext type name. Default is derived from the database identifier")]
    [FlagAlias("context")]
    public string? ContextFlag { get; set; }

    [Description("Schema for the relocated __EFMigrationsHistory table. Default is the first non-default schema of the database's objects")]
    [FlagAlias("history-schema")]
    public string? HistorySchemaFlag { get; set; }

    [Description("Diff against the live database (Weasel delta detection wrapped in Sql operations) instead of the serialized snapshot")]
    [FlagAlias("against-database")]
    public bool AgainstDatabaseFlag { get; set; }

    internal EfMigrationGenerationOptions ToOptions()
    {
        var options = new EfMigrationGenerationOptions();
        if (OutputFlag.IsNotEmpty())
        {
            options.Directory = OutputFlag!;
        }

        if (NamespaceFlag.IsNotEmpty())
        {
            options.Namespace = NamespaceFlag!;
        }

        options.ContextTypeName = ContextFlag;
        options.HistorySchema = HistorySchemaFlag;
        return options;
    }
}

[Description(
    "Generates and manages EF Core migrations for the Weasel-managed schema objects of an IDatabase",
    Name = "db-ef-migration")]
public class EfMigrationCommand : JasperFxAsyncCommand<EfMigrationInput>
{
    public EfMigrationCommand()
    {
        Usage("Generate the next migration").Arguments(x => x.Action, x => x.Name);
        Usage("Baseline an existing database or print scripting guidance").Arguments(x => x.Action);
    }

    public override async Task<bool> Execute(EfMigrationInput input)
    {
        JasperFxEnvironment.RunQuiet = true;

        AnsiConsole.Write(new FigletText("Weasel") { Justification = Justify.Left });

        using var host = input.BuildHost();

        var (found, database) = await input.TryChooseSingleDatabase(host).ConfigureAwait(false);
        if (!found)
        {
            return false;
        }

        var options = input.ToOptions();

        switch (input.Action.ToLowerInvariant())
        {
            case "add":
            {
                if (input.Name.IsEmpty())
                {
                    AnsiConsole.MarkupLine("[red]A migration name is required: db-ef-migration add <Name>[/]");
                    return false;
                }

                var result = await EfMigrationGenerator
                    .AddAsync(database!, input.Name!, options, input.AgainstDatabaseFlag)
                    .ConfigureAwait(false);

                if (!result.HasChanges)
                {
                    AnsiConsole.MarkupLine(
                        "[green]No differences were detected between the model and the snapshot — no migration generated[/]");
                    return true;
                }

                AnsiConsole.MarkupLine($"[green]Wrote migration {result.MigrationId} to {result.MigrationFile}[/]");
                if (result.ContextFile != null)
                {
                    AnsiConsole.MarkupLine($"[green]Wrote stub DbContext to {result.ContextFile}[/]");
                }

                AnsiConsole.MarkupLine($"[green]Updated schema snapshot at {result.SnapshotFile}[/]");
                return true;
            }

            case "script":
            {
                // the canonical idempotent script comes from the EF toolchain
                // itself once the generated files are compiled into a project —
                // the #364 spike verified this end to end
                var contextName = options.ContextTypeName ?? "<StubDbContext>";
                AnsiConsole.MarkupLine(
                    "[yellow]Idempotent SQL scripts are produced by the EF toolchain from the generated migration files:[/]");
                AnsiConsole.MarkupLine(
                    $"    dotnet ef migrations script --idempotent --context {contextName} -o migrations.sql");
                AnsiConsole.MarkupLine(
                    "[yellow]Run it in the project that compiles the generated files. Migration bundles " +
                    "(dotnet ef migrations bundle) work the same way.[/]");
                return true;
            }

            case "baseline":
            {
                var recorded = await EfMigrationGenerator.BaselineAsync(database!, options).ConfigureAwait(false);

                if (!recorded.Any())
                {
                    AnsiConsole.MarkupLine(
                        $"[yellow]No new migration files found in {options.Directory} to baseline (already recorded or none generated)[/]");
                    return true;
                }

                foreach (var migrationId in recorded)
                {
                    AnsiConsole.MarkupLine($"[green]Recorded {migrationId} as applied[/]");
                }

                return true;
            }

            default:
                AnsiConsole.MarkupLine(
                    $"[red]Unknown action '{input.Action}'. Use add, script, or baseline[/]");
                return false;
        }
    }
}
