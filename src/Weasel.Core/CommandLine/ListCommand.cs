using JasperFx.CommandLine;
using JasperFx.Core;
using Spectre.Console;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

[Description("List all database(s) based on the current configuration",
    Name = "db-list")]
public class ListCommand: JasperFxAsyncCommand<WeaselInput>
{
    public override async Task<bool> Execute(WeaselInput input)
    {
        AnsiConsole.Write(
            new FigletText("Weasel"){Justification = Justify.Left});

        using var host = input.BuildHost();

        var databases = await input.AllDatabases(host).ConfigureAwait(false);

        if (!databases.Any())
        {
            AnsiConsole.Write("No Weasel databases are configured for this application");
        }

        var descriptors = databases.Select(x => x.Describe()).ToArray();

        var table = new Table();
        table.AddColumn(nameof(IDatabase.Identifier));
        table.AddColumn("DatabaseUri");
        bool hasTenants = false;

        if (descriptors.Any(x => x.TenantIds.Any()))
        {
            table.AddColumn("TenantId(s)");
        }

        foreach (var descriptor in descriptors)
        {
            var values = new List<string>();
            values.Add(descriptor.Identifier);
            values.Add(descriptor.DatabaseUri().ToString());

            if (hasTenants)
            {
                values.Add(descriptor.TenantIds.Join(", "));
            }

            table.AddRow(values.ToArray());
        }

        AnsiConsole.Write(table);

        return true;
    }
}
