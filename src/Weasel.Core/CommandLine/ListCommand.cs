using JasperFx.CommandLine;
using JasperFx.Core;
using JasperFx.Descriptors;
using Spectre.Console;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

[Description("List all database(s) based on the current configuration",
    Name = "db-list")]
public class ListCommand: JasperFxAsyncCommand<WeaselInput>
{
    public override async Task<bool> Execute(WeaselInput input)
    {
        JasperFxEnvironment.RunQuiet = true;

        AnsiConsole.Write(
            new FigletText("Weasel"){Justification = Justify.Left});

        using var host = input.BuildHost();

        var databases = await input.AllDatabases(host).ConfigureAwait(false);

        if (!databases.Any())
        {
            AnsiConsole.Write("No Weasel databases are configured for this application");
        }

        RenderDatabases(databases);

        return true;
    }

    internal static void RenderDatabases(List<IDatabase> databases)
    {
        var descriptors = new DatabaseDescriptor[databases.Count];
        for (int i = 0; i < databases.Count; i++)
        {
            descriptors[i] = databases[i].Describe();
        }

        var table = new Table();
        table.AddColumn("DatabaseUri");
        table.AddColumn("SubjectUri");

        bool hasTenants = false;

        if (descriptors.Any(x => x.TenantIds.Any()))
        {
            table.AddColumn("TenantId(s)");
        }

        foreach (var descriptor in descriptors)
        {
            var values = new List<string>();
            values.Add(descriptor.DatabaseUri().ToString());
            values.Add(descriptor.SubjectUri?.ToString() ?? string.Empty);

            if (hasTenants)
            {
                values.Add(descriptor.TenantIds.Join(", "));
            }

            table.AddRow(values.ToArray());
        }

        AnsiConsole.Write(table);
    }
}
