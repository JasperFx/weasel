using Oakton.Resources;
using Spectre.Console;
using Spectre.Console.Rendering;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.CommandLine;

internal class DatabaseResource: IStatefulResource
{
    private readonly IDatabase _database;

    public DatabaseResource(IDatabase database)
    {
        _database = database;
        Type = "WeaselDatabase";
        Name = database.Identifier;
    }

    public Task Check(CancellationToken token)
    {
        return _database.AssertDatabaseMatchesConfigurationAsync();
    }

    public Task ClearState(CancellationToken token)
    {
        return Task.CompletedTask;
    }

    public Task Teardown(CancellationToken token)
    {
        return Task.CompletedTask;
    }

    public Task Setup(CancellationToken token)
    {
        return _database.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    public async Task<IRenderable> DetermineStatus(CancellationToken token)
    {
        var migration = await _database.CreateMigrationAsync();
        switch (migration.Difference)
        {
            case SchemaPatchDifference.None:
                return new Markup("[green]Database matches the expected configuration[/]");

            case SchemaPatchDifference.Invalid:
                return new Markup("[red]Cannot apply a detected database configuration change![/]");

            case SchemaPatchDifference.Create:
                return new Markup("[yellow]Missing database objects detected.[/]");

            case SchemaPatchDifference.Update:
                return new Markup("[yellow]Database schema objects need to be updated.[/]");

            default:
                throw new NotSupportedException(); // can't get here, but compiler
        }
    }

    public string Type { get; }
    public string Name { get; }
}
