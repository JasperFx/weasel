using JasperFx.Resources;
using Spectre.Console;
using Spectre.Console.Rendering;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

/// <summary>
/// Exposes optional status information about a database to the JasperFx
/// command line report "resources statistics" model
/// </summary>
public interface IDatabaseWithStatistics
{
    Task<IRenderable> DetermineStatus(CancellationToken token);
}

/// <summary>
/// Exposes optional status information about a database to the JasperFx
/// command line report "resources clear" model
/// </summary>
public interface IDatabaseWithRewindableState
{
    Task ClearState(CancellationToken token);
}

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
        return _database.AssertDatabaseMatchesConfigurationAsync(token);
    }

    public Task ClearState(CancellationToken token)
    {
        if (_database is IDatabaseWithRewindableState d) return d.ClearState(token);
        return Task.CompletedTask;
    }

    public Task Teardown(CancellationToken token)
    {
        return Task.CompletedTask;
    }

    public Task Setup(CancellationToken token)
    {
        return _database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);
    }

    public async Task<IRenderable> DetermineStatus(CancellationToken token)
    {
        if (_database is IDatabaseWithStatistics d) return await d.DetermineStatus(token).ConfigureAwait(false);

        var migration = await _database.CreateMigrationAsync(token).ConfigureAwait(false);
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
