using JasperFx.Core.Reflection;
using NSubstitute;
using NSubstitute.Core.Arguments;
using Shouldly;
using Spectre.Console;
using Spectre.Console.Rendering;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.CommandLine.Tests;

public class DatabaseResourceTests
{
    private readonly IDatabase theDatabase;
    private readonly DatabaseResource theResource;

    public DatabaseResourceTests()
    {
        theDatabase = Substitute.For<IDatabase>();
        theResource = new DatabaseResource(theDatabase);
    }

    [Fact]
    public async Task check_delegates()
    {
        await theResource.Check(CancellationToken.None);
        await theDatabase.Received().AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task optionally_delegates_on_clear_state()
    {
        var database = Substitute.For<IDatabase, IDatabaseWithRewindableState>();
        var resource = new DatabaseResource(database);
        var cancellationToken = CancellationToken.None;
        await resource.ClearState(cancellationToken);

        await database.As<IDatabaseWithRewindableState>().Received().ClearState(cancellationToken);
    }


    [Fact]
    public async Task optionally_delegates_on_statistics()
    {
        var database = Substitute.For<IDatabase, IDatabaseWithStatistics>();
        var resource = new DatabaseResource(database);
        var databaseWithStatistics = database.As<IDatabaseWithStatistics>();

        var cancellationToken = CancellationToken.None;

        await resource.DetermineStatus(cancellationToken);


        await databaseWithStatistics.Received().DetermineStatus(cancellationToken);
    }

    [Fact]
    public async Task setup_delegates()
    {
        await theResource.Setup(CancellationToken.None);
        await theDatabase.Received().ApplyAllConfiguredChangesToDatabaseAsync();
    }

    [Fact]
    public async Task determine_status_with_no_changes()
    {
        var delta = Substitute.For<ISchemaObjectDelta>();
        delta.Difference.Returns(SchemaPatchDifference.None);
        var migration = new SchemaMigration(delta);
        theDatabase.CreateMigrationAsync().Returns(migration);

        var markup = await theResource.DetermineStatus(CancellationToken.None);

        AnsiConsole.Record();
        try
        {
            AnsiConsole.Write(markup);
            AnsiConsole.ExportText().ShouldContain("Database matches the expected configuration");
        }
        finally
        {
            AnsiConsole.Reset();
        }
    }

    [Fact]
    public async Task determine_status_with_invalid_changes()
    {
        var delta = Substitute.For<ISchemaObjectDelta>();
        delta.Difference.Returns(SchemaPatchDifference.Invalid);
        var migration = new SchemaMigration(delta);
        theDatabase.CreateMigrationAsync().Returns(migration);

        var markup = await theResource.DetermineStatus(CancellationToken.None);

        AnsiConsole.Record();
        try
        {
            AnsiConsole.Write(markup);
            AnsiConsole.ExportText().ShouldContain("Cannot apply a detected database configuration change!");
        }
        finally
        {
            AnsiConsole.Reset();
        }
    }


    [Fact]
    public async Task determine_status_with_creates()
    {
        var delta = Substitute.For<ISchemaObjectDelta>();
        delta.Difference.Returns(SchemaPatchDifference.Create);
        var migration = new SchemaMigration(delta);
        theDatabase.CreateMigrationAsync().Returns(migration);

        var markup = await theResource.DetermineStatus(CancellationToken.None);

        AnsiConsole.Record();
        try
        {
            AnsiConsole.Write(markup);
            AnsiConsole.ExportText().ShouldContain("Missing database objects detected.");
        }
        finally
        {
            AnsiConsole.Reset();
        }
    }

    [Fact]
    public async Task determine_status_with_updates()
    {
        var delta = Substitute.For<ISchemaObjectDelta>();
        delta.Difference.Returns(SchemaPatchDifference.Update);
        var migration = new SchemaMigration(delta);
        theDatabase.CreateMigrationAsync().Returns(migration);

        var markup = await theResource.DetermineStatus(CancellationToken.None);

        AnsiConsole.Record();
        try
        {
            AnsiConsole.Write(markup);
            AnsiConsole.ExportText().ShouldContain("Database schema objects need to be updated.");
        }
        finally
        {
            AnsiConsole.Reset();
        }
    }
}
