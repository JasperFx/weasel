using NSubstitute;
using Shouldly;
using Spectre.Console;
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
