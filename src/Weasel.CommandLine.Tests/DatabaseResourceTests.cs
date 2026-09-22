using JasperFx.Core;
using JasperFx.Core.Reflection;
using JasperFx.Descriptors;
using NSubstitute;
using Shouldly;
using Spectre.Console;
using Spectre.Console.Rendering;
using Weasel.Core;
using Weasel.Core.CommandLine;
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
        theDatabase.Describe().Returns(new DatabaseDescriptor
        {
            DatabaseName = "Foo", ServerName = "server1", SchemaOrNamespace = "schema1", Engine = "postgresql"
        });

        theResource = new DatabaseResource(theDatabase, "marten://db".ToUri());
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
        database.Describe().Returns(new DatabaseDescriptor
        {
            DatabaseName = "Foo", ServerName = "server1", SchemaOrNamespace = "schema1", Engine = "postgresql"
        });

        var resource = new DatabaseResource(database, "marten://db".ToUri());
        var cancellationToken = CancellationToken.None;
        await resource.ClearState(cancellationToken);

        await database.As<IDatabaseWithRewindableState>().Received().ClearState(cancellationToken);
    }


    [Fact]
    public async Task optionally_delegates_on_statistics()
    {
        var database = Substitute.For<IDatabase, IDatabaseWithStatistics>();
        database.Describe().Returns(new DatabaseDescriptor
        {
            DatabaseName = "Foo", ServerName = "server1", SchemaOrNamespace = "schema1", Engine = "postgresql"
        });

        var resource = new DatabaseResource(database, "marten://db".ToUri());
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

        Render(markup).ShouldContain("Database matches the expected configuration");
    }

    /// <summary>
    ///     weasel#600. This used to assert the bare headline "Cannot apply a detected database
    ///     configuration change!", which is only half true: under <c>AutoCreate.All</c> the change
    ///     <em>is</em> applied, by dropping and recreating the object and taking its rows with it.
    ///     <c>resources check</c> now names the object, the reason, and what <c>All</c> would do
    ///     with it -- before anything runs, which is the only moment the warning is useful.
    /// </summary>
    [Fact]
    public async Task determine_status_with_invalid_changes()
    {
        var schemaObject = Substitute.For<ISchemaObject>();
        schemaObject.Identifier.Returns(new DbObjectName("things", "documents"));

        var delta = Substitute.For<ISchemaObjectDelta>();
        delta.Difference.Returns(SchemaPatchDifference.Invalid);
        delta.SchemaObject.Returns(schemaObject);

        var migration = new SchemaMigration(delta);
        theDatabase.CreateMigrationAsync().Returns(migration);

        var markup = await theResource.DetermineStatus(CancellationToken.None);

        var text = Render(markup);

        text.ShouldContain("Cannot apply a detected database configuration change incrementally!");
        text.ShouldContain("things.documents would be dropped and recreated");
        text.ShouldContain("any rows in it would be lost");
        text.ShouldContain("Under AutoCreate.All");
    }

    /// <summary>
    ///     A delta that reports Invalid but can rebuild in place loses no data, so it gets the
    ///     plain headline rather than the data-loss warning.
    /// </summary>
    [Fact]
    public async Task determine_status_with_an_invalid_change_that_rebuilds_in_place()
    {
        var delta = Substitute.For<ISchemaObjectDelta, ISchemaObjectDeltaWithRebuild>();
        delta.Difference.Returns(SchemaPatchDifference.Invalid);
        ((ISchemaObjectDeltaWithRebuild)delta).CanRebuildInPlace.Returns(true);

        var migration = new SchemaMigration(delta);
        theDatabase.CreateMigrationAsync().Returns(migration);

        var markup = await theResource.DetermineStatus(CancellationToken.None);

        var text = Render(markup);

        text.ShouldContain("Cannot apply a detected database configuration change!");
        text.ShouldNotContain("dropped and recreated");
    }


    [Fact]
    public async Task determine_status_with_creates()
    {
        var delta = Substitute.For<ISchemaObjectDelta>();
        delta.Difference.Returns(SchemaPatchDifference.Create);
        var migration = new SchemaMigration(delta);
        theDatabase.CreateMigrationAsync().Returns(migration);

        var markup = await theResource.DetermineStatus(CancellationToken.None);

        Render(markup).ShouldContain("Missing database objects detected.");
    }

    [Fact]
    public async Task determine_status_with_updates()
    {
        var delta = Substitute.For<ISchemaObjectDelta>();
        delta.Difference.Returns(SchemaPatchDifference.Update);
        var migration = new SchemaMigration(delta);
        theDatabase.CreateMigrationAsync().Returns(migration);

        var markup = await theResource.DetermineStatus(CancellationToken.None);

        Render(markup).ShouldContain("Database schema objects need to be updated.");
    }

    /// <summary>
    ///     Render to a console of this test's own rather than <see cref="AnsiConsole" />'s global
    ///     recorder. The recorder is process-wide state: another test in this assembly writing to
    ///     the console while one is recording both pollutes the captured text and can throw
    ///     "Collection was modified" straight out of Spectre's encoder. xUnit parallelises this
    ///     assembly, so that is a race, not a possibility.
    /// </summary>
    private static string Render(IRenderable renderable)
    {
        var buffer = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Out = new AnsiConsoleOutput(buffer)
        });

        console.Write(renderable);
        return buffer.ToString();
    }

}
