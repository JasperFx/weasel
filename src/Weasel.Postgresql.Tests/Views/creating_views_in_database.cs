using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Views;
using Xunit;

namespace Weasel.Postgresql.Tests.Views;

[Collection("views")]
public class creating_views_in_database: IntegrationContext
{
    public creating_views_in_database(): base("views")
    {
    }


    [Fact]
    public async Task create_view_in_the_database()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("views");

        var view = new View("views.people_view", "SELECT 1 AS id");
        await CreateSchemaObjectInDatabase(view);

        (await view.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();

        await theConnection.CreateCommand(
                "SELECT * FROM views.people_view")
            .ExecuteNonQueryAsync();
    }


    [Fact]
    public async Task create_then_drop()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("views");

        var view = new View("views.people_view", "SELECT 1 AS id");
        await CreateSchemaObjectInDatabase(view);
        await DropSchemaObjectInDatabase(view);

        (await view.ExistsInDatabaseAsync(theConnection))
            .ShouldBeFalse();
    }


    [Fact]
    public async Task after_creating_view_no_pending_changes(){
        await theConnection.OpenAsync();
        await theConnection.ResetSchemaAsync("views");
        var view = new View("views.people_view", "SELECT 1 AS id");
        await CreateSchemaObjectInDatabase(view);
        var builder = new DbCommandBuilder(theConnection);
        view.ConfigureQueryCommand(builder);
        await using var reader = await theConnection.ExecuteReaderAsync(builder);
        var delta = await view.CreateDeltaAsync(reader, CancellationToken.None);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
