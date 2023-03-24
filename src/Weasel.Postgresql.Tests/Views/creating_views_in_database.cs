using Shouldly;
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

        var view = new View("people", "SELECT 1 AS id");
        await CreateSchemaObjectInDatabase(view);

        (await view.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();

        await theConnection.CreateCommand(
                "SELECT * FROM views.people")
            .ExecuteNonQueryAsync();
    }


    [Fact]
    public async Task create_then_drop()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("views");

        var view = new View("people", "SELECT 1 AS id");
        await CreateSchemaObjectInDatabase(view);
        await DropSchemaObjectInDatabase(view);

        (await view.ExistsInDatabaseAsync(theConnection))
            .ShouldBeFalse();
    }
}
