using JasperFx;
using Shouldly;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     weasel#598, the introspection half. Catalog reads are privilege-filtered on every provider,
///     so an object the connection's role cannot see is indistinguishable from one that is not
///     there. A restricted role therefore reads back an empty schema, Weasel concludes that every
///     object is missing, and <see cref="DatabaseValidationException" /> lists the entire
///     configuration as absent -- which reads as "the database is empty" when it is not. The assert
///     path now says so, in the one shape that can mean it.
/// </summary>
[Collection("total_absence")]
public class total_absence_note_on_assert: IntegrationContext
{
    public total_absence_note_on_assert(): base("total_absence")
    {
    }

    [Fact]
    public async Task says_so_when_every_configured_object_is_missing()
    {
        await ResetSchema();

        var db = new DatabaseWithTables("total_absence", theDataSource, AutoCreate.None);
        db.AddTable(new PostgresqlObjectName(SchemaName, "one")).AddPrimaryKeyColumn("id", typeof(int));
        db.AddTable(new PostgresqlObjectName(SchemaName, "two")).AddPrimaryKeyColumn("id", typeof(int));

        var exception = await Should.ThrowAsync<DatabaseValidationException>(
            () => db.AssertDatabaseMatchesConfigurationAsync());

        exception.Message.ShouldContain("all 2 of the configured objects are reported as missing");
        exception.Message.ShouldContain("privileges");

        // And the DDL is still the body of the message -- the note is an addition, not a
        // replacement.
        exception.Message.ShouldContain("CREATE TABLE");
    }

    [Fact]
    public async Task stays_quiet_when_only_some_of_the_configuration_is_missing()
    {
        await ResetSchema();

        var applied = new DatabaseWithTables("total_absence", theDataSource, AutoCreate.All);
        applied.AddTable(new PostgresqlObjectName(SchemaName, "one")).AddPrimaryKeyColumn("id", typeof(int));
        await applied.ApplyAllConfiguredChangesToDatabaseAsync();

        var db = new DatabaseWithTables("total_absence", theDataSource, AutoCreate.None);
        db.AddTable(new PostgresqlObjectName(SchemaName, "one")).AddPrimaryKeyColumn("id", typeof(int));
        db.AddTable(new PostgresqlObjectName(SchemaName, "two")).AddPrimaryKeyColumn("id", typeof(int));

        var exception = await Should.ThrowAsync<DatabaseValidationException>(
            () => db.AssertDatabaseMatchesConfigurationAsync());

        // A partially-migrated database is ordinary drift, and the note would be noise on it.
        exception.Message.ShouldNotContain("reported as missing");
    }
}
