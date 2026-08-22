using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     weasel#499. A table whose schema arrived delimited was created once and then never migrated
///     again.
/// </summary>
/// <remarks>
///     <para>
///         <c>QualifiedNameParser</c> keeps the parts of a qualified name exactly as written, so
///         <c>"RoundTripMixed".things</c> reached the model with its quotes on. <c>ConfigureQueryCommand</c>
///         binds <c>Identifier.Schema</c> against <c>information_schema</c>, which holds the bare name,
///         so introspection came back empty and the table read as absent.
///     </para>
///     <para>
///         The delta was therefore <c>Create</c> on every run, and <c>Create</c> is emitted as
///         <c>CREATE TABLE IF NOT EXISTS</c> — which succeeds against the table that is already there
///         and does nothing. Every later column, index or constraint change was discarded with no
///         error and no warning, and <c>ApplyAllAsync</c> returned normally. That silence is what
///         makes this worth an integration test rather than a unit one: the unit-level contract is in
///         <c>object_name_normalization_conformance</c>, but only a real database shows the change
///         going missing.
///     </para>
/// </remarks>
[Collection("delimited_round_trip")]
public class delimited_schema_name_round_trip: IntegrationContext
{
    public delimited_schema_name_round_trip(): base("delimited_round_trip")
    {
    }

    private static Table Build(bool withSecondColumn)
    {
        var table = new Table(DbObjectName.Parse(PostgresqlProvider.Instance, "\"RoundTripMixed\".things"));
        table.AddColumn<int>("id").AsPrimaryKey();
        if (withSecondColumn)
        {
            table.AddColumn<string>("added_later").AllowNulls();
        }

        return table;
    }

    private async Task DropTheSchemaAsync()
    {
        await theConnection.CreateCommand("DROP SCHEMA IF EXISTS \"RoundTripMixed\" CASCADE;").ExecuteNonQueryAsync();
    }

    /// <summary>
    ///     The model has to hold what the catalog reports, which is the bare name.
    /// </summary>
    [Fact]
    public void the_schema_is_held_without_its_delimiters()
    {
        Build(false).Identifier.Schema.ShouldBe("RoundTripMixed");
    }

    [Fact]
    public async Task a_second_check_reports_nothing_to_do()
    {
        await theConnection.OpenAsync();
        await DropTheSchemaAsync();

        var first = await SchemaMigration.DetermineAsync(theConnection, Build(false));
        first.Difference.ShouldBe(SchemaPatchDifference.Create);

        await new PostgresqlMigrator().ApplyAllAsync(theConnection, first, AutoCreate.CreateOrUpdate);

        // Pre-fix this was Create again, forever.
        var second = await SchemaMigration.DetermineAsync(theConnection, Build(false));
        second.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The one that matters: a change made after the table exists has to reach the database.
    /// </summary>
    [Fact]
    public async Task a_later_change_is_applied_rather_than_silently_discarded()
    {
        await theConnection.OpenAsync();
        await DropTheSchemaAsync();

        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, Build(false)), AutoCreate.CreateOrUpdate);

        var migration = await SchemaMigration.DetermineAsync(theConnection, Build(true));

        // Pre-fix: Create, written as CREATE TABLE IF NOT EXISTS, which did nothing at all.
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        await new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        var count = await theConnection.CreateCommand(
                "select count(*) from information_schema.columns where table_schema = 'RoundTripMixed' "
                + "and table_name = 'things' and column_name = 'added_later'")
            .ExecuteScalarAsync();

        Convert.ToInt32(count).ShouldBe(1, "the added column never reached the database");
    }
}
