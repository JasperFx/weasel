using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Postgresql.Tests.Migrations;

/// <summary>
/// Opt-in schema fingerprinting (Migrator.UseSchemaFingerprinting): a successful FULL apply stamps a
/// fingerprint of the configured schema's expected DDL; the next apply with an unchanged configuration
/// skips all catalog introspection (a single SELECT). The stamp is TRUSTED — drift applied outside
/// Weasel is not detected while it matches; AssertDatabaseMatchesConfigurationAsync remains the
/// verification route. Any configuration change (new table/column/partition) changes the hash and
/// re-enables the full apply.
/// </summary>
[Collection("fingerprint")]
public class schema_fingerprint_tests: IntegrationContext, IAsyncLifetime
{
    private readonly TestDatabaseWithTables theDatabase;

    public schema_fingerprint_tests(): base("fingerprint")
    {
        theDatabase = new TestDatabaseWithTables(AutoCreate.CreateOrUpdate, "Fingerprint", theDataSource);
    }

    public override async Task InitializeAsync()
    {
        await ResetSchema();

        // The stamp lives in the migrator's default schema ('public') — clear it so tests are
        // order-independent.
        await theConnection.CreateCommand("drop table if exists public.weasel_schema_fingerprint")
            .ExecuteNonQueryAsync();
    }

    private async Task<string?> readStampAsync()
    {
        try
        {
            return await theConnection
                .CreateCommand("select fingerprint from public.weasel_schema_fingerprint where id = 1")
                .ExecuteScalarAsync() as string;
        }
        catch (Npgsql.PostgresException e) when (e.SqlState == Npgsql.PostgresErrorCodes.UndefinedTable)
        {
            return null;
        }
    }

    private async Task<bool> tableExistsAsync(string qualifiedName)
    {
        var result = await theConnection
            .CreateCommand($"select to_regclass('{qualifiedName}') is not null")
            .ExecuteScalarAsync();
        return result is true;
    }

    [Fact]
    public async Task full_apply_stamps_and_a_matching_stamp_skips_the_apply_entirely()
    {
        theDatabase.Migrator.UseSchemaFingerprinting = true;
        theDatabase.Features["One"].AddTable(SchemaName, "one");

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var stamp = await readStampAsync();
        stamp.ShouldNotBeNull();
        (await tableExistsAsync($"{SchemaName}.one")).ShouldBeTrue();

        // Drift outside Weasel: drop the table manually. While the stamp matches the configuration,
        // the next apply is a no-op single SELECT — the table must NOT come back. That is the proof
        // the introspection was skipped, and the documented trust semantics of the stamp.
        await theConnection.CreateCommand($"drop table {SchemaName}.one").ExecuteNonQueryAsync();

        var difference = await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        difference.ShouldBe(SchemaPatchDifference.None);
        (await tableExistsAsync($"{SchemaName}.one")).ShouldBeFalse();

        // The verification route is unaffected: asserting the configuration still detects the drift.
        await Should.ThrowAsync<DatabaseValidationException>(
            () => theDatabase.AssertDatabaseMatchesConfigurationAsync());
    }

    [Fact]
    public async Task a_configuration_change_invalidates_the_stamp_and_reapplies()
    {
        theDatabase.Migrator.UseSchemaFingerprinting = true;
        theDatabase.Features["One"].AddTable(SchemaName, "one");

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        var firstStamp = await readStampAsync();
        firstStamp.ShouldNotBeNull();

        // New object in the configuration -> different fingerprint -> the apply runs for real.
        theDatabase.Features["One"].AddTable(SchemaName, "two");

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        (await tableExistsAsync($"{SchemaName}.two")).ShouldBeTrue();
        var secondStamp = await readStampAsync();
        secondStamp.ShouldNotBeNull();
        secondStamp.ShouldNotBe(firstStamp);
    }

    [Fact]
    public async Task disabled_by_default_behavior_is_unchanged_and_nothing_is_stamped()
    {
        theDatabase.Migrator.UseSchemaFingerprinting.ShouldBeFalse();
        theDatabase.Features["One"].AddTable(SchemaName, "one");

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        (await readStampAsync()).ShouldBeNull();

        // Without fingerprinting every apply introspects, so drift is repaired — the historical behavior.
        await theConnection.CreateCommand($"drop table {SchemaName}.one").ExecuteNonQueryAsync();

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        (await tableExistsAsync($"{SchemaName}.one")).ShouldBeTrue();
    }
}
