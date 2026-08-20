using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Postgresql.Tests.Migrations;

/// <summary>
/// Coverage for weasel#439. The stamp used to be a single row (<c>where id = 1</c>) in the migrator's
/// default schema, and <c>PostgresqlMigrator</c> hardcodes that schema to "public" -- so two logical
/// databases sharing a physical database overwrote each other's fingerprint. Neither ever read back
/// its own, every apply ran in full, and each one paid an extra SELECT and upsert for the privilege:
/// measurably slower than leaving fingerprinting off. Exactly the topology it was meant to help.
/// </summary>
// The stamp table lives in the migrator's default schema, which PostgresqlMigrator hardcodes to
// "public" -- so every fixture that mutates the public schema has to be serialized against this one,
// not just the fixtures that read the stamps. That is the whole "public schema" collection.
[Collection("public schema")]
public class schema_fingerprint_colocated_tests: IntegrationContext, IAsyncLifetime
{
    private const string OtherSchema = "fingerprint_colocated_other";

    private readonly TestDatabaseWithTables theFirst;
    private readonly TestDatabaseWithTables theSecond;

    public schema_fingerprint_colocated_tests(): base("fingerprint_colocated")
    {
        // Two logical databases, different schemas, one physical database -- a Marten deployment with
        // more than one store per database, which is what #431's reporter runs at 512 shards.
        theFirst = new TestDatabaseWithTables(AutoCreate.CreateOrUpdate, "First", theDataSource);
        theSecond = new TestDatabaseWithTables(AutoCreate.CreateOrUpdate, "Second", theDataSource);

        theFirst.Migrator.UseSchemaFingerprinting = true;
        theSecond.Migrator.UseSchemaFingerprinting = true;

        theFirst.Features["One"].AddTable(SchemaName, "one");
        theSecond.Features["Two"].AddTable(OtherSchema, "two");
    }

    public override async ValueTask InitializeAsync()
    {
        await ResetSchema();
        await theConnection.ResetSchemaAsync(OtherSchema);

        await theConnection.CreateCommand("drop table if exists public.weasel_schema_fingerprints")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand("drop table if exists public.weasel_schema_fingerprint")
            .ExecuteNonQueryAsync();
    }

    private async Task<bool> tableExistsAsync(string qualifiedName)
    {
        var result = await theConnection
            .CreateCommand($"select to_regclass('{qualifiedName}') is not null")
            .ExecuteScalarAsync();
        return result is true;
    }

    private async Task<long> stampCountAsync()
    {
        var result = await theConnection
            .CreateCommand("select count(*) from public.weasel_schema_fingerprints")
            .ExecuteScalarAsync();
        return (long)result!;
    }

    [Fact]
    public async Task co_located_databases_each_keep_their_own_stamp()
    {
        await theFirst.ApplyAllConfiguredChangesToDatabaseAsync();
        await theSecond.ApplyAllConfiguredChangesToDatabaseAsync();

        (await tableExistsAsync($"{SchemaName}.one")).ShouldBeTrue();
        (await tableExistsAsync($"{OtherSchema}.two")).ShouldBeTrue();

        // One stamp each, rather than one row the two of them fight over.
        (await stampCountAsync()).ShouldBe(2);
    }

    [Fact]
    public async Task the_second_apply_short_circuits_for_both_databases()
    {
        await theFirst.ApplyAllConfiguredChangesToDatabaseAsync();
        await theSecond.ApplyAllConfiguredChangesToDatabaseAsync();

        // Drift applied outside Weasel. A matching stamp is trusted, so an apply that genuinely
        // short-circuits will NOT bring these back -- that is the observable proof the introspection
        // was skipped. Before #439 the second database's stamp had clobbered the first's, so both
        // applies ran in full and both tables reappeared.
        await theConnection.CreateCommand($"drop table {SchemaName}.one").ExecuteNonQueryAsync();
        await theConnection.CreateCommand($"drop table {OtherSchema}.two").ExecuteNonQueryAsync();

        (await theFirst.ApplyAllConfiguredChangesToDatabaseAsync()).ShouldBe(SchemaPatchDifference.None);
        (await theSecond.ApplyAllConfiguredChangesToDatabaseAsync()).ShouldBe(SchemaPatchDifference.None);

        (await tableExistsAsync($"{SchemaName}.one")).ShouldBeFalse();
        (await tableExistsAsync($"{OtherSchema}.two")).ShouldBeFalse();
    }

    [Fact]
    public async Task interleaved_applies_do_not_invalidate_each_other()
    {
        // The failure mode was order-dependent: A stamps, B stamps over it, A reads B's stamp and
        // re-applies. Walk them repeatedly to make sure neither ever loses its short-circuit.
        await theFirst.ApplyAllConfiguredChangesToDatabaseAsync();
        await theSecond.ApplyAllConfiguredChangesToDatabaseAsync();

        for (var i = 0; i < 3; i++)
        {
            await theFirst.ApplyAllConfiguredChangesToDatabaseAsync();
            await theSecond.ApplyAllConfiguredChangesToDatabaseAsync();
        }

        // Still exactly two stamps -- a re-stamp per pass would mean the short-circuit never fired.
        (await stampCountAsync()).ShouldBe(2);
    }

    [Fact]
    public async Task the_legacy_single_row_table_is_cleaned_up_on_the_first_stamp()
    {
        // A database upgrading to weasel#439 has the old one-row table sitting there. It is a cache
        // and nothing reads it any more, so it should not be left behind forever.
        await theConnection.CreateCommand(
                "create table public.weasel_schema_fingerprint (id int not null primary key, fingerprint varchar(128) not null, applied_at varchar(64) not null)")
            .ExecuteNonQueryAsync();

        await theFirst.ApplyAllConfiguredChangesToDatabaseAsync();

        (await tableExistsAsync("public.weasel_schema_fingerprint")).ShouldBeFalse();
        (await tableExistsAsync("public.weasel_schema_fingerprints")).ShouldBeTrue();
    }

    [Fact]
    public async Task stamps_are_capped_so_the_table_cannot_grow_without_bound()
    {
        // Keying rows by fingerprint means a configuration change leaves the previous row behind
        // rather than replacing it. 30 distinct configurations must not leave 30 rows.
        for (var i = 0; i < 30; i++)
        {
            theFirst.Features["One"].AddTable(SchemaName, $"table_{i}");
            await theFirst.ApplyAllConfiguredChangesToDatabaseAsync();
        }

        var count = await stampCountAsync();
        count.ShouldBeLessThanOrEqualTo(25);

        // And the newest configuration still short-circuits — pruning evicts the oldest, not the live one.
        await theConnection.CreateCommand($"drop table {SchemaName}.table_29").ExecuteNonQueryAsync();
        (await theFirst.ApplyAllConfiguredChangesToDatabaseAsync()).ShouldBe(SchemaPatchDifference.None);
        (await tableExistsAsync($"{SchemaName}.table_29")).ShouldBeFalse();
    }

    [Fact]
    public async Task a_configuration_change_still_re_enables_the_apply()
    {
        await theFirst.ApplyAllConfiguredChangesToDatabaseAsync();
        await theSecond.ApplyAllConfiguredChangesToDatabaseAsync();

        theFirst.Features["One"].AddTable(SchemaName, "one_more");

        (await theFirst.ApplyAllConfiguredChangesToDatabaseAsync()).ShouldBe(SchemaPatchDifference.Create);
        (await tableExistsAsync($"{SchemaName}.one_more")).ShouldBeTrue();

        // ...and the other database is undisturbed by its neighbour's new stamp.
        await theConnection.CreateCommand($"drop table {OtherSchema}.two").ExecuteNonQueryAsync();
        (await theSecond.ApplyAllConfiguredChangesToDatabaseAsync()).ShouldBe(SchemaPatchDifference.None);
        (await tableExistsAsync($"{OtherSchema}.two")).ShouldBeFalse();
    }
}
