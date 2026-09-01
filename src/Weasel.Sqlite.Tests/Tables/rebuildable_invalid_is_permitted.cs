using JasperFx;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     weasel#538: <c>SchemaMigration.AssertPatchingIsValid</c> refused every <c>Invalid</c> delta
///     below <see cref="AutoCreate.All" />, including the ones the apply path right below it knows
///     how to carry out without losing a row.
/// </summary>
/// <remarks>
///     <para>
///         weasel#477 taught <c>SchemaMigration.WriteAllUpdates</c> and <c>Migrator.WriteUpdate</c>
///         to honour <see cref="ISchemaObjectDeltaWithRebuild.CanRebuildInPlace" /> and rebuild the
///         table rather than drop it. The gate was not taught the same thing, so it rejected
///         migrations the machinery it guards would have applied correctly.
///     </para>
///     <para>
///         Reachable in 9.28.0 through weasel#533: a table Weasel created from a model declaring
///         <c>numeric</c> or <c>decimal</c> has a <c>REAL</c> column, and the model now asks for
///         <c>NUMERIC</c>. That is a column type change, which SQLite reports as <c>Invalid</c>, so
///         the first migration after upgrading threw under <c>CreateOrUpdate</c>.
///     </para>
/// </remarks>
public class rebuildable_invalid_is_permitted
{
    private readonly string _connectionString = $"Data Source={Path.GetTempFileName()};";

    private async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync();
        await conn.ResetSchemaAsync("main");
        return conn;
    }

    private static Table PricesTable(string amountType)
    {
        var table = new Table("pm_prices");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("amount", amountType);
        return table;
    }

    /// <summary>
    ///     The 9.28.0 upgrade shape: a table created when <c>numeric</c> mapped to <c>REAL</c>,
    ///     against a model that now asks for <c>NUMERIC</c>.
    /// </summary>
    private async Task<SqliteConnection> upgradedDatabaseAsync()
    {
        var conn = await openAsync();

        var asCreatedBefore928 = await SchemaMigration.DetermineAsync(
            conn, CancellationToken.None, PricesTable("REAL"));
        await new SqliteMigrator().ApplyAllAsync(conn, asCreatedBefore928, AutoCreate.All);

        await conn.CreateCommand("INSERT INTO pm_prices (id, amount) VALUES (1, 19.95)")
            .ExecuteNonQueryAsync();

        return conn;
    }

    [Fact]
    public async Task the_upgrade_delta_is_invalid_but_rebuildable()
    {
        await using var conn = await upgradedDatabaseAsync();

        var delta = await PricesTable("NUMERIC").FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.CanRebuildInPlace.ShouldBeTrue();
    }

    [Fact]
    public async Task create_or_update_permits_a_rebuildable_invalid()
    {
        await using var conn = await upgradedDatabaseAsync();

        var migration = await SchemaMigration.DetermineAsync(
            conn, CancellationToken.None, PricesTable("NUMERIC"));

        Should.NotThrow(() => migration.AssertPatchingIsValid(AutoCreate.CreateOrUpdate));
    }

    /// <summary>
    ///     And the permission is worth something: the migration it let through actually converges,
    ///     and the row is still there afterwards.
    /// </summary>
    [Fact]
    public async Task create_or_update_applies_the_rebuild_and_keeps_the_rows()
    {
        await using var conn = await upgradedDatabaseAsync();

        var migration = await SchemaMigration.DetermineAsync(
            conn, CancellationToken.None, PricesTable("NUMERIC"));
        await new SqliteMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate);

        var amount = await conn.CreateCommand("SELECT amount FROM pm_prices WHERE id = 1")
            .ExecuteScalarAsync();
        Convert.ToDecimal(amount).ShouldBe(19.95m);

        var after = await PricesTable("NUMERIC").FindDeltaAsync(conn);
        after.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     A rebuild recreates a table that is already there, which is an update however you look at
    ///     it — so <see cref="AutoCreate.CreateOnly" /> still refuses it. This falls out of the
    ///     existing <c>CreateOnly</c> branch rather than being special-cased, but it is the open
    ///     question in weasel#538 and deserves to be pinned.
    /// </summary>
    [Fact]
    public async Task create_only_still_refuses_a_rebuildable_invalid()
    {
        await using var conn = await upgradedDatabaseAsync();

        var migration = await SchemaMigration.DetermineAsync(
            conn, CancellationToken.None, PricesTable("NUMERIC"));

        Should.Throw<SchemaMigrationException>(
            () => migration.AssertPatchingIsValid(AutoCreate.CreateOnly));
    }
}
