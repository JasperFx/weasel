using JasperFx;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     The rollback of an add-only table's rebuild dropped the values of every column the model does
///     not declare.
/// </summary>
/// <remarks>
///     <para>
///         Neither weasel#639 nor weasel#648 has this defect alone; together they do, and both suites
///         stay green over it. #639 made the forward rebuild of an
///         <see cref="ITable.AddOnlyMigrations" /> table keep the columns the model does not declare,
///         so the rebuilt table still has them and their rows. #648 then routed the rollback of that
///         rebuild to the reverse rebuild — which copies back only the columns the model *does*
///         declare. An undeclared column survived the rebuild and came back NULL from the rollback.
///     </para>
///     <para>
///         The forward half is #639's own subject and the rollback half is #648's, so only a test
///         that runs both against one table sees it. That is the whole point of it being here.
///     </para>
/// </remarks>
public class add_only_rebuild_rolls_back_the_undeclared_rows
{
    private readonly string _connectionString = $"Data Source={Path.GetTempFileName()};";

    private static CancellationToken ct => TestContext.Current.CancellationToken;

    private async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync(ct);
        await conn.ResetSchemaAsync("main", ct);
        await executeAsync(conn, "PRAGMA foreign_keys = ON");
        return conn;
    }

    private static async Task executeAsync(SqliteConnection conn, string sql)
    {
        await conn.CreateCommand(sql).ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> scalarAsync(SqliteConnection conn, string sql)
    {
        return await conn.CreateCommand(sql).ExecuteScalarAsync(ct);
    }

    /// <summary>
    ///     The declared model. <paramref name="quantityType" /> is the lever: SQLite can only change a
    ///     column's type by rebuilding the table.
    /// </summary>
    private static Table Orders(string quantityType = "INTEGER")
    {
        var table = new Table("ar_orders") { AddOnlyMigrations = true };
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("quantity", quantityType);
        return table;
    }

    /// <summary>
    ///     An add-only table with a column the model has never heard of, holding values, carried
    ///     through a rebuild and then back out of it.
    /// </summary>
    [Fact]
    public async Task an_undeclared_column_keeps_its_values_through_the_rollback()
    {
        await using var conn = await openAsync();
        var migrator = new SqliteMigrator();

        var create = await SchemaMigration.DetermineAsync(conn, ct, Orders());
        await migrator.ApplyAllAsync(conn, create, AutoCreate.CreateOrUpdate, ct: ct);

        // Undeclared: added outside the model, the way an add-only table earns the setting
        await executeAsync(conn, "ALTER TABLE ar_orders ADD COLUMN user_note TEXT");
        await executeAsync(conn, "INSERT INTO ar_orders (id, quantity, user_note) VALUES (1, 5, 'keep me'), (2, 9, 'and me')");

        // Retyping quantity forces the rebuild
        var migration = await SchemaMigration.DetermineAsync(conn, ct, Orders("TEXT"));
        migration.Deltas.Single().ShouldBeOfType<TableDelta>().CanRebuildInPlace.ShouldBeTrue();
        await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct);

        // #639: the forward rebuild kept it
        (await scalarAsync(conn, "SELECT user_note FROM ar_orders WHERE id = 1"))
            .ShouldBe("keep me", "the forward rebuild dropped the undeclared column");

        await migration.RollbackAllAsync(conn, migrator, ct);

        (await scalarAsync(conn, "SELECT type FROM pragma_table_info('ar_orders') WHERE name = 'quantity'"))
            .ShouldBe("INTEGER", "the rollback has to have run");

        // #648: and the rollback has to keep it too
        (await scalarAsync(conn, "SELECT user_note FROM ar_orders WHERE id = 1"))
            .ShouldBe("keep me", "the rollback emptied the undeclared column");
        (await scalarAsync(conn, "SELECT user_note FROM ar_orders WHERE id = 2"))
            .ShouldBe("and me", "the rollback emptied the undeclared column");
    }

    /// <summary>
    ///     The control: a table that is not add-only declares everything it keeps, so the rollback
    ///     copies back exactly the declared columns, as it did before.
    /// </summary>
    [Fact]
    public async Task a_declared_table_still_rolls_back_only_what_it_declares()
    {
        await using var conn = await openAsync();
        var migrator = new SqliteMigrator();

        var table = new Table("ar_plain");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("quantity", "INTEGER");

        var create = await SchemaMigration.DetermineAsync(conn, ct, table);
        await migrator.ApplyAllAsync(conn, create, AutoCreate.CreateOrUpdate, ct: ct);
        await executeAsync(conn, "INSERT INTO ar_plain (id, quantity) VALUES (1, 5)");

        var retyped = new Table("ar_plain");
        retyped.AddColumn<int>("id").AsPrimaryKey();
        retyped.AddColumn("quantity", "TEXT");

        var migration = await SchemaMigration.DetermineAsync(conn, ct, retyped);
        await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct);
        await migration.RollbackAllAsync(conn, migrator, ct);

        (await scalarAsync(conn, "SELECT quantity FROM ar_plain WHERE id = 1")).ShouldBe(5L);
    }
}
