using JasperFx;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     A table rebuild threw away everything an <see cref="ITable.AddOnlyMigrations" /> table holds
///     that the model does not declare.
/// </summary>
/// <remarks>
///     <para>
///         weasel#629 leaves an add-only table's undeclared columns, indexes and foreign keys out of
///         the comparison, so none of them is ever an Extra and no <c>ALTER</c> drops one. But SQLite
///         changes a column's type, a foreign key or a primary key by rebuilding the table, and the
///         rebuild built the new table from the model alone. The undeclared column and its rows, and
///         the undeclared indexes and keys, went with the <c>DROP TABLE</c> — under
///         <c>CreateOrUpdate</c>, with the withheld-drop warning still in the log saying they had been
///         left in place.
///     </para>
///     <para>
///         These go through the migrator under <see cref="AutoCreate.CreateOrUpdate" />: the mode an
///         EF-derived model migrates under, and one that reaches the rebuild without asking for
///         <see cref="AutoCreate.All" /> (weasel#538).
///     </para>
/// </remarks>
public class add_only_rebuild
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

    private static async Task applyAsync(SqliteConnection conn, params Table[] tables)
    {
        var migration = await SchemaMigration.DetermineAsync(conn, ct, tables);
        await new SqliteMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct);
    }

    private static async Task executeAsync(SqliteConnection conn, string sql)
    {
        await conn.CreateCommand(sql).ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> scalarAsync(SqliteConnection conn, string sql)
    {
        return await conn.CreateCommand(sql).ExecuteScalarAsync(ct);
    }

    private static async Task<int> countAsync(SqliteConnection conn, string sql)
    {
        return Convert.ToInt32(await scalarAsync(conn, sql));
    }

    private static async Task<string?> storedSqlAsync(SqliteConnection conn, string type, string name)
    {
        return await scalarAsync(conn, $"SELECT sql FROM sqlite_master WHERE type = '{type}' AND name = '{name}'") as string;
    }

    private static async Task<string?> columnTypeAsync(SqliteConnection conn, string column)
    {
        return await scalarAsync(conn, $"SELECT type FROM pragma_table_xinfo('ao_orders') WHERE name = '{column}'") as string;
    }

    /// <summary>
    ///     The model. <paramref name="quantityType" /> is the lever: SQLite can only change a
    ///     column's type by rebuilding the table.
    /// </summary>
    private static Table Orders(string quantityType = "INTEGER", bool addOnly = true)
    {
        var table = new Table("ao_orders") { AddOnlyMigrations = addOnly };
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("quantity", quantityType);
        table.AddColumn<string>("status");
        return table;
    }

    /// <summary>The table as Weasel created it, holding one row.</summary>
    private async Task<SqliteConnection> ordersAsync()
    {
        var conn = await openAsync();
        await applyAsync(conn, Orders(addOnly: false));
        await executeAsync(conn, "INSERT INTO ao_orders (id, quantity, status) VALUES (1, 5, 'open')");
        return conn;
    }

    [Fact]
    public async Task an_undeclared_column_and_its_rows_survive()
    {
        await using var conn = await ordersAsync();
        await executeAsync(conn, "ALTER TABLE ao_orders ADD COLUMN user_note TEXT");
        await executeAsync(conn, "UPDATE ao_orders SET user_note = 'keep me'");

        await applyAsync(conn, Orders("TEXT"));

        (await columnTypeAsync(conn, "quantity")).ShouldBe("TEXT", "the rebuild has to have run for this to prove anything");
        (await scalarAsync(conn, "SELECT user_note FROM ao_orders WHERE id = 1")).ShouldBe("keep me");
    }

    /// <summary>
    ///     No pragma reports a column's collation or its <c>CHECK</c>; only the stored
    ///     <c>CREATE TABLE</c> text holds them. A column rebuilt from what the pragmas say would come
    ///     back looking right and behaving differently.
    /// </summary>
    [Fact]
    public async Task an_undeclared_column_keeps_everything_its_definition_says()
    {
        await using var conn = await ordersAsync();
        await executeAsync(conn,
            """ALTER TABLE ao_orders ADD COLUMN "Ref Code" TEXT COLLATE NOCASE NOT NULL DEFAULT 'n/a' CHECK (length("Ref Code") <= 8)""");

        await applyAsync(conn, Orders("TEXT"));

        (await scalarAsync(conn, """SELECT "Ref Code" FROM ao_orders WHERE id = 1""")).ShouldBe("n/a");
        (await countAsync(conn, """SELECT COUNT(*) FROM ao_orders WHERE "Ref Code" = 'N/A'"""))
            .ShouldBe(1, "COLLATE NOCASE was lost");

        await executeAsync(conn, "INSERT INTO ao_orders (id, quantity, status) VALUES (2, '1', 'open')");
        (await scalarAsync(conn, """SELECT "Ref Code" FROM ao_orders WHERE id = 2""")).ShouldBe("n/a", "the DEFAULT was lost");

        await Should.ThrowAsync<SqliteException>(() =>
            executeAsync(conn, """UPDATE ao_orders SET "Ref Code" = 'far too long' WHERE id = 1"""));
    }

    /// <summary>
    ///     SQLite refuses a write to a generated column, so this one has to be carried across in the
    ///     new table's definition and left out of the copy.
    /// </summary>
    [Fact]
    public async Task an_undeclared_generated_column_is_carried_rather_than_copied()
    {
        await using var conn = await ordersAsync();
        await executeAsync(conn, "ALTER TABLE ao_orders ADD COLUMN id_twice INTEGER GENERATED ALWAYS AS (id * 2) VIRTUAL");

        await applyAsync(conn, Orders("TEXT"));

        Convert.ToInt64(await scalarAsync(conn, "SELECT id_twice FROM ao_orders WHERE id = 1")).ShouldBe(2);
        (await countAsync(conn, "SELECT hidden FROM pragma_table_xinfo('ao_orders') WHERE name = 'id_twice'"))
            .ShouldBe(2, "still a VIRTUAL generated column, not a plain one holding the values it had");
    }

    /// <summary>
    ///     Per-column collation, mixed sort order and a predicate: exactly what re-rendering the
    ///     index from an <see cref="IndexDefinition" /> would flatten, so it comes back from the
    ///     statement SQLite stored for it instead.
    /// </summary>
    [Fact]
    public async Task an_undeclared_index_comes_back_exactly_as_it_was_written()
    {
        await using var conn = await ordersAsync();
        await executeAsync(conn,
            "CREATE INDEX ix_ao_user ON ao_orders (status COLLATE NOCASE DESC, quantity) WHERE quantity > 0");
        var before = await storedSqlAsync(conn, "index", "ix_ao_user");

        await applyAsync(conn, Orders("TEXT"));

        (await storedSqlAsync(conn, "index", "ix_ao_user")).ShouldBe(before);
    }

    /// <summary>
    ///     Both shapes of an undeclared foreign key: a table constraint over a column the model
    ///     declares, and an inline <c>REFERENCES</c> on a column it does not. The second is carried
    ///     inside its column's definition, so it must not be written a second time.
    /// </summary>
    [Fact]
    public async Task undeclared_foreign_keys_are_still_enforced_and_not_doubled()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "CREATE TABLE ao_customers (id INTEGER PRIMARY KEY)");
        await executeAsync(conn, "CREATE TABLE ao_regions (id INTEGER PRIMARY KEY)");
        await executeAsync(conn, "INSERT INTO ao_customers (id) VALUES (10)");
        await executeAsync(conn, "INSERT INTO ao_regions (id) VALUES (20)");
        await executeAsync(conn, """
            CREATE TABLE ao_orders (
                id INTEGER NOT NULL PRIMARY KEY,
                quantity INTEGER,
                status TEXT,
                customer_id INTEGER,
                FOREIGN KEY (customer_id) REFERENCES ao_customers (id) ON DELETE CASCADE
            )
            """);
        await executeAsync(conn, "ALTER TABLE ao_orders ADD COLUMN region_id INTEGER REFERENCES ao_regions (id)");
        await executeAsync(conn,
            "INSERT INTO ao_orders (id, quantity, status, customer_id, region_id) VALUES (1, 5, 'open', 10, 20)");

        var model = Orders("TEXT");
        model.AddColumn<int>("customer_id");
        await applyAsync(conn, model);

        (await countAsync(conn, "SELECT COUNT(*) FROM pragma_foreign_key_list('ao_orders')")).ShouldBe(2);
        (await scalarAsync(conn,
                "SELECT on_delete FROM pragma_foreign_key_list('ao_orders') WHERE \"from\" = 'customer_id' AND \"table\" = 'ao_customers'"))
            .ShouldBe("CASCADE");
        (await countAsync(conn,
                "SELECT COUNT(*) FROM pragma_foreign_key_list('ao_orders') WHERE \"from\" = 'region_id' AND \"table\" = 'ao_regions'"))
            .ShouldBe(1);

        await Should.ThrowAsync<SqliteException>(() =>
            executeAsync(conn, "INSERT INTO ao_orders (id, customer_id) VALUES (2, 999)"));
        await Should.ThrowAsync<SqliteException>(() =>
            executeAsync(conn, "INSERT INTO ao_orders (id, region_id) VALUES (3, 999)"));
    }

    /// <summary>
    ///     SQLite never addresses a foreign key by name, so a key the model declares over the same
    ///     columns and the same table <em>is</em> the undeclared one, renamed. Carrying the old one
    ///     as well would leave the table with two copies of the constraint.
    /// </summary>
    [Fact]
    public async Task a_foreign_key_the_model_declares_under_another_name_is_not_kept_twice()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "CREATE TABLE ao_customers (id INTEGER PRIMARY KEY)");
        await executeAsync(conn, """
            CREATE TABLE ao_orders (
                id INTEGER NOT NULL PRIMARY KEY,
                quantity INTEGER,
                status TEXT,
                customer_id INTEGER,
                FOREIGN KEY (customer_id) REFERENCES ao_customers (id)
            )
            """);

        var model = Orders();
        model.AddColumn<int>("customer_id");
        model.ForeignKeys.Add(new ForeignKey("fk_ao_orders_customer")
        {
            LinkedTable = new SqliteObjectName("ao_customers"), ColumnNames = ["customer_id"], LinkedNames = ["id"]
        });

        await applyAsync(conn, model);

        (await countAsync(conn, "SELECT COUNT(*) FROM pragma_foreign_key_list('ao_orders')")).ShouldBe(1);
        (await model.FindDeltaAsync(conn, ct)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     What the rebuild did right before, pinned on the add-only path: the table's triggers are
    ///     put back, and a table that references it still does, with its rows.
    /// </summary>
    [Fact]
    public async Task triggers_and_referencing_tables_come_through()
    {
        await using var conn = await ordersAsync();
        await executeAsync(conn, "ALTER TABLE ao_orders ADD COLUMN user_note TEXT");
        await executeAsync(conn, "CREATE TABLE ao_audit (id INTEGER PRIMARY KEY, order_id INTEGER REFERENCES ao_orders (id))");
        await executeAsync(conn, "INSERT INTO ao_audit (order_id) VALUES (1)");
        await executeAsync(conn,
            "CREATE TRIGGER ao_orders_audit AFTER UPDATE ON ao_orders BEGIN INSERT INTO ao_audit (order_id) VALUES (NEW.id); END");

        await applyAsync(conn, Orders("TEXT"));

        (await countAsync(conn, "SELECT COUNT(*) FROM pragma_foreign_key_list('ao_audit') WHERE \"table\" = 'ao_orders'"))
            .ShouldBe(1);
        (await countAsync(conn, "SELECT COUNT(*) FROM pragma_foreign_key_check")).ShouldBe(0);

        await executeAsync(conn, "UPDATE ao_orders SET status = 'touched'");
        (await countAsync(conn, "SELECT COUNT(*) FROM ao_audit")).ShouldBe(2, "the trigger was not put back");
    }

    [Fact]
    public async Task the_rebuilt_table_converges_with_its_undeclared_objects_still_withheld()
    {
        await using var conn = await ordersAsync();
        await executeAsync(conn, "ALTER TABLE ao_orders ADD COLUMN user_note TEXT");
        await executeAsync(conn, "CREATE INDEX ix_ao_user ON ao_orders (user_note)");

        await applyAsync(conn, Orders("TEXT"));

        var delta = await Orders("TEXT").FindDeltaAsync(conn, ct);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.WithheldDrops.ShouldBe(["column user_note", "index ix_ao_user"], ignoreOrder: true);
    }

    /// <summary>
    ///     A primary key is the model's to declare, so a column in it that the model does not
    ///     declare leaves the rebuild two choices — keep a key the model does not ask for, or take
    ///     the column out of it — and neither is leaving it alone. The rebuild refuses, before
    ///     anything runs.
    /// </summary>
    [Fact]
    public async Task an_undeclared_column_in_the_primary_key_is_refused_rather_than_guessed_at()
    {
        await using var conn = await openAsync();
        await executeAsync(conn,
            "CREATE TABLE ao_orders (tenant TEXT NOT NULL, id INTEGER NOT NULL, quantity INTEGER, status TEXT, PRIMARY KEY (tenant, id))");
        await executeAsync(conn, "INSERT INTO ao_orders (tenant, id, quantity, status) VALUES ('a', 1, 5, 'open')");

        var failure = await Should.ThrowAsync<SchemaMigrationException>(() => applyAsync(conn, Orders("TEXT")));

        failure.Message.ShouldContain("tenant");
        failure.Message.ShouldContain(nameof(ITable.AddOnlyMigrations));
        (await columnTypeAsync(conn, "quantity")).ShouldBe("INTEGER", "nothing may have run");
        (await scalarAsync(conn, "SELECT tenant FROM ao_orders WHERE id = 1")).ShouldBe("a");
    }

    /// <summary>
    ///     A UNIQUE or CHECK table constraint is something the SQLite model cannot declare at all
    ///     (weasel#488), so on an add-only table every one of them is undeclared.
    /// </summary>
    [Fact]
    public async Task undeclared_table_constraints_are_still_enforced()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, """
            CREATE TABLE ao_orders (
                id INTEGER NOT NULL PRIMARY KEY,
                quantity INTEGER,
                status TEXT,
                CONSTRAINT ak_ao_orders_status UNIQUE (status),
                CHECK (status <> '')
            )
            """);
        await executeAsync(conn, "INSERT INTO ao_orders (id, quantity, status) VALUES (1, 5, 'open')");

        await applyAsync(conn, Orders("TEXT"));

        (await columnTypeAsync(conn, "quantity")).ShouldBe("TEXT");
        await Should.ThrowAsync<SqliteException>(() =>
            executeAsync(conn, "INSERT INTO ao_orders (id, quantity, status) VALUES (2, '1', 'open')"));
        await Should.ThrowAsync<SqliteException>(() =>
            executeAsync(conn, "INSERT INTO ao_orders (id, quantity, status) VALUES (3, '1', '')"));
    }

    /// <summary>
    ///     The stored definition is split where SQLite splits it, not at every comma: commas and
    ///     parentheses inside quoted names, string literals, nested expressions and comments all
    ///     belong to the definition they sit in.
    /// </summary>
    [Fact]
    public async Task a_definition_full_of_commas_quotes_and_comments_is_carried_whole()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, """
            CREATE TABLE ao_orders (
                id INTEGER NOT NULL PRIMARY KEY, -- the key, with a comma (and a parenthesis
                quantity INTEGER,
                status TEXT,
                "odd, (name)" TEXT DEFAULT 'a, b (c)' /* a comment, ( unbalanced */,
                [bracketed] TEXT CHECK ([bracketed] <> 'x,y'),
                `ticked` INTEGER DEFAULT (1 + (2 * 3))
            )
            """);
        await executeAsync(conn, "INSERT INTO ao_orders (id, quantity, status) VALUES (1, 5, 'open')");

        await applyAsync(conn, Orders("TEXT"));

        (await columnTypeAsync(conn, "quantity")).ShouldBe("TEXT");
        (await scalarAsync(conn, """SELECT "odd, (name)" FROM ao_orders WHERE id = 1""")).ShouldBe("a, b (c)");
        (await countAsync(conn, "SELECT ticked FROM ao_orders WHERE id = 1")).ShouldBe(7);
        await Should.ThrowAsync<SqliteException>(() =>
            executeAsync(conn, "UPDATE ao_orders SET bracketed = 'x,y' WHERE id = 1"));
    }

    /// <summary>
    ///     <see cref="TableBase{TColumn,TIndex,TForeignKey}.IgnoredIndexes" /> promises an index a
    ///     third party owns is never dropped or recreated, and it is the same list: an index the delta
    ///     was told to leave alone. It went with the <c>DROP TABLE</c> too, add-only or not.
    /// </summary>
    [Fact]
    public async Task an_ignored_index_survives_the_rebuild_of_any_table()
    {
        await using var conn = await ordersAsync();
        await executeAsync(conn, "CREATE INDEX ix_ao_external ON ao_orders (status)");
        var before = await storedSqlAsync(conn, "index", "ix_ao_external");

        var model = Orders("TEXT", addOnly: false);
        model.IgnoreIndex("ix_ao_external");
        await applyAsync(conn, model);

        (await columnTypeAsync(conn, "quantity")).ShouldBe("TEXT");
        (await storedSqlAsync(conn, "index", "ix_ao_external")).ShouldBe(before);
    }

    /// <summary>
    ///     Unchanged, and pinned: on a table that is not add-only the model is the whole truth, and a
    ///     rebuild leaves out what it does not declare, as it always has.
    /// </summary>
    [Fact]
    public async Task a_table_that_is_not_add_only_still_rebuilds_to_the_model_alone()
    {
        await using var conn = await ordersAsync();
        await executeAsync(conn, "ALTER TABLE ao_orders ADD COLUMN user_note TEXT");
        await executeAsync(conn, "CREATE INDEX ix_ao_user ON ao_orders (user_note)");

        await applyAsync(conn, Orders("TEXT", addOnly: false));

        (await columnTypeAsync(conn, "quantity")).ShouldBe("TEXT");
        (await columnTypeAsync(conn, "user_note")).ShouldBeNull();
        (await storedSqlAsync(conn, "index", "ix_ao_user")).ShouldBeNull();
    }
}
