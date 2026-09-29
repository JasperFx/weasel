using JasperFx;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     A table modelled with <see cref="ITable.PreserveIdentifierCase" /> never matched the table in
///     the database, not even the one it had just created, so every migration rebuilt it.
/// </summary>
/// <remarks>
///     <para>
///         The columns read back from <c>pragma_table_xinfo</c> are folded to lowercase, and
///         <c>ItemDelta</c> paired the model's names with them through a case-sensitive dictionary.
///         So <c>SCHED_NAME</c> in the model and <c>sched_name</c> from the catalog were two
///         different columns: every column was both Missing and Extra, the delta was
///         <c>Invalid</c>, and SQLite answers <c>Invalid</c> by rebuilding the table — on every
///         apply. Index and foreign key names were paired the same way.
///     </para>
///     <para>
///         SQLite identifiers are case-insensitive, and PostgreSQL, SQL Server, Oracle and MySQL all
///         pair names that way already. So does <c>AddOnlyMigration.DeclaredOnly</c>, which says it
///         matches "the same way <c>ItemDelta</c> pairs the two sides" — true everywhere but here.
///     </para>
/// </remarks>
public class preserved_identifier_case
{
    private readonly string _connectionString = $"Data Source={Path.GetTempFileName()};";

    private static CancellationToken ct => TestContext.Current.CancellationToken;

    private async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync(ct);
        await conn.ResetSchemaAsync("main", ct);
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

    private static Table Customers()
    {
        var table = new Table("pc_customers") { PreserveIdentifierCase = true };
        table.AddColumn<int>("Id").AsPrimaryKey();
        return table;
    }

    private static Table Orders(bool addOnly = false)
    {
        var table = new Table("pc_orders") { PreserveIdentifierCase = true, AddOnlyMigrations = addOnly };
        table.AddColumn<int>("Id").AsPrimaryKey();
        table.AddColumn<int>("CustomerId");
        table.AddColumn<string>("Status");
        table.Indexes.Add(new IndexDefinition("IX_Orders_Status") { Columns = ["Status"] });
        table.ForeignKeys.Add(new ForeignKey("FK_Orders_Customers")
        {
            LinkedTable = new SqliteObjectName("pc_customers"), ColumnNames = ["CustomerId"], LinkedNames = ["Id"]
        });
        return table;
    }

    [Fact]
    public async Task a_table_created_from_the_model_matches_it()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, Customers(), Orders());

        var migration = await SchemaMigration.DetermineAsync(conn, ct, Customers(), Orders());

        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The shape of a table another tool created from an upper-case script — two key columns of
    ///     the same type, so no rename could be guessed either — against a model that keeps that
    ///     case. Before, this was a primary key column Extra, and so a rebuild.
    /// </summary>
    [Fact]
    public async Task a_table_created_with_the_same_case_by_another_tool_matches_it()
    {
        await using var conn = await openAsync();
        await executeAsync(conn,
            "CREATE TABLE PC_LOCKS (SCHED_NAME NVARCHAR(120) NOT NULL, LOCK_NAME NVARCHAR(40) NOT NULL, PRIMARY KEY (SCHED_NAME, LOCK_NAME))");

        var locks = new Table("PC_LOCKS") { PreserveIdentifierCase = true };
        locks.AddColumn("SCHED_NAME", "NVARCHAR(120)").NotNull().AsPrimaryKey();
        locks.AddColumn("LOCK_NAME", "NVARCHAR(40)").NotNull().AsPrimaryKey();

        var delta = await locks.FindDeltaAsync(conn, ct);

        delta.Columns.Missing.ShouldBeEmpty();
        delta.Columns.Extras.ShouldBeEmpty();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     SQLite will not hold two indexes whose names differ only in case, and never addresses a
    ///     foreign key by name at all, so a model spelling either differently is naming the same
    ///     object. Before, the index was dropped and recreated, and the foreign key forced a rebuild.
    /// </summary>
    [Fact]
    public async Task index_and_foreign_key_names_that_differ_only_in_case_are_the_same_object()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "CREATE TABLE pc_customers (Id INTEGER NOT NULL PRIMARY KEY)");
        await executeAsync(conn, """
            CREATE TABLE pc_orders (
                Id INTEGER NOT NULL PRIMARY KEY,
                CustomerId INTEGER,
                Status TEXT,
                CONSTRAINT fk_orders_customers FOREIGN KEY (CustomerId) REFERENCES pc_customers (Id)
            )
            """);
        await executeAsync(conn, "CREATE INDEX ix_orders_status ON pc_orders (Status)");

        var delta = await Orders().FindDeltaAsync(conn, ct);

        delta.Indexes.HasChanges().ShouldBeFalse();
        delta.ForeignKeys.HasChanges().ShouldBeFalse();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The combination an EF Core model maps to: <c>MapToTable</c> sets both flags.
    /// </summary>
    [Fact]
    public async Task an_add_only_table_matches_it_too()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, Customers(), Orders(addOnly: true));

        var delta = await Orders(addOnly: true).FindDeltaAsync(conn, ct);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.WithheldDrops.ShouldBeEmpty();
    }

    /// <summary>
    ///     Pairing by name regardless of case must not pair away a real difference.
    /// </summary>
    [Fact]
    public async Task a_real_change_is_still_found_and_only_that_change()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, Customers(), Orders());

        var added = Orders();
        added.AddColumn<string>("ShippedOn");

        var addition = await added.FindDeltaAsync(conn, ct);
        addition.Difference.ShouldBe(SchemaPatchDifference.Update);
        addition.Columns.Missing.Select(x => x.Name).ShouldBe(["ShippedOn"]);
        addition.Columns.Extras.ShouldBeEmpty();

        var retyped = Orders();
        retyped.ModifyColumn("Status")!.Column.Type = "INTEGER";

        var change = await retyped.FindDeltaAsync(conn, ct);
        change.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        change.Columns.Different.Select(x => x.Expected.Name).ShouldBe(["Status"]);
        change.Columns.Missing.ShouldBeEmpty();
        change.Columns.Extras.ShouldBeEmpty();
    }
}
