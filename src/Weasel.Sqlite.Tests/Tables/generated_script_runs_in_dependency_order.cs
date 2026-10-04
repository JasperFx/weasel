using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     weasel#677, the SQLite side. See the PostgreSQL class of the same name for why only executing
///     the script proves anything.
/// </summary>
/// <remarks>
///     <para>
///         SQLite is the provider where ordering is the <i>only</i> available answer. It has no
///         <c>ALTER TABLE … ADD CONSTRAINT</c> — <see cref="ForeignKey.WriteAddStatement" /> throws
///         rather than render one — so a foreign key can only be declared inline at table creation,
///         and the deferral the migration path uses elsewhere is not available here at all.
///     </para>
///     <para>
///         <b>It is also the one provider where the misordering was harmless, which is worth
///         recording rather than leaving as a surprise.</b> Measured both ways: SQLite resolves a
///         foreign key's target by name at DML time, not at <c>CREATE TABLE</c>, so a child declared
///         before its parent runs clean <i>and</i> links correctly — the insert tests below pass
///         with or without the sort, because by the time anything writes, the script has created
///         both tables. PostgreSQL (42P01) and SQL Server (<c>references invalid table</c>) both
///         fail outright on the same model.
///     </para>
///     <para>
///         So <see cref="the_script_declares_the_parent_first" /> is the test with teeth here, and
///         the two executing tests are a guard in the other direction: that reordering did not break
///         a schema that already worked. They are deliberately kept rather than deleted, because
///         "SQLite tolerates it" is a claim that should fail loudly if a future SQLite ever stops.
///     </para>
/// </remarks>
public class generated_script_runs_in_dependency_order
{
    private static Table table(string name, params string[] references)
    {
        var table = new Table(new SqliteObjectName("main", name));
        table.AddColumn<int>("id").AsPrimaryKey();

        foreach (var reference in references)
        {
            table.AddColumn<int>($"{reference}_id").AllowNulls();
            table.ForeignKeys.Add(new ForeignKey($"fk_{name}_to_{reference}")
            {
                LinkedTable = new SqliteObjectName("main", reference),
                ColumnNames = [$"{reference}_id"],
                LinkedNames = ["id"]
            });
        }

        return table;
    }

    private static string connectionString() => $"Data Source={Path.GetTempFileName()};";

    private static async Task executeAsync(string connectionString, string script)
    {
        await using var conn = new SqliteConnection(connectionString);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = script;
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    ///     With foreign keys enforced, inserting a child row is what asks SQLite to resolve the
    ///     reference — and the only thing that notices the target table was never created.
    /// </summary>
    private static async Task insertAsync(string connectionString, string child, string parent)
    {
        await using var conn = new SqliteConnection(connectionString);
        await conn.OpenAsync();

        var pragma = conn.CreateCommand();
        pragma.CommandText = "PRAGMA foreign_keys = ON;";
        await pragma.ExecuteNonQueryAsync();

        var insertParent = conn.CreateCommand();
        insertParent.CommandText = $"insert into {parent} (id) values (1);";
        await insertParent.ExecuteNonQueryAsync();

        var insertChild = conn.CreateCommand();
        insertChild.CommandText = $"insert into {child} (id, {parent}_id) values (1, 1);";
        await insertChild.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task a_child_yielded_before_its_parent_still_runs_and_still_links()
    {
        var connection = connectionString();

        var db = new DatabaseWithTables("main", connection);
        db.AddTable(table("so_child", "so_parent"));
        db.AddTable(table("so_parent"));

        await executeAsync(connection, db.ToDatabaseScript());

        // Not the assertion with teeth on SQLite -- see the class remarks. This guards that the
        // reordered script still produces a schema that actually links.
        await insertAsync(connection, "so_child", "so_parent");

        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task a_chain_yielded_backwards_still_runs_and_still_links()
    {
        var connection = connectionString();

        var db = new DatabaseWithTables("main", connection);
        db.AddTable(table("so_c", "so_b"));
        db.AddTable(table("so_b", "so_a"));
        db.AddTable(table("so_a"));

        await executeAsync(connection, db.ToDatabaseScript());

        await insertAsync(connection, "so_b", "so_a");

        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    /// <summary>
    ///     The ordering itself, on the script text. Because SQLite accepts a forward reference, this
    ///     is the only assertion in this class that distinguishes the sort from the server simply
    ///     being forgiving — and it is what makes the script usable by a tool that is not SQLite.
    /// </summary>
    [Fact]
    public void the_script_declares_the_parent_first()
    {
        var db = new DatabaseWithTables("main", connectionString());
        db.AddTable(table("so_text_child", "so_text_parent"));
        db.AddTable(table("so_text_parent"));

        var script = db.ToDatabaseScript();

        script.IndexOf("so_text_parent", StringComparison.Ordinal)
            .ShouldBeLessThan(script.IndexOf("so_text_child", StringComparison.Ordinal));
    }
}
