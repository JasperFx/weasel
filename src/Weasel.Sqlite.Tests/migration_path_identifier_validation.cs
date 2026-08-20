using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Sqlite.Tests;

/// <summary>
///     End-to-end cover for weasel#448: the migration path validates every identifier a table
///     writes, not only the ones that name a database object. Before this, a column name, the
///     primary key constraint name and a check constraint name all went into DDL unexamined —
///     <c>DatabaseBase</c> only walked <see cref="ISchemaObject.AllNames" />, and none of the five
///     providers put those names there.
/// </summary>
/// <remarks>
///     SQLite is the provider these live on because it needs no container. The coverage itself is
///     cross-provider and structural, and is held by
///     <c>Weasel.Core.Tests.table_identifier_coverage_conformance</c>; this suite proves the
///     structural coverage actually reaches <c>Migrator.AssertValidIdentifier</c> on a real
///     migration.
/// </remarks>
public class migration_path_identifier_validation
{
    private static DatabaseWithTables NewDatabase()
        => new("test", $"Data Source={Path.GetTempFileName()};");

    [Fact]
    public async Task a_column_name_carrying_the_delimiter_is_rejected()
    {
        var db = NewDatabase();
        var table = db.AddTable(new DbObjectName("main", "mpiv_people"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("na\"me", typeof(string));

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            () => db.ApplyAllConfiguredChangesToDatabaseAsync());

        ex.Message.ShouldContain("na\"me");
        ex.Message.ShouldContain("a double quote");
    }

    [Fact]
    public async Task a_column_name_carrying_a_semicolon_is_rejected()
    {
        var db = NewDatabase();
        var table = db.AddTable(new DbObjectName("main", "mpiv_semicolon"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name; drop table students", typeof(string));

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            () => db.ApplyAllConfiguredChangesToDatabaseAsync());

        ex.Message.ShouldContain("a semicolon");
    }

    [Fact]
    public async Task a_primary_key_constraint_name_is_rejected()
    {
        var db = NewDatabase();
        var table = db.AddTable(new DbObjectName("main", "mpiv_pk"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.PrimaryKeyName = "pk\"broken";

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            () => db.ApplyAllConfiguredChangesToDatabaseAsync());

        ex.Message.ShouldContain("pk\"broken");
    }

    [Fact]
    public async Task a_check_constraint_name_is_rejected()
    {
        var db = NewDatabase();
        var table = db.AddTable(new DbObjectName("main", "mpiv_check"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("qty", typeof(int));
        table.AddCheckConstraint("ck\"broken", "qty > 0");

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            () => db.ApplyAllConfiguredChangesToDatabaseAsync());

        ex.Message.ShouldContain("ck\"broken");
    }

    /// <summary>
    ///     A foreign key name reaches validation now that SQLite's <c>AllNames()</c> yields it.
    ///     SQLite writes its foreign keys inline in CREATE TABLE, which is why they had been left
    ///     out — but the name is still written, so it still has to be checked.
    /// </summary>
    [Fact]
    public async Task a_foreign_key_constraint_name_is_rejected()
    {
        var db = NewDatabase();
        var table = db.AddTable(new DbObjectName("main", "mpiv_fk"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("state_id", typeof(int));
        table.AddForeignKey("fk\"broken", new DbObjectName("main", "mpiv_states"), ["state_id"], ["id"]);

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            () => db.ApplyAllConfiguredChangesToDatabaseAsync());

        ex.Message.ShouldContain("fk\"broken");
    }

    /// <summary>
    ///     The other half of the policy settled in weasel#448: an interior space is a legitimate
    ///     legacy name, every provider quotes for shape as of weasel#447, and the migration path
    ///     lets it through and creates the column.
    /// </summary>
    [Fact]
    public async Task an_interior_space_survives_the_whole_migration_path()
    {
        var db = NewDatabase();
        var table = db.AddTable(new DbObjectName("main", "mpiv_legacy"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("unit price", typeof(decimal));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        await db.AssertDatabaseMatchesConfigurationAsync();
    }
}
