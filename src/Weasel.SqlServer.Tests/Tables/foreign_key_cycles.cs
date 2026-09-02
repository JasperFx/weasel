using JasperFx;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

[Collection("integration")]
public class foreign_key_cycles
{
    private static Table BuildTable(string name, string linkedTable, string foreignKeyName)
    {
        var table = new Table(new SqlServerObjectName("dbo", name));
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("other_id").AllowNulls();

        table.ForeignKeys.Add(new ForeignKey(foreignKeyName)
        {
            LinkedTable = new SqlServerObjectName("dbo", linkedTable),
            ColumnNames = ["other_id"],
            LinkedNames = ["id"]
        });

        return table;
    }

    private static async Task DropAsync(params string[] tableNames)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        foreach (var name in tableNames)
        {
            await conn.CreateCommand($@"
declare @sql nvarchar(max) = '';
select @sql = @sql + 'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(fk.parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(fk.parent_object_id)) + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
from sys.foreign_keys fk where fk.referenced_object_id = OBJECT_ID('dbo.{name}');
exec sp_executesql @sql;").ExecuteNonQueryAsync();
        }

        foreach (var name in tableNames)
        {
            await conn.CreateCommand($"drop table if exists dbo.{name};").ExecuteNonQueryAsync();
        }
    }

    private static async Task<bool> ConstraintExistsAsync(string name)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        var count = await conn.CreateCommand("select count(*) from sys.foreign_keys where name = @name")
            .With("name", name)
            .ExecuteScalarAsync();

        return Convert.ToInt32(count) == 1;
    }

    [Fact]
    public async Task mutually_referencing_tables_are_created_from_scratch()
    {
        await DropAsync("fk_cycle_a", "fk_cycle_b");

        var db = new DatabaseWithTables("cycles", ConnectionSource.ConnectionString);
        db.AddTable(BuildTable("fk_cycle_a", "fk_cycle_b", "fk_cycle_a_to_b"));
        db.AddTable(BuildTable("fk_cycle_b", "fk_cycle_a", "fk_cycle_b_to_a"));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();

        (await ConstraintExistsAsync("fk_cycle_a_to_b")).ShouldBeTrue();
        (await ConstraintExistsAsync("fk_cycle_b_to_a")).ShouldBeTrue();

        await db.AssertDatabaseMatchesConfigurationAsync();

        await DropAsync("fk_cycle_a", "fk_cycle_b");
    }

    [Fact]
    public async Task a_partially_created_cycle_is_completed_by_the_next_apply()
    {
        await DropAsync("fk_half_a", "fk_half_b");

        var first = new DatabaseWithTables("cycles", ConnectionSource.ConnectionString);
        var withoutForeignKey = new Table(new SqlServerObjectName("dbo", "fk_half_a"));
        withoutForeignKey.AddColumn<int>("id").AsPrimaryKey();
        withoutForeignKey.AddColumn<int>("other_id").AllowNulls();
        first.AddTable(withoutForeignKey);

        await first.ApplyAllConfiguredChangesToDatabaseAsync();

        var db = new DatabaseWithTables("cycles", ConnectionSource.ConnectionString);
        db.AddTable(BuildTable("fk_half_a", "fk_half_b", "fk_half_a_to_b"));
        db.AddTable(BuildTable("fk_half_b", "fk_half_a", "fk_half_b_to_a"));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();

        (await ConstraintExistsAsync("fk_half_a_to_b")).ShouldBeTrue();
        (await ConstraintExistsAsync("fk_half_b_to_a")).ShouldBeTrue();

        await DropAsync("fk_half_a", "fk_half_b");
    }

    [Fact]
    public void an_acyclic_migration_generates_exactly_the_same_ddl_as_before()
    {
        var parent = new Table(new SqlServerObjectName("dbo", "fk_parent"));
        parent.AddColumn<int>("id").AsPrimaryKey();

        var child = BuildTable("fk_child", "fk_parent", "fk_child_to_parent");

        var migrator = new SqlServerMigrator();
        var migration = new SchemaMigration(new ISchemaObjectDelta[]
        {
            new TableDelta(parent, null), new TableDelta(child, null)
        });

        var actual = new StringWriter();
        migration.WriteAllUpdates(actual, migrator, AutoCreate.All);

        var expected = new StringWriter();
        parent.WriteCreateStatement(migrator, expected);
        child.WriteCreateStatement(migrator, expected);

        actual.ToString().ShouldBe(expected.ToString());
    }

    [Fact]
    public void a_target_created_earlier_is_not_deferred_against_a_later_duplicate_of_itself()
    {
        var parent = new Table(new SqlServerObjectName("dbo", "fk_parent"));
        parent.AddColumn<int>("id").AsPrimaryKey();

        var child = BuildTable("fk_child", "fk_parent", "fk_child_to_parent");

        var migrator = new SqlServerMigrator();
        var migration = new SchemaMigration(new ISchemaObjectDelta[]
        {
            new TableDelta(parent, null), new TableDelta(child, null), new TableDelta(parent, null)
        });

        var actual = new StringWriter();
        migration.WriteAllUpdates(actual, migrator, AutoCreate.All);

        var expected = new StringWriter();
        parent.WriteCreateStatement(migrator, expected);
        child.WriteCreateStatement(migrator, expected);
        parent.WriteCreateStatement(migrator, expected);

        actual.ToString().ShouldBe(expected.ToString());
    }

    [Fact]
    public void a_cyclic_migration_moves_the_backward_key_to_the_end()
    {
        var a = BuildTable("fk_cycle_a", "fk_cycle_b", "fk_cycle_a_to_b");
        var b = BuildTable("fk_cycle_b", "fk_cycle_a", "fk_cycle_b_to_a");

        var migrator = new SqlServerMigrator();
        var migration = new SchemaMigration(new ISchemaObjectDelta[]
        {
            new TableDelta(a, null), new TableDelta(b, null)
        });

        var writer = new StringWriter();
        migration.WriteAllUpdates(writer, migrator, AutoCreate.All);
        var sql = writer.ToString();

        sql.IndexOf("fk_cycle_a_to_b", StringComparison.Ordinal)
            .ShouldBeGreaterThan(sql.IndexOf("CREATE TABLE dbo.fk_cycle_b", StringComparison.Ordinal));

        sql.IndexOf("fk_cycle_b_to_a", StringComparison.Ordinal)
            .ShouldBeGreaterThan(sql.IndexOf("CREATE TABLE dbo.fk_cycle_a", StringComparison.Ordinal));
    }
}
