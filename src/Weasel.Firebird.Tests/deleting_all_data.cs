using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     A8: <c>GenerateDeleteAllSql</c> is one <c>EXECUTE BLOCK</c>, because <c>DatabaseCleaner</c> runs
///     it as a single command; it empties the tables children first and restarts identities so the next
///     value is 1 -- which takes <c>RESTART WITH 0</c> on Firebird 3 and <c>RESTART WITH 1</c> on 4 and 5.
/// </summary>
public class deleting_all_data: IntegrationContext
{
    private async Task createParentAndChildAsync()
    {
        var parents = new Table("parents");
        parents.AddColumn<long>("id").AsPrimaryKey().AutoIncrement();
        parents.AddColumn<string>("name");

        var children = new Table("children");
        children.AddColumn<long>("id").AsPrimaryKey().AutoIncrement();
        children.AddColumn<long>("parent_id").ForeignKeyTo(parents, "id");

        await ApplyAsync(parents, children);

        await ExecuteAsync("""
            INSERT INTO parents (name) VALUES ('a');
            INSERT INTO parents (name) VALUES ('b');
            INSERT INTO children (parent_id) VALUES (1);
            INSERT INTO children (parent_id) VALUES (2);
            """);
    }

    /// <summary>
    ///     As <c>DatabaseCleaner</c> runs it: one command.
    /// </summary>
    private async Task runAsOneCommandAsync(string sql)
    {
        await using var cmd = theConnection.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    private static readonly DbObjectName[] ChildrenFirst =
        [new FirebirdObjectName("children"), new FirebirdObjectName("parents")];

    [Fact]
    public async Task deletes_every_row_children_first_as_one_command()
    {
        await createParentAndChildAsync();

        await runAsOneCommandAsync(new FirebirdMigrator().GenerateDeleteAllSql(ChildrenFirst));

        (await ScalarAsync<int>("SELECT COUNT(*) FROM parents")).ShouldBe(0);
        (await ScalarAsync<int>("SELECT COUNT(*) FROM children")).ShouldBe(0);
    }

    [Fact]
    public async Task restarts_identities_so_the_next_value_is_one()
    {
        await createParentAndChildAsync();

        await runAsOneCommandAsync(new FirebirdMigrator().GenerateDeleteAllSql(ChildrenFirst));
        await ExecuteAsync("INSERT INTO parents (name) VALUES ('fresh')");

        (await ScalarAsync<long>("SELECT id FROM parents")).ShouldBe(1L,
            $"Firebird {ServerVersion} restarts one past the value on 3 and at it on 4 and 5");
    }

    [Fact]
    public async Task can_leave_identities_where_they_are()
    {
        await createParentAndChildAsync();

        await runAsOneCommandAsync(new FirebirdMigrator().GenerateDeleteAllSql(ChildrenFirst, resetIdentity: false));
        await ExecuteAsync("INSERT INTO parents (name) VALUES ('fresh')");

        (await ScalarAsync<long>("SELECT id FROM parents")).ShouldBe(3L);
    }

    [Fact]
    public async Task finds_a_table_created_with_a_case_preserved_name()
    {
        var blogs = new Table("Blogs") { PreserveIdentifierCase = true };
        blogs.AddColumn<long>("Id").AsPrimaryKey().AutoIncrement();
        blogs.AddColumn<string>("Url");
        blogs.PrimaryKeyName = "PK_Blogs";
        await ApplyAsync(blogs);
        await ExecuteAsync("INSERT INTO \"Blogs\" (\"Url\") VALUES ('x')");

        await runAsOneCommandAsync(new FirebirdMigrator().GenerateDeleteAllSql([new FirebirdObjectName("Blogs")]));
        await ExecuteAsync("INSERT INTO \"Blogs\" (\"Url\") VALUES ('y')");

        (await ScalarAsync<long>("SELECT \"Id\" FROM \"Blogs\"")).ShouldBe(1L);
    }

    [Fact]
    public async Task the_async_form_is_the_same_sql()
    {
        var migrator = new FirebirdMigrator();

        (await migrator.GenerateDeleteAllSqlAsync(theConnection, ChildrenFirst))
            .ShouldBe(migrator.GenerateDeleteAllSql(ChildrenFirst));
    }
}
