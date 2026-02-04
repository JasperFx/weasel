using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
/// Tests to verify fluent API parity with PostgreSQL for ColumnExpression
/// </summary>
public class FluentApiParityTests
{
    [Fact]
    public void add_index_on_column()
    {
        var table = new Table("users");
        table.AddColumn<string>("email").AddIndex();

        table.Indexes.Count.ShouldBe(1);
        table.Indexes[0].Name.ShouldBe("idx_users_email");
        table.Indexes[0].Columns.ShouldBe(new[] { "email" });
    }

    [Fact]
    public void add_index_with_configuration()
    {
        var table = new Table("users");
        table.AddColumn<string>("email").AddIndex(idx =>
        {
            idx.IsUnique = true;
            idx.Predicate = "email IS NOT NULL";
        });

        table.Indexes.Count.ShouldBe(1);
        table.Indexes[0].IsUnique.ShouldBeTrue();
        table.Indexes[0].Predicate.ShouldBe("email IS NOT NULL");
    }

    [Fact]
    public void add_index_throws_when_index_is_ignored()
    {
        var table = new Table("users");
        table.IgnoreIndex("idx_users_email");

        Should.Throw<ArgumentException>(() =>
        {
            table.AddColumn<string>("email").AddIndex();
        });
    }

    [Fact]
    public void foreign_key_to_with_table_name_string()
    {
        var table = new Table("posts");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("user_id").ForeignKeyTo("users", "id");

        table.ForeignKeys.Count.ShouldBe(1);
        table.ForeignKeys[0].Name.ShouldBe("fkey_posts_user_id");
        table.ForeignKeys[0].ColumnNames.ShouldBe(new[] { "user_id" });
        table.ForeignKeys[0].LinkedNames.ShouldBe(new[] { "id" });
        table.ForeignKeys[0].LinkedTable.Name.ShouldBe("users");
    }

    [Fact]
    public void foreign_key_to_with_table_object()
    {
        var usersTable = new Table("users");
        usersTable.AddColumn<int>("id").AsPrimaryKey();

        var postsTable = new Table("posts");
        postsTable.AddColumn<int>("id").AsPrimaryKey();
        postsTable.AddColumn<int>("user_id").ForeignKeyTo(usersTable, "id");

        postsTable.ForeignKeys.Count.ShouldBe(1);
        postsTable.ForeignKeys[0].LinkedTable.Name.ShouldBe("users");
    }

    [Fact]
    public void foreign_key_to_with_custom_name()
    {
        var table = new Table("posts");
        table.AddColumn<int>("user_id")
            .ForeignKeyTo("users", "id", fkName: "fk_posts_users");

        table.ForeignKeys.Count.ShouldBe(1);
        table.ForeignKeys[0].Name.ShouldBe("fk_posts_users");
    }

    [Fact]
    public void foreign_key_to_with_cascade_actions()
    {
        var table = new Table("posts");
        table.AddColumn<int>("user_id")
            .ForeignKeyTo("users", "id",
                onDelete: CascadeAction.Cascade,
                onUpdate: CascadeAction.SetNull);

        table.ForeignKeys.Count.ShouldBe(1);
        table.ForeignKeys[0].DeleteAction.ShouldBe(CascadeAction.Cascade);
        table.ForeignKeys[0].UpdateAction.ShouldBe(CascadeAction.SetNull);
    }

    [Fact]
    public void foreign_key_to_with_db_object_name()
    {
        var referencedName = DbObjectName.Parse(SqliteProvider.Instance, "users");
        var table = new Table("posts");
        table.AddColumn<int>("user_id").ForeignKeyTo(referencedName, "id");

        table.ForeignKeys.Count.ShouldBe(1);
        table.ForeignKeys[0].LinkedTable.Name.ShouldBe("users");
    }

    [Fact]
    public void foreign_key_to_with_sqlite_object_name()
    {
        var referencedName = new SqliteObjectName("users");
        var table = new Table("posts");
        table.AddColumn<int>("user_id").ForeignKeyTo(referencedName, "id");

        table.ForeignKeys.Count.ShouldBe(1);
        table.ForeignKeys[0].LinkedTable.Name.ShouldBe("users");
    }

    [Fact]
    public void chaining_multiple_fluent_calls()
    {
        var table = new Table("users");
        table.AddColumn<string>("email")
            .NotNull()
            .AddIndex(idx => idx.IsUnique = true);

        table.Columns[0].AllowNulls.ShouldBeFalse();
        table.Indexes.Count.ShouldBe(1);
        table.Indexes[0].IsUnique.ShouldBeTrue();
    }

    [Fact]
    public void table_move_to_schema()
    {
        var table = new Table("users");
        table.Identifier.Schema.ShouldBe("main");
        table.Identifier.Name.ShouldBe("users");

        table.MoveToSchema("temp");

        table.Identifier.Schema.ShouldBe("temp");
        table.Identifier.Name.ShouldBe("users");
    }

    [Fact]
    public void table_exists_in_database_async_is_present()
    {
        var table = new Table("users");

        // Just verify the method exists and has the right signature
        var method = typeof(Table).GetMethod("ExistsInDatabaseAsync");
        method.ShouldNotBeNull();
        method.ReturnType.Name.ShouldBe("Task`1"); // Task<bool>
    }
}
