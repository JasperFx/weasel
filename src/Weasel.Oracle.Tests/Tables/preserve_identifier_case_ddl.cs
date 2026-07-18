using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Tables;

/// <summary>
///     Pure DDL-generation tests (no database) for
///     <see cref="ITable.PreserveIdentifierCase" /> on Oracle: case-preserved
///     tables must emit quoted identifiers everywhere or Oracle folds them to
///     uppercase, breaking consumers (EF Core) that emit quoted PascalCase SQL.
/// </summary>
public class preserve_identifier_case_ddl
{
    [Fact]
    public void folded_convention_is_unchanged_by_default()
    {
        var table = new Table(new DbObjectName("weasel", "blogs"));
        table.AddColumn("Id", "NUMBER(10)").AsPrimaryKey();
        table.AddColumn("Name", "VARCHAR2(200)");

        // default behavior: lowercased names, unquoted DDL
        table.Columns.Select(x => x.Name).ShouldBe(["id", "name"]);
        var ddl = table.ToBasicCreateTableSql();
        ddl.ShouldContain("id");
        ddl.ShouldNotContain("\"Id\"");
    }

    [Fact]
    public void preserved_columns_keep_case_and_are_quoted()
    {
        var table = new Table(new DbObjectName("weasel", "Blogs"));
        ((ITable)table).PreserveIdentifierCase = true;
        table.AddColumn("Id", "NUMBER(10)").AsPrimaryKey();
        table.AddColumn("Name", "VARCHAR2(200)");
        table.PrimaryKeyName = "PK_Blogs";

        table.Columns.Select(x => x.Name).ShouldBe(["Id", "Name"]);

        var ddl = table.ToBasicCreateTableSql();
        ddl.ShouldContain("\"Id\"");
        ddl.ShouldContain("\"Name\"");
        ddl.ShouldContain("CONSTRAINT \"PK_Blogs\" PRIMARY KEY (\"Id\")");
    }

    [Fact]
    public void preserved_foreign_keys_are_quoted()
    {
        var table = new Table(new DbObjectName("weasel", "Posts"));
        ((ITable)table).PreserveIdentifierCase = true;
        table.AddColumn("Id", "NUMBER(10)").AsPrimaryKey();
        table.AddColumn("BlogId", "NUMBER(10)");

        var fk = ((ITable)table).AddForeignKey(
            "FK_Posts_Blogs_BlogId",
            new DbObjectName("weasel", "Blogs"),
            ["BlogId"],
            ["Id"]);
        fk.DeleteAction = CascadeAction.Cascade;

        var writer = new StringWriter();
        table.ForeignKeys.Single().WriteAddStatement(table, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("ADD CONSTRAINT \"FK_Posts_Blogs_BlogId\" FOREIGN KEY(\"BlogId\")");
        ddl.ShouldContain("(\"Id\")");
    }

    [Fact]
    public void preserved_indexes_flow_through_the_core_seam()
    {
        var table = new Table(new DbObjectName("weasel", "Posts"));
        var asTable = (ITable)table;
        asTable.PreserveIdentifierCase = true;
        table.AddColumn("BlogId", "NUMBER(10)");

        var index = asTable.AddIndex("IX_Posts_BlogId", ["BlogId"]);

        index.Name.ShouldBe("IX_Posts_BlogId");
        table.Indexes.Single().Columns.ShouldBe(["BlogId"]);
    }
}
