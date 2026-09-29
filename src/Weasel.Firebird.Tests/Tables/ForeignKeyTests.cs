using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

public class ForeignKeyTests
{
    private readonly Table people = new("people");

    private static ForeignKey key(CascadeAction onDelete = CascadeAction.NoAction,
        CascadeAction onUpdate = CascadeAction.NoAction) => new("fk_people_state")
    {
        LinkedTable = new FirebirdObjectName("states"),
        ColumnNames = ["state_id"],
        LinkedNames = ["id"],
        DeleteAction = onDelete,
        UpdateAction = onUpdate
    };

    [Fact]
    public void writes_an_add_constraint_statement()
    {
        key().ToDDL(people)
            .ShouldBe("ALTER TABLE people ADD CONSTRAINT fk_people_state FOREIGN KEY (state_id) REFERENCES states (id)");
    }

    [Theory]
    [InlineData(CascadeAction.Cascade, "ON DELETE CASCADE")]
    [InlineData(CascadeAction.SetNull, "ON DELETE SET NULL")]
    [InlineData(CascadeAction.SetDefault, "ON DELETE SET DEFAULT")]
    public void writes_the_delete_action(CascadeAction action, string clause)
    {
        key(onDelete: action).ToDDL(people).ShouldEndWith(clause);
    }

    [Fact]
    public void writes_the_update_action()
    {
        key(CascadeAction.Cascade, CascadeAction.SetNull).ToDDL(people)
            .ShouldEndWith("ON DELETE CASCADE ON UPDATE SET NULL");
    }

    /// <summary>
    ///     <c>ON DELETE RESTRICT</c> is a syntax error in Firebird; no clause is what it means.
    /// </summary>
    [Fact]
    public void restrict_and_no_action_write_no_clause()
    {
        key(CascadeAction.Restrict, CascadeAction.Restrict).ToDDL(people).ShouldNotContain(" ON ");
        key().ToDDL(people).ShouldNotContain(" ON ");
    }

    [Fact]
    public void a_composite_key_keeps_its_column_order()
    {
        var composite = new ForeignKey("fk_triggers_job")
        {
            LinkedTable = new FirebirdObjectName("QRTZ_JOB_DETAILS"),
            ColumnNames = ["SCHED_NAME", "JOB_NAME", "JOB_GROUP"],
            LinkedNames = ["SCHED_NAME", "JOB_NAME", "JOB_GROUP"]
        };

        composite.ToDDL(new Table("QRTZ_TRIGGERS")).ShouldBe(
            "ALTER TABLE QRTZ_TRIGGERS ADD CONSTRAINT fk_triggers_job FOREIGN KEY (SCHED_NAME, JOB_NAME, JOB_GROUP) REFERENCES QRTZ_JOB_DETAILS (SCHED_NAME, JOB_NAME, JOB_GROUP)");
    }

    [Fact]
    public void names_are_quoted_where_firebird_needs_it()
    {
        var quoted = new ForeignKey("fk order")
        {
            LinkedTable = new FirebirdObjectName("order"),
            ColumnNames = ["order id"],
            LinkedNames = ["value"]
        };

        quoted.ToDDL(people).ShouldBe(
            "ALTER TABLE people ADD CONSTRAINT \"FK ORDER\" FOREIGN KEY (\"ORDER ID\") REFERENCES \"ORDER\" (\"VALUE\")");
    }

    [Fact]
    public void a_key_to_another_schema_is_refused()
    {
        var elsewhere = key();
        elsewhere.LinkedTable = new FirebirdObjectName("sales", "states");

        Should.Throw<NotSupportedException>(() => elsewhere.ToDDL(people)).Message.ShouldContain("sales");
    }

    [Fact]
    public void a_key_without_a_linked_table_is_refused()
    {
        Should.Throw<InvalidOperationException>(() => new ForeignKey("fk").ToDDL(people));
    }

    /// <summary>
    ///     Constraint and index names share one namespace in a Firebird database, and the guard is on
    ///     the constraint catalog.
    /// </summary>
    [Fact]
    public void the_add_is_guarded_on_the_constraint_name()
    {
        var writer = new StringWriter();
        key().WriteAddStatement(people, writer);

        var sql = writer.ToString();
        sql.ShouldContain("IF (NOT EXISTS(SELECT 1 FROM RDB$RELATION_CONSTRAINTS WHERE RDB$CONSTRAINT_NAME = 'FK_PEOPLE_STATE'))");
        FirebirdScript.Split(sql).Count.ShouldBe(1);
    }

    [Fact]
    public void the_drop_runs_only_while_the_key_exists()
    {
        var writer = new StringWriter();
        key().WriteDropStatement(people, writer);

        var sql = writer.ToString();
        sql.ShouldContain("IF (EXISTS(SELECT 1 FROM RDB$RELATION_CONSTRAINTS WHERE RDB$CONSTRAINT_NAME = 'FK_PEOPLE_STATE'))");
        sql.ShouldContain("'ALTER TABLE people DROP CONSTRAINT fk_people_state'");
    }

    /// <summary>
    ///     The catalog records a key declared without an action as RESTRICT, so a model that says
    ///     nothing must not drift against it.
    /// </summary>
    [Fact]
    public void restrict_read_back_equals_no_action()
    {
        var catalog = key();
        catalog.ReadReferentialActions("RESTRICT", "RESTRICT");

        catalog.DeleteAction.ShouldBe(CascadeAction.Restrict);
        key().IsEquivalentTo(catalog).ShouldBeTrue();
        key().GetHashCode().ShouldBe(catalog.GetHashCode());
    }

    [Fact]
    public void compares_names_and_columns_ignoring_case()
    {
        var catalog = new ForeignKey("FK_PEOPLE_STATE")
        {
            LinkedTable = new FirebirdObjectName("STATES"),
            ColumnNames = ["STATE_ID"],
            LinkedNames = ["ID"]
        };

        key().IsEquivalentTo(catalog).ShouldBeTrue();
    }

    [Fact]
    public void a_different_action_is_a_different_key()
    {
        key(CascadeAction.Cascade).IsEquivalentTo(key()).ShouldBeFalse();
    }

    [Fact]
    public void parses_a_linked_table_into_a_firebird_name()
    {
        var parsed = new ForeignKey("fk");
        parsed.Parse("FOREIGN KEY (state_id) REFERENCES states(id) ON DELETE CASCADE");

        parsed.LinkedTable.ShouldBeOfType<FirebirdObjectName>();
        parsed.LinkedTable!.QualifiedName.ShouldBe("states");
        parsed.DeleteAction.ShouldBe(CascadeAction.Cascade);
    }

    [Fact]
    public void a_case_preserving_table_delimits_every_name_exactly()
    {
        var blogs = new Table("Posts") { PreserveIdentifierCase = true };
        var foreignKey = new ForeignKey("FK_Posts_Blogs_BlogId")
        {
            LinkedTable = new FirebirdObjectName("Blogs"),
            ColumnNames = ["BlogId"],
            LinkedNames = ["Id"]
        };

        foreignKey.ToDDL(blogs).ShouldBe(
            "ALTER TABLE \"Posts\" ADD CONSTRAINT \"FK_Posts_Blogs_BlogId\" FOREIGN KEY (\"BlogId\") REFERENCES \"Blogs\" (\"Id\")");
    }
}
