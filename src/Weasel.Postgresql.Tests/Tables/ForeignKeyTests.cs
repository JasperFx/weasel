using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

using static PostgresqlProvider;

public class ForeignKeyTests
{
    [Fact]
    public void write_fk_ddl()
    {
        var table = new Table("people");
        var fk = new ForeignKey("fk_state")
        {
            LinkedTable = ToDbObjectName("public", "states"),
            ColumnNames = new[] { "state_id" },
            LinkedNames = new[] { "id" }
        };

        var ddl = fk.ToDDL(table);
        ddl.ShouldNotContain("ON DELETE");
        ddl.ShouldNotContain("ON UPDATE");

        ddl.ShouldContain($"ALTER TABLE {ToDbObjectName("public.people")}");
        ddl.ShouldContain("ADD CONSTRAINT fk_state FOREIGN KEY(state_id)");
        ddl.ShouldContain($"REFERENCES {ToDbObjectName("public.states")}(id)");
    }

    [Fact]
    public void write_fk_ddl_with_on_delete()
    {
        var table = new Table("people");
        var fk = new ForeignKey("fk_state")
        {
            LinkedTable = PostgresqlProvider.ToDbObjectName("public", "states"),
            ColumnNames = new[] { "state_id" },
            LinkedNames = new[] { "id" },
            OnDelete = CascadeAction.Restrict
        };


        var ddl = fk.ToDDL(table);
        ddl.ShouldContain("ON DELETE RESTRICT");
        ddl.ShouldNotContain("ON UPDATE");
    }

    [Fact]
    public void write_fk_ddl_with_on_update()
    {
        var table = new Table("people");
        var fk = new ForeignKey("fk_state")
        {
            LinkedTable = PostgresqlProvider.ToDbObjectName("public", "states"),
            ColumnNames = new[] { "state_id" },
            LinkedNames = new[] { "id" },
            OnUpdate = CascadeAction.Cascade
        };


        var ddl = fk.ToDDL(table);
        ddl.ShouldNotContain("ON DELETE");
        ddl.ShouldContain("ON UPDATE CASCADE");
    }

    [Fact]
    public void write_fk_ddl_with_multiple_columns()
    {
        var table = new Table("people");
        var fk = new ForeignKey("fk_state")
        {
            LinkedTable = PostgresqlProvider.ToDbObjectName("public", "states"),
            ColumnNames = new[] { "state_id", "tenant_id" },
            LinkedNames = new[] { "id", "tenant_id" }
        };

        var ddl = fk.ToDDL(table);


        ddl.ShouldContain($"ALTER TABLE {ToDbObjectName("public.people")}");
        ddl.ShouldContain("ADD CONSTRAINT fk_state FOREIGN KEY(state_id, tenant_id)");
        ddl.ShouldContain($"REFERENCES {ToDbObjectName("public.states")}(id, tenant_id)");
    }

    [Theory]
    [InlineData("FOREIGN KEY (state_id) REFERENCES states(id)", CascadeAction.NoAction, CascadeAction.NoAction)]
    [InlineData("FOREIGN KEY (state_id) REFERENCES states(id) ON DELETE CASCADE", CascadeAction.Cascade,
        CascadeAction.NoAction)]
    [InlineData("FOREIGN KEY (state_id) REFERENCES states(id) ON UPDATE CASCADE", CascadeAction.NoAction,
        CascadeAction.Cascade)]
    [InlineData("FOREIGN KEY (state_id) REFERENCES states(id) ON DELETE CASCADE ON UPDATE CASCADE",
        CascadeAction.Cascade, CascadeAction.Cascade)]
    [InlineData("FOREIGN KEY (state_id) REFERENCES states(id) ON DELETE CASCADE ON UPDATE RESTRICT",
        CascadeAction.Cascade, CascadeAction.Restrict)]
    [InlineData("FOREIGN KEY (state_id) REFERENCES states(id) ON DELETE CASCADE ON UPDATE SET NULL",
        CascadeAction.Cascade, CascadeAction.SetNull)]
    [InlineData("FOREIGN KEY (state_id) REFERENCES states(id) ON DELETE CASCADE ON UPDATE SET DEFAULT",
        CascadeAction.Cascade, CascadeAction.SetDefault)]
    [InlineData("FOREIGN KEY (state_id) REFERENCES states(id) ON DELETE RESTRICT", CascadeAction.Restrict,
        CascadeAction.NoAction)]
    [InlineData("FOREIGN KEY (state_id) REFERENCES states(id) ON DELETE SET DEFAULT", CascadeAction.SetDefault,
        CascadeAction.NoAction)]
    [InlineData("FOREIGN KEY (state_id) REFERENCES states(id) ON DELETE SET NULL", CascadeAction.SetNull,
        CascadeAction.NoAction)]
    public void read_on_delete_and_on_update(string definition, CascadeAction onDelete, CascadeAction onUpdate)
    {
        var fk = new ForeignKey("fk_people_state_id") { };

        fk.Parse(definition);
        fk.OnDelete.ShouldBe(onDelete);
        fk.OnUpdate.ShouldBe(onUpdate);
    }

    [Fact]
    public void parse_single_column_fk_definition()
    {
        var fk = new ForeignKey("fk_people_state_id") { };

        fk.Parse("FOREIGN KEY (state_id) REFERENCES states(id)");

        fk.ColumnNames.Single().ShouldBe("state_id");
        fk.LinkedNames.Single().ShouldBe("id");
        fk.LinkedTable.ShouldBe(PostgresqlProvider.ToDbObjectName("public", "states"));
    }

    [Fact]
    public void parse_multiple_column_fk_definition()
    {
        var fk = new ForeignKey("fk_people_state_id") { };

        fk.Parse("FOREIGN KEY (state_id, tenant_id) REFERENCES states(id, tenant_id)");

        fk.ColumnNames.ShouldBe(new string[] { "state_id", "tenant_id" });
        fk.LinkedNames.ShouldBe(new string[] { "id", "tenant_id" });
        fk.LinkedTable.ShouldBe(PostgresqlProvider.ToDbObjectName("public", "states"));
    }

    [Fact]
    public void parse_single_column_fk_definition_with_schema_set_explicitly()
    {
        var foreignKey = new ForeignKey("fk_people_state_id");
        const string definition = "FOREIGN KEY (state_id) REFERENCES states(id)";
        const string schema = "test";

        foreignKey.Parse(definition, schema);

        foreignKey.ColumnNames.Single().ShouldBe("state_id");
        foreignKey.LinkedNames.Single().ShouldBe("id");
        var expectedLinkedTable = PostgresqlProvider.ToDbObjectName(schema, "states");
        foreignKey.LinkedTable.ShouldBe(expectedLinkedTable);
    }
}
