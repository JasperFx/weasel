using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

public class TableTests
{
    private static string createSql(Table table, Migrator? migrator = null)
    {
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator ?? new FirebirdMigrator(), writer);
        return writer.ToString();
    }

    private static Table people()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name").NotNull();
        return table;
    }

    [Fact]
    public void a_bare_name_lives_in_the_default_schema()
    {
        var table = new Table("people");

        table.Identifier.ShouldBeOfType<FirebirdObjectName>();
        table.Identifier.Schema.ShouldBe("PUBLIC");
        table.Identifier.Name.ShouldBe("people");
    }

    [Fact]
    public void a_hand_built_identifier_is_normalized()
    {
#pragma warning disable CS0618
        var table = new Table(new DbObjectName("PUBLIC", "people"));
#pragma warning restore CS0618

        table.Identifier.ShouldBeOfType<FirebirdObjectName>();
        table.Identifier.QualifiedName.ShouldBe("people");
    }

    [Fact]
    public void the_default_primary_key_name_is_derived_from_the_table_alone()
    {
        people().PrimaryKeyName.ShouldBe("pk_people");
    }

    [Fact]
    public void the_primary_key_columns_come_from_the_flagged_columns()
    {
        var table = new Table("things");
        table.AddColumn<int>("a").AsPrimaryKey();
        table.AddColumn<int>("b");
        table.AddColumn<int>("c").AsPrimaryKey();

        table.PrimaryKeyColumns.ShouldBe(["a", "c"]);
    }

    [Fact]
    public void create_is_one_guarded_create_table_with_the_key_inline()
    {
        var sql = createSql(people());

        var statements = FirebirdScript.Split(sql);
        statements.Count.ShouldBe(1);

        var statement = statements.Single().Replace("\r\n", "\n");
        statement.ShouldStartWith("EXECUTE BLOCK AS");
        statement.ShouldContain("IF (NOT EXISTS(SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'PEOPLE')) THEN");
        statement.ShouldContain("EXECUTE STATEMENT 'CREATE TABLE people (");
        statement.ShouldContain("CONSTRAINT pk_people PRIMARY KEY (id)");
    }

    [Fact]
    public void the_create_is_isql_form()
    {
        createSql(people()).ShouldStartWith("SET TERM ^ ;");
    }

    [Fact]
    public void concise_formatting_writes_one_declaration_per_line()
    {
        var sql = createSql(people(), new FirebirdMigrator { Formatting = SqlFormatting.Concise });

        sql.ShouldContain("id INTEGER NOT NULL,\n");
        sql.ShouldContain("first_name VARCHAR(255),\n");
        sql.ShouldContain("last_name VARCHAR(255) NOT NULL,\n");
        sql.ShouldContain("CONSTRAINT pk_people PRIMARY KEY (id)\n)';");
    }

    [Fact]
    public void pretty_formatting_lines_the_columns_up()
    {
        var sql = createSql(people(), new FirebirdMigrator { Formatting = SqlFormatting.Pretty });

        sql.ShouldContain("    id            INTEGER         NOT NULL,");
        sql.ShouldContain("    first_name    VARCHAR(255),");
    }

    /// <summary>
    ///     weasel#643: a default is inside the EXECUTE STATEMENT literal, so its quotes are doubled --
    ///     once, in the one place that writes the literal.
    /// </summary>
    [Fact]
    public void a_string_default_survives_the_statement_literal()
    {
        var table = new Table("notes");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("body").DefaultValueByString("it's; done");

        createSql(table).ShouldContain("DEFAULT ''it''''s; done''");
    }

    [Fact]
    public void foreign_keys_and_indexes_are_statements_of_their_own()
    {
        var table = people();
        table.AddColumn<int>("state_id").ForeignKeyTo("states", "id");
        table.ModifyColumn("last_name").AddIndex();

        var statements = FirebirdScript.Split(createSql(table));

        statements.Count.ShouldBe(3);
        statements[1].ShouldContain("RDB$CONSTRAINT_NAME = 'FK_PEOPLE_STATE_ID'");
        statements[1].ShouldContain("ALTER TABLE people ADD CONSTRAINT fk_people_state_id FOREIGN KEY (state_id) REFERENCES states (id)");
        statements[2].ShouldContain("RDB$INDEX_NAME = 'IDX_PEOPLE_LAST_NAME'");
        statements[2].ShouldContain("CREATE INDEX idx_people_last_name ON people (last_name)");
    }

    [Fact]
    public void drop_then_create_drops_first()
    {
        var statements = FirebirdScript.Split(createSql(people(),
            new FirebirdMigrator { TableCreation = CreationStyle.DropThenCreate }));

        statements.Count.ShouldBe(3);
        statements[1].ShouldContain("'DROP TABLE people'");
        statements[2].ShouldContain("CREATE TABLE people");
    }

    /// <summary>
    ///     Firebird has no DROP TABLE … CASCADE, and refuses to drop a table another one references.
    /// </summary>
    [Fact]
    public void drop_removes_the_keys_that_reference_the_table_then_the_table()
    {
        var writer = new StringWriter();
        people().WriteDropStatement(new FirebirdMigrator(), writer);

        var statements = FirebirdScript.Split(writer.ToString());

        statements.Count.ShouldBe(2);
        statements[0].ShouldContain("WHERE pk.RDB$RELATION_NAME = 'PEOPLE' AND fk.RDB$RELATION_NAME <> 'PEOPLE'");
        statements[0].ShouldContain("DROP CONSTRAINT");
        statements[1].ShouldContain("IF (EXISTS(SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'PEOPLE'))");
        statements[1].ShouldContain("'DROP TABLE people'");
    }

    [Fact]
    public void a_table_in_another_schema_is_refused_before_anything_is_written()
    {
        var table = new Table("sales.people");
        table.AddColumn<int>("id");

        var writer = new StringWriter();
        Should.Throw<NotSupportedException>(() => table.WriteCreateStatement(new FirebirdMigrator(), writer))
            .Message.ShouldContain("table people in schema 'sales'");
        writer.ToString().ShouldBeEmpty();
    }

    [Fact]
    public void a_table_without_columns_is_refused()
    {
        Should.Throw<InvalidOperationException>(() => createSql(new Table("empty")));
    }

    /// <summary>
    ///     A6: Firebird 3's limit is 31 bytes, and Firebird refuses a longer name rather than truncating
    ///     it -- so a table name over 28 characters cannot take the derived <c>pk_{table}</c>.
    /// </summary>
    [Fact]
    public void a_derived_primary_key_name_over_the_limit_is_refused_with_the_setting_to_use()
    {
        var table = new Table("a_table_name_of_29_characters");
        table.AddColumn<int>("id").AsPrimaryKey();

        var ex = Should.Throw<InvalidOperationException>(() => createSql(table));
        ex.Message.ShouldContain("pk_a_table_name_of_29_characters");
        ex.Message.ShouldContain("PrimaryKeyName");
    }

    [Fact]
    public void a_derived_primary_key_name_fits_firebird_4s_limit()
    {
        var table = new Table("a_table_name_of_29_characters");
        table.AddColumn<int>("id").AsPrimaryKey();

        createSql(table, new FirebirdMigrator { MaxIdentifierLength = 63 })
            .ShouldContain("CONSTRAINT pk_a_table_name_of_29_characters PRIMARY KEY");
    }

    [Fact]
    public void an_explicit_primary_key_name_is_left_to_the_migration_path()
    {
        var table = new Table("a_table_name_of_29_characters");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.PrimaryKeyName = "pk_short";

        createSql(table).ShouldContain("CONSTRAINT pk_short PRIMARY KEY");
    }

    [Fact]
    public void check_constraints_are_refused_rather_than_ignored()
    {
        var table = people();

        Should.Throw<NotSupportedException>(() => table.CheckConstraints.Add(new TableCheckConstraint("ck", "id > 0")))
            .Message.ShouldContain("Firebird");
    }

    [Fact]
    public void all_names_are_the_table_its_indexes_and_its_foreign_keys()
    {
        var table = people();
        table.AddColumn<int>("state_id").ForeignKeyTo("states", "id");
        table.ModifyColumn("last_name").AddIndex();

        table.AllNames().Select(x => x.Name).ShouldBe(["people", "idx_people_last_name", "fk_people_state_id"]);
        table.AllNames().ShouldAllBe(x => x is FirebirdObjectName);
    }

    [Fact]
    public void local_identifiers_are_the_columns_and_the_key_name()
    {
        people().LocalIdentifiers().ShouldBe(["id", "first_name", "last_name", "pk_people"]);
    }

    [Fact]
    public void a_case_preserving_table_delimits_every_name_exactly()
    {
        var table = new Table("Blogs") { PreserveIdentifierCase = true };
        table.AddColumn<int>("BlogId").AsPrimaryKey();
        table.AddColumn<string>("Url");
        table.PrimaryKeyName = "PK_Blogs";

        var sql = createSql(table, new FirebirdMigrator { Formatting = SqlFormatting.Concise });

        sql.ShouldContain("RDB$RELATION_NAME = 'Blogs'");
        sql.ShouldContain("CREATE TABLE \"Blogs\" (");
        sql.ShouldContain("\"BlogId\" INTEGER NOT NULL");
        sql.ShouldContain("CONSTRAINT \"PK_Blogs\" PRIMARY KEY (\"BlogId\")");
    }

    [Fact]
    public void a_reserved_or_spaced_name_is_delimited_in_the_folded_spelling()
    {
        var table = new Table("order");
        table.AddColumn<int>("value").AsPrimaryKey();
        table.AddColumn<string>("order date");

        var sql = createSql(table, new FirebirdMigrator { Formatting = SqlFormatting.Concise });

        sql.ShouldContain("RDB$RELATION_NAME = 'ORDER'");
        sql.ShouldContain("CREATE TABLE \"ORDER\" (");
        sql.ShouldContain("\"ORDER DATE\" VARCHAR(255)");
    }

    [Fact]
    public void an_enum_column_needs_an_explicit_type()
    {
        Should.Throw<InvalidOperationException>(() => new Table("t").AddColumn<DayOfWeek>("day"));
    }

    [Fact]
    public void add_column_by_clr_type()
    {
        var table = new Table("t");
        table.AddColumn<Guid>("id");
        table.AddColumn<DateTimeOffset>("at");

        table.Columns.Select(x => x.Type).ShouldBe(["CHAR(16) CHARACTER SET OCTETS", "TIMESTAMP WITH TIME ZONE"]);
    }

    [Fact]
    public void an_identity_column_is_not_null()
    {
        var table = new Table("t");
        table.AddColumn<long>("id").AutoIncrement();

        table.Columns.Single().AllowNulls.ShouldBeFalse();
        table.Columns.Single().IsAutoNumber.ShouldBeTrue();
    }

    [Fact]
    public void defaults_by_value()
    {
        var table = new Table("t");
        table.AddColumn<int>("a").DefaultValue(3);
        table.AddColumn<long>("b").DefaultValue(4L);
        table.AddColumn<double>("c").DefaultValue(1.5);
        table.AddColumn<string>("d").DefaultValueByString("x");

        table.Columns.Select(x => x.DefaultExpression).ShouldBe(["3", "4", "1.5", "'x'"]);
    }

    [Fact]
    public void a_foreign_key_to_a_hand_built_name_is_normalized()
    {
        var table = new Table("people");
#pragma warning disable CS0618
        table.AddColumn<int>("state_id").ForeignKeyTo(new DbObjectName("PUBLIC", "states"), "id",
            onDelete: CascadeAction.Cascade);
#pragma warning restore CS0618

        var foreignKey = table.ForeignKeys.Single();
        foreignKey.LinkedTable.ShouldBeOfType<FirebirdObjectName>();
        foreignKey.DeleteAction.ShouldBe(CascadeAction.Cascade);
    }

    [Fact]
    public void find_or_create_foreign_key_ignores_case()
    {
        var table = new Table("people");
        var first = table.FindOrCreateForeignKey("fk_a");

        table.FindOrCreateForeignKey("FK_A").ShouldBeSameAs(first);
        table.ForeignKeys.Count.ShouldBe(1);
    }

    [Fact]
    public void modifying_a_missing_column_is_refused()
    {
        Should.Throw<ArgumentOutOfRangeException>(() => new Table("t").ModifyColumn("nope"));
    }

    [Fact]
    public void column_lookup_ignores_case()
    {
        people().HasColumn("FIRST_NAME").ShouldBeTrue();
    }
}
