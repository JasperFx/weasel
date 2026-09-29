using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     The delta against hand-built "catalog" tables -- upper-case names and catalog spellings, as the
///     reader produces them -- so every rule is pinned without a server.
/// </summary>
public class TableDeltaTests
{
    private static Table model()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("name", "VARCHAR(100)");
        return table;
    }

    private static Table catalog()
    {
        var table = new Table("PEOPLE");
        table.AddColumn("ID", "INTEGER").AsPrimaryKey();
        table.AddColumn("NAME", "VARCHAR(100) CHARACTER SET UTF8");
        table.PrimaryKeyName = "PK_PEOPLE";
        return table;
    }

    private static string[] update(TableDelta delta)
    {
        var writer = new StringWriter();
        delta.WriteUpdate(new FirebirdMigrator(), writer);
        return FirebirdScript.Split(writer.ToString()).ToArray();
    }

    private static string[] rollback(TableDelta delta)
    {
        var writer = new StringWriter();
        delta.WriteRollback(new FirebirdMigrator(), writer);
        return FirebirdScript.Split(writer.ToString()).ToArray();
    }

    [Fact]
    public void no_table_is_a_create()
    {
        var delta = new TableDelta(model(), null);

        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
        delta.HasChanges().ShouldBeTrue();
        update(delta).Single().ShouldContain("CREATE TABLE people");
    }

    [Fact]
    public void the_same_table_in_the_catalog_spelling_is_no_change()
    {
        new TableDelta(model(), catalog()).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void a_missing_column_is_added_with_a_guard()
    {
        var expected = model();
        expected.AddColumn<int>("age");

        var delta = new TableDelta(expected, catalog());

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Missing.Single().Name.ShouldBe("age");
        var statement = update(delta).Single();
        statement.ShouldContain("RDB$FIELD_NAME = 'AGE'");
        statement.ShouldContain("'ALTER TABLE people ADD age INTEGER'");
    }

    [Fact]
    public void an_extra_column_is_dropped()
    {
        var actual = catalog();
        actual.AddColumn("AGE", "INTEGER");

        var delta = new TableDelta(model(), actual);

        delta.Columns.Extras.Single().Name.ShouldBe("AGE");
        update(delta).Single().ShouldContain("'ALTER TABLE people DROP AGE'");
    }

    /// <summary>
    ///     weasel#629: a table translated from another model does not drop what it does not declare, and
    ///     says so.
    /// </summary>
    [Fact]
    public void an_add_only_table_withholds_the_drop()
    {
        var expected = model();
        expected.AddOnlyMigrations = true;

        var actual = catalog();
        actual.AddColumn("AGE", "INTEGER");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.WithheldDrops.ShouldBe(["column AGE"]);
    }

    [Fact]
    public void a_widened_column_is_altered_in_place()
    {
        var expected = model();
        expected.ModifyColumn("name").Column.Type = "VARCHAR(200)";

        var delta = new TableDelta(expected, catalog());

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        update(delta).Single().ShouldBe("ALTER TABLE people ALTER name TYPE VARCHAR(200)");
    }

    [Fact]
    public void a_narrowed_column_cannot_be_altered_in_place()
    {
        var expected = model();
        expected.ModifyColumn("name").Column.Type = "VARCHAR(50)";

        var delta = new TableDelta(expected, catalog());

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.InvalidReason!.ShouldContain("column 'name'");
        delta.InvalidReason!.ShouldContain("only widens");
    }

    /// <summary>
    ///     A4: Firebird refuses to change the type of a key column, widening included (335544538), and
    ///     Weasel never drops the key to make room.
    /// </summary>
    [Fact]
    public void widening_a_primary_key_column_is_invalid()
    {
        var expected = new Table("people");
        expected.AddColumn<long>("id").AsPrimaryKey();
        expected.AddColumn("name", "VARCHAR(100)");

        var delta = new TableDelta(expected, catalog());

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.InvalidReason!.ShouldContain("column 'id' (INTEGER to BIGINT)");
        delta.InvalidReason!.ShouldContain("primary key, unique index or foreign key");
    }

    [Fact]
    public void widening_a_column_a_unique_index_covers_is_invalid()
    {
        var expected = model();
        expected.ModifyColumn("name").Column.Type = "VARCHAR(200)";
        expected.Indexes.Add(new IndexDefinition("idx_name") { Columns = ["name"], IsUnique = true });

        var actual = catalog();
        actual.Indexes.Add(new IndexDefinition("IDX_NAME") { Columns = ["NAME"], IsUnique = true });

        new TableDelta(expected, actual).Difference.ShouldBe(SchemaPatchDifference.Invalid);
    }

    [Fact]
    public void widening_a_foreign_key_column_is_invalid()
    {
        var expected = model();
        expected.AddColumn<long>("state_id").ForeignKeyTo("states", "id");

        var actual = catalog();
        actual.AddColumn("STATE_ID", "INTEGER");
        actual.ForeignKeys.Add(new ForeignKey("FK_PEOPLE_STATE_ID")
        {
            LinkedTable = new FirebirdObjectName("STATES"), ColumnNames = ["STATE_ID"], LinkedNames = ["ID"]
        });

        new TableDelta(expected, actual).Difference.ShouldBe(SchemaPatchDifference.Invalid);
    }

    [Fact]
    public void widening_a_column_a_plain_index_covers_is_fine()
    {
        var expected = model();
        expected.ModifyColumn("name").Column.Type = "VARCHAR(200)";
        expected.Indexes.Add(new IndexDefinition("idx_name") { Columns = ["name"] });

        var actual = catalog();
        actual.Indexes.Add(new IndexDefinition("IDX_NAME") { Columns = ["NAME"] });

        new TableDelta(expected, actual).Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public void a_not_null_column_without_a_default_cannot_be_added()
    {
        var expected = model();
        expected.AddColumn<int>("age").NotNull();

        var delta = new TableDelta(expected, catalog());

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.InvalidReason!.ShouldContain("column 'age' cannot be added");
    }

    [Fact]
    public void a_not_null_column_with_a_default_can_be_added()
    {
        var expected = model();
        expected.AddColumn<int>("age").NotNull().DefaultValue(0);

        var delta = new TableDelta(expected, catalog());

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        update(delta).Single().ShouldContain("'ALTER TABLE people ADD age INTEGER DEFAULT 0 NOT NULL'");
    }

    [Fact]
    public void an_identity_column_cannot_be_added()
    {
        var expected = model();
        expected.AddColumn<long>("seq").AutoIncrement();

        new TableDelta(expected, catalog()).Difference.ShouldBe(SchemaPatchDifference.Invalid);
    }

    [Fact]
    public void identity_is_not_compared()
    {
        var expected = new Table("people");
        expected.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        expected.AddColumn("name", "VARCHAR(100)");

        new TableDelta(expected, catalog()).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void defaults_and_nullability_are_ignored_unless_asked_for()
    {
        var expected = model();
        expected.ModifyColumn("name").NotNull().DefaultValueByString("x");

        new TableDelta(expected, catalog()).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void column_drift_detection_alters_a_default_then_nullability()
    {
        var expected = model();
        expected.DetectColumnDrift = true;
        expected.ModifyColumn("name").NotNull().DefaultValueByString("x");

        var delta = new TableDelta(expected, catalog());

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        update(delta).ShouldBe([
            "ALTER TABLE people ALTER name SET DEFAULT 'x'",
            "ALTER TABLE people ALTER name SET NOT NULL"
        ]);
    }

    [Fact]
    public void column_drift_detection_reads_default_null_as_no_default()
    {
        var expected = model();
        expected.DetectColumnDrift = true;
        expected.ModifyColumn("name").DefaultValueByExpression("NULL");

        new TableDelta(expected, catalog()).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void column_drift_detection_drops_a_default_the_model_no_longer_has()
    {
        var expected = model();
        expected.DetectColumnDrift = true;

        var actual = catalog();
        actual.ModifyColumn("NAME").DefaultValueByString("x");

        update(new TableDelta(expected, actual)).ShouldBe(["ALTER TABLE people ALTER name DROP DEFAULT"]);
    }

    [Fact]
    public void a_missing_index_is_created_and_an_extra_one_dropped()
    {
        var expected = model();
        expected.Indexes.Add(new IndexDefinition("idx_name") { Columns = ["name"] });

        var actual = catalog();
        actual.Indexes.Add(new IndexDefinition("IDX_OLD") { Columns = ["NAME"] });

        var statements = update(new TableDelta(expected, actual));

        statements.Length.ShouldBe(2);
        statements[0].ShouldContain("'DROP INDEX IDX_OLD'");
        statements[1].ShouldContain("'CREATE INDEX idx_name ON people (name)'");
    }

    [Fact]
    public void a_changed_index_is_dropped_and_recreated()
    {
        var expected = model();
        expected.Indexes.Add(new IndexDefinition("idx_name") { Columns = ["name"], IsUnique = true });

        var actual = catalog();
        actual.Indexes.Add(new IndexDefinition("IDX_NAME") { Columns = ["NAME"] });

        var delta = new TableDelta(expected, actual);

        delta.Indexes.Different.Count.ShouldBe(1);
        var statements = update(delta);
        statements[0].ShouldContain("DROP INDEX IDX_NAME");
        statements[1].ShouldContain("CREATE UNIQUE INDEX idx_name");
    }

    /// <summary>
    ///     weasel#642: an index Weasel was told to leave alone is not dropped, on either side.
    /// </summary>
    [Fact]
    public void an_ignored_index_is_left_alone()
    {
        var expected = model();
        expected.IgnoreIndex("idx_theirs");

        var actual = catalog();
        actual.Indexes.Add(new IndexDefinition("IDX_THEIRS") { Columns = ["NAME"] });

        new TableDelta(expected, actual).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void a_missing_foreign_key_is_added_and_an_extra_one_dropped()
    {
        var expected = model();
        expected.AddColumn<int>("state_id").ForeignKeyTo("states", "id");

        var actual = catalog();
        actual.AddColumn("STATE_ID", "INTEGER");
        actual.ForeignKeys.Add(new ForeignKey("FK_OLD")
        {
            LinkedTable = new FirebirdObjectName("OTHERS"), ColumnNames = ["STATE_ID"], LinkedNames = ["ID"]
        });

        var statements = update(new TableDelta(expected, actual));

        statements.Length.ShouldBe(2);
        statements[0].ShouldContain("'ALTER TABLE people DROP CONSTRAINT FK_OLD'");
        statements[1].ShouldContain("ADD CONSTRAINT fk_people_state_id");
    }

    [Fact]
    public void a_foreign_key_read_back_as_restrict_is_no_change()
    {
        var expected = model();
        expected.AddColumn<int>("state_id").ForeignKeyTo("states", "id");

        var actual = catalog();
        actual.AddColumn("STATE_ID", "INTEGER");
        var foreignKey = new ForeignKey("FK_PEOPLE_STATE_ID")
        {
            LinkedTable = new FirebirdObjectName("STATES"), ColumnNames = ["STATE_ID"], LinkedNames = ["ID"]
        };
        foreignKey.ReadReferentialActions("RESTRICT", "RESTRICT");
        actual.ForeignKeys.Add(foreignKey);

        new TableDelta(expected, actual).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void the_key_order_is_compared_only_when_pinned()
    {
        var expected = new Table("people");
        expected.AddColumn<int>("a").AsPrimaryKey();
        expected.AddColumn<int>("b").AsPrimaryKey();

        var actual = new Table("PEOPLE");
        actual.AddColumn("A", "INTEGER").AsPrimaryKey();
        actual.AddColumn("B", "INTEGER").AsPrimaryKey();
        actual.SetPrimaryKeyOrder(["B", "A"]);

        new TableDelta(expected, actual).Difference.ShouldBe(SchemaPatchDifference.None);

        expected.SetPrimaryKeyOrder(["a", "b"]);
        var delta = new TableDelta(expected, actual);

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);
        var statements = update(delta);
        statements[0].ShouldContain("DROP CONSTRAINT");
        statements[1].ShouldContain("ADD CONSTRAINT pk_people PRIMARY KEY (a, b)");
    }

    [Fact]
    public void a_missing_primary_key_is_added()
    {
        var actual = catalog();
        actual.ModifyColumn("ID").Column.IsPrimaryKey = false;

        var delta = new TableDelta(model(), actual);

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Create);
        update(delta).Single().ShouldContain("RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'");
    }

    /// <summary>
    ///     Constraints and indexes come off before the columns under them change, and go back on after.
    /// </summary>
    [Fact]
    public void the_update_runs_in_the_order_firebird_accepts()
    {
        var expected = model();
        expected.AddColumn<int>("age");
        expected.Indexes.Add(new IndexDefinition("idx_age") { Columns = ["age"] });
        expected.AddColumn<int>("state_id").ForeignKeyTo("states", "id");

        var actual = catalog();
        actual.AddColumn("OLD", "INTEGER");
        actual.AddColumn("STATE_ID", "INTEGER");
        actual.Indexes.Add(new IndexDefinition("IDX_OLD") { Columns = ["OLD"] });
        actual.ForeignKeys.Add(new ForeignKey("FK_OLD")
        {
            LinkedTable = new FirebirdObjectName("OTHERS"), ColumnNames = ["OLD"], LinkedNames = ["ID"]
        });

        var statements = update(new TableDelta(expected, actual));

        int at(string fragment) => Array.FindIndex(statements, x => x.Contains(fragment));

        at("DROP CONSTRAINT FK_OLD").ShouldBeLessThan(at("DROP INDEX IDX_OLD"));
        at("DROP INDEX IDX_OLD").ShouldBeLessThan(at("DROP OLD"));
        at("DROP OLD").ShouldBeLessThan(at("ADD age"));
        at("ADD age").ShouldBeLessThan(at("CREATE INDEX idx_age"));
        at("CREATE INDEX idx_age").ShouldBeLessThan(at("ADD CONSTRAINT fk_people_state_id"));
    }

    [Fact]
    public void writing_an_invalid_delta_says_why()
    {
        var expected = model();
        expected.ModifyColumn("name").Column.Type = "VARCHAR(50)";

        var delta = new TableDelta(expected, catalog());

        Should.Throw<InvalidOperationException>(() => update(delta)).Message.ShouldContain("only widens");
    }

    [Fact]
    public void the_rollback_undoes_the_update()
    {
        var expected = model();
        expected.AddColumn<int>("age");
        expected.Indexes.Add(new IndexDefinition("idx_age") { Columns = ["age"] });

        var actual = catalog();
        actual.AddColumn("OLD", "INTEGER");

        var statements = rollback(new TableDelta(expected, actual));

        statements.Length.ShouldBe(3);
        statements[0].ShouldContain("'DROP INDEX idx_age'");
        statements[1].ShouldContain("'ALTER TABLE people DROP age'");
        statements[2].ShouldContain("'ALTER TABLE people ADD OLD INTEGER'");
    }

    [Fact]
    public void the_rollback_of_a_widening_says_it_cannot_narrow()
    {
        var expected = model();
        expected.ModifyColumn("name").Column.Type = "VARCHAR(200)";

        var writer = new StringWriter();
        new TableDelta(expected, catalog()).WriteRollback(new FirebirdMigrator(), writer);

        writer.ToString().ShouldContain("cannot narrow it back");
        FirebirdScript.Split(writer.ToString()).ShouldBeEmpty();
    }

    [Fact]
    public void the_rollback_of_a_create_drops_the_table()
    {
        var statements = rollback(new TableDelta(model(), null));

        statements.Last().ShouldContain("DROP TABLE people");
    }

    [Fact]
    public void restoring_a_created_table_has_no_previous_state()
    {
        var writer = new StringWriter();
        new TableDelta(model(), null).WriteRestorationOfPreviousState(new FirebirdMigrator(), writer);

        writer.ToString().ShouldBeEmpty();
    }

    [Fact]
    public void a_deferred_foreign_key_is_held_back_from_the_create()
    {
        var expected = model();
        expected.AddColumn<int>("state_id").ForeignKeyTo("states", "id");

        var delta = new TableDelta(expected, null);
        delta.ForeignKeysToCreate.Select(x => x.Name).ShouldBe(["fk_people_state_id"]);

        delta.DeferForeignKey("fk_people_state_id");
        delta.HasDeferredForeignKeys.ShouldBeTrue();

        var create = new StringWriter();
        delta.WriteCreateWithoutDeferredForeignKeys(new FirebirdMigrator(), create);
        create.ToString().ShouldNotContain("FOREIGN KEY");

        var deferred = new StringWriter();
        delta.WriteDeferredForeignKeys(new FirebirdMigrator(), deferred);
        deferred.ToString().ShouldContain("ADD CONSTRAINT fk_people_state_id");
    }

    [Fact]
    public void an_update_offers_only_its_missing_foreign_keys_for_deferral()
    {
        var expected = model();
        expected.AddColumn<int>("state_id").ForeignKeyTo("states", "id");

        var delta = new TableDelta(expected, catalog());
        delta.ForeignKeysToCreate.Select(x => x.Name).ShouldBe(["fk_people_state_id"]);

        delta.DeferForeignKey("fk_people_state_id");
        update(delta).ShouldAllBe(x => !x.Contains("FOREIGN KEY"));
    }

    [Fact]
    public void describes_itself()
    {
        new TableDelta(model(), null).ToString().ShouldBe("TableDelta for people");
    }
}
