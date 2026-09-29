using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

public class detecting_table_deltas: IntegrationContext
{
    private readonly Table theTable;

    public detecting_table_deltas()
    {
        theTable = new Table("people");
        theTable.AddColumn<int>("id").AsPrimaryKey();
        theTable.AddColumn<string>("first_name");
        theTable.AddColumn<string>("last_name");
        theTable.AddColumn<string>("user_name");
        theTable.AddColumn("data", "BLOB SUB_TYPE TEXT");
    }

    private async Task AssertNoDeltasAfterPatching(Table? table = null)
    {
        table ??= theTable;

        await table.ApplyChangesAsync(theConnection);

        var delta = await table.FindDeltaAsync(theConnection);
        if (delta.HasChanges())
        {
            var writer = new StringWriter();
            delta.WriteUpdate(new FirebirdMigrator(), writer);
            throw new Exception("Found these differences:\n\n" + writer);
        }
    }

    [Fact]
    public async Task detect_all_new_table()
    {
        (await theTable.FetchExistingAsync(theConnection)).ShouldBeNull();

        (await theTable.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public async Task no_delta()
    {
        await CreateSchemaObjectInDatabase(theTable);

        (await theTable.FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task missing_column()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.AddColumn<DateTime>("birth_day");
        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.Columns.Missing.Single().Name.ShouldBe("birth_day");
        delta.Columns.Extras.ShouldBeEmpty();
        delta.Columns.Different.ShouldBeEmpty();
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task extra_column()
    {
        theTable.AddColumn<DateTime>("birth_day");
        await CreateSchemaObjectInDatabase(theTable);

        theTable.RemoveColumn("birth_day");
        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.Columns.Extras.Single().Name.ShouldBe("BIRTH_DAY");
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task detect_new_index()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);
        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.Indexes.Missing.Single().Name.ShouldBe("idx_people_user_name");
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task detect_matched_index()
    {
        theTable.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);
        await CreateSchemaObjectInDatabase(theTable);

        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.Indexes.Matched.Single().Name.ShouldBe("idx_people_user_name");
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task detect_different_index()
    {
        theTable.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);
        await CreateSchemaObjectInDatabase(theTable);

        var index = theTable.Indexes.Single();
        index.SortOrder = SortOrder.Desc;
        index.IsUnique = false;

        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.Indexes.Different.Single().Expected.Name.ShouldBe("idx_people_user_name");
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task detect_extra_index()
    {
        theTable.ModifyColumn("user_name").AddIndex();
        await CreateSchemaObjectInDatabase(theTable);

        theTable.Indexes.Clear();
        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.Indexes.Extras.Single().Name.ShouldBe("IDX_PEOPLE_USER_NAME");

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task an_inactive_index_is_rebuilt()
    {
        theTable.ModifyColumn("user_name").AddIndex();
        await CreateSchemaObjectInDatabase(theTable);
        await ExecuteAsync("ALTER INDEX idx_people_user_name INACTIVE");

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Indexes.Different.Single().Actual.IsInactive.ShouldBeTrue();

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task detect_new_foreign_key()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(states);

        theTable.AddColumn<int>("state_id");
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("state_id").ForeignKeyTo(states, "id");
        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.ForeignKeys.Missing.Single().Name.ShouldBe("fk_people_state_id");

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task detect_extra_foreign_key()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(states);

        theTable.AddColumn<int>("state_id").ForeignKeyTo(states, "id");
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ForeignKeys.Clear();
        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.ForeignKeys.Extras.Single().Name.ShouldBe("FK_PEOPLE_STATE_ID");

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task detect_changed_foreign_key_actions()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(states);

        theTable.AddColumn<int>("state_id").ForeignKeyTo(states, "id");
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ForeignKeys.Single().DeleteAction = CascadeAction.Cascade;
        theTable.ForeignKeys.Single().UpdateAction = CascadeAction.SetNull;
        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.ForeignKeys.Different.Single().Expected.Name.ShouldBe("fk_people_state_id");

        await AssertNoDeltasAfterPatching();

        var existing = await theTable.FetchExistingAsync(theConnection);
        existing!.ForeignKeys.Single().DeleteAction.ShouldBe(CascadeAction.Cascade);
        existing.ForeignKeys.Single().UpdateAction.ShouldBe(CascadeAction.SetNull);
    }

    /// <summary>
    ///     The catalog records a key declared without an action as RESTRICT, and one declared NO ACTION
    ///     as that. Neither drifts against a model that says nothing, or NoAction, or Restrict.
    /// </summary>
    [Theory]
    [InlineData("")]
    [InlineData(" ON DELETE NO ACTION ON UPDATE NO ACTION")]
    public async Task restrict_and_no_action_read_back_as_no_change(string clause)
    {
        await ExecuteAsync($"""
            CREATE TABLE states (id INTEGER NOT NULL, CONSTRAINT pk_states PRIMARY KEY (id));
            CREATE TABLE people (id INTEGER NOT NULL, state_id INTEGER, CONSTRAINT pk_people PRIMARY KEY (id));
            ALTER TABLE people ADD CONSTRAINT fk_people_state_id FOREIGN KEY (state_id) REFERENCES states (id){clause};
            """);

        foreach (var action in new[] { CascadeAction.NoAction, CascadeAction.Restrict })
        {
            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<int>("state_id").ForeignKeyTo("states", "id", onDelete: action, onUpdate: action);

            (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        }
    }

    [Fact]
    public async Task a_composite_foreign_key_round_trips_in_column_order()
    {
        var parent = new Table("parents");
        parent.AddColumn<int>("b").AsPrimaryKey();
        parent.AddColumn<int>("a").AsPrimaryKey();
        parent.SetPrimaryKeyOrder(["b", "a"]);
        await CreateSchemaObjectInDatabase(parent);

        var child = new Table("children");
        child.AddColumn<int>("id").AsPrimaryKey();
        child.AddColumn<int>("pb");
        child.AddColumn<int>("pa");
        child.ForeignKeys.Add(new ForeignKey("fk_children_parents")
        {
            LinkedTable = parent.Identifier, ColumnNames = ["pb", "pa"], LinkedNames = ["b", "a"]
        });

        await AssertNoDeltasAfterPatching(child);

        var existing = await child.FetchExistingAsync(theConnection);
        existing!.ForeignKeys.Single().ColumnNames.ShouldBe(["PB", "PA"]);
        existing.ForeignKeys.Single().LinkedNames.ShouldBe(["B", "A"]);
    }

    [Fact]
    public async Task a_pinned_primary_key_order_is_compared_and_rebuilt()
    {
        var table = new Table("keyed");
        table.AddColumn<int>("a").AsPrimaryKey();
        table.AddColumn<int>("b").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(table);

        table.SetPrimaryKeyOrder(["b", "a"]);
        (await table.FindDeltaAsync(theConnection)).PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(table);
        (await table.FetchExistingAsync(theConnection))!.PrimaryKeyColumns.ShouldBe(["B", "A"]);
    }

    [Fact]
    public async Task a_primary_key_is_added_to_a_table_without_one()
    {
        await ExecuteAsync("CREATE TABLE keyless (id INTEGER NOT NULL, name VARCHAR(20))");

        var table = new Table("keyless");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("name", "VARCHAR(20)");

        (await table.FindDeltaAsync(theConnection)).PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Create);

        await AssertNoDeltasAfterPatching(table);
    }

    [Fact]
    public async Task a_primary_key_the_model_no_longer_has_is_dropped()
    {
        var table = new Table("keyed");
        table.AddColumn<int>("id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(table);

        var keyless = new Table("keyed");
        keyless.AddColumn<int>("id").NotNull();

        await AssertNoDeltasAfterPatching(keyless);
        (await keyless.FetchExistingAsync(theConnection))!.PrimaryKeyColumns.ShouldBeEmpty();
    }

    /// <summary>
    ///     A4: Firebird refuses to change the type of a key column, widening included (335544538), so the
    ///     delta is Invalid and says why, and a migration short of AutoCreate.All refuses before anything
    ///     runs.
    /// </summary>
    [Fact]
    public async Task widening_a_key_column_is_refused_up_front()
    {
        await CreateSchemaObjectInDatabase(theTable);

        var widened = new Table("people");
        widened.AddColumn<long>("id").AsPrimaryKey();
        widened.AddColumn<string>("first_name");
        widened.AddColumn<string>("last_name");
        widened.AddColumn<string>("user_name");
        widened.AddColumn("data", "BLOB SUB_TYPE TEXT");
        widened.AddColumn<int>("added");

        var delta = await widened.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.InvalidReason!.ShouldContain("column 'id'");

        var ex = await Should.ThrowAsync<SchemaMigrationException>(() => ApplyAsync(AutoCreate.CreateOrUpdate, widened));
        ex.Message.ShouldContain("primary key, unique constraint, unique index or foreign key");

        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = 'PEOPLE' AND RDB$FIELD_NAME = 'ADDED'"))
            .ShouldBe(0, "nothing runs when the migration is refused");
    }

    /// <summary>
    ///     The measurement behind A4: this is the error the delta saves the migration from.
    /// </summary>
    [Fact]
    public async Task firebird_really_refuses_to_widen_a_key_column()
    {
        await CreateSchemaObjectInDatabase(theTable);

        var ex = await Should.ThrowAsync<FirebirdSql.Data.FirebirdClient.FbException>(() =>
            ExecuteAsync("ALTER TABLE people ALTER id TYPE BIGINT"));

        FirebirdMigrator.HasErrorNumber(ex, 335544538).ShouldBeTrue();
    }

    /// <summary>
    ///     A unique constraint created outside Weasel covers its columns as a key does: Firebird will not
    ///     retype them either, so the delta refuses before anything runs rather than halfway through.
    /// </summary>
    [Fact]
    public async Task widening_a_column_a_unique_constraint_covers_is_refused_up_front()
    {
        await ExecuteAsync("""
            CREATE TABLE people (id INTEGER NOT NULL, name VARCHAR(20),
                CONSTRAINT pk_people PRIMARY KEY (id), CONSTRAINT uq_people_name UNIQUE (name))
            """);

        await Should.ThrowAsync<FirebirdSql.Data.FirebirdClient.FbException>(() =>
            ExecuteAsync("ALTER TABLE people ALTER name TYPE VARCHAR(40)"));

        var widened = new Table("people");
        widened.AddColumn<int>("id").AsPrimaryKey();
        widened.AddColumn("name", "VARCHAR(40)");
        widened.AddColumn<int>("added");

        (await widened.FetchExistingAsync(theConnection))!.UniqueConstraintColumns.ShouldBe(["NAME"]);

        var delta = await widened.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.InvalidReason!.ShouldContain("column 'name'");

        await Should.ThrowAsync<SchemaMigrationException>(() => ApplyAsync(AutoCreate.CreateOrUpdate, widened));
        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = 'PEOPLE' AND RDB$FIELD_NAME = 'ADDED'"))
            .ShouldBe(0, "nothing runs when the migration is refused");
    }

    /// <summary>
    ///     The unique constraint's own index is still not an index of the table's, and its columns are
    ///     not a key: a model that does not retype them sees no change.
    /// </summary>
    [Fact]
    public async Task a_unique_constraint_created_outside_weasel_is_no_change()
    {
        await ExecuteAsync("""
            CREATE TABLE people (id INTEGER NOT NULL, name VARCHAR(20),
                CONSTRAINT pk_people PRIMARY KEY (id), CONSTRAINT uq_people_name UNIQUE (name))
            """);

        var same = new Table("people");
        same.AddColumn<int>("id").AsPrimaryKey();
        same.AddColumn("name", "VARCHAR(20)");

        var existing = (await same.FetchExistingAsync(theConnection))!;
        existing.PrimaryKeyColumns.ShouldBe(["ID"]);
        existing.PrimaryKeyName.ShouldBe("PK_PEOPLE");

        (await same.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task under_all_an_unalterable_table_is_dropped_and_recreated()
    {
        await CreateSchemaObjectInDatabase(theTable);

        var narrowed = new Table("people");
        narrowed.AddColumn<int>("id").AsPrimaryKey();
        narrowed.AddColumn("first_name", "VARCHAR(10)");

        await ApplyAsync(AutoCreate.All, narrowed);

        (await narrowed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_table_among_several_is_found_through_the_migration_path()
    {
        var other = new Table("others");
        other.AddColumn<int>("id").AsPrimaryKey();

        await ApplyAsync(theTable, other);

        theTable.AddColumn<int>("age");
        var migration = await DetermineAsync(other, theTable);

        migration.Deltas.Select(x => x.Difference).ShouldBe([SchemaPatchDifference.None, SchemaPatchDifference.Update]);
    }
}
