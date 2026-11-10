using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

public class foreign_key_state: IntegrationContext
{
    public foreign_key_state(): base("fk_state")
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await ResetSchema();
        await theConnection.CreateCommand("create table fk_state.parent (id int not null primary key)").ExecuteNonQueryAsync();
        await theConnection.CreateCommand("insert into fk_state.parent (id) values (1)").ExecuteNonQueryAsync();
    }

    private const string ChildTable =
        "create table fk_state.child (id int not null constraint pk_child primary key, plain int null, untrusted int null, nfr int null, inactive int null)";

    private async Task createChildWithEveryState()
    {
        await theConnection.CreateCommand(ChildTable).ExecuteNonQueryAsync();
        await theConnection.CreateCommand("""
            alter table fk_state.child add constraint fk_plain foreign key (plain) references fk_state.parent (id);
            alter table fk_state.child with nocheck add constraint fk_untrusted foreign key (untrusted) references fk_state.parent (id);
            alter table fk_state.child add constraint fk_nfr foreign key (nfr) references fk_state.parent (id) not for replication;
            alter table fk_state.child add constraint fk_inactive foreign key (inactive) references fk_state.parent (id);
            alter table fk_state.child nocheck constraint fk_inactive;
            """).ExecuteNonQueryAsync();
    }

    private async Task createChildWithPlainKey(string column)
    {
        await theConnection.CreateCommand(ChildTable).ExecuteNonQueryAsync();
        await theConnection.CreateCommand(
                $"alter table fk_state.child add constraint fk_{column} foreign key ({column}) references fk_state.parent (id)")
            .ExecuteNonQueryAsync();
    }

    private static Table childModel(params ForeignKey[] foreignKeys)
    {
        var table = new Table("fk_state.child");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.PrimaryKeyName = "pk_child";
        foreach (var column in new[] { "plain", "untrusted", "nfr", "inactive" }) table.AddColumn<int>(column).AllowNulls();
        foreach (var foreignKey in foreignKeys) table.ForeignKeys.Add(foreignKey);
        return table;
    }

    private static ForeignKey key(string column, bool? disabled = null, bool? notForReplication = null, bool? notTrusted = null)
        => new($"fk_{column}")
        {
            ColumnNames = [column],
            LinkedNames = ["id"],
            LinkedTable = new SqlServerObjectName("fk_state", "parent"),
            IsDisabled = disabled,
            IsNotForReplication = notForReplication,
            IsNotTrusted = notTrusted
        };

    private static Table everyKeyUnpinned() => childModel(key("plain"), key("untrusted"), key("nfr"), key("inactive"));

    private async Task<string[]> catalogState()
    {
        var states = new List<string>();
        await using var reader = await theConnection.CreateCommand("""
            select name, is_disabled, is_not_for_replication, is_not_trusted
            from sys.foreign_keys
            where parent_object_id = object_id('fk_state.child')
            order by name
            """).ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            states.Add($"{reader.GetString(0)} {reader.GetBoolean(1)} {reader.GetBoolean(2)} {reader.GetBoolean(3)}");
        }

        return states.ToArray();
    }

    private async Task apply(Table table)
    {
        var migration = new SchemaMigration(await table.FindDeltaAsync(theConnection));
        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);
    }

    [Fact]
    public async Task a_table_read_back_recreates_the_state_of_its_foreign_keys()
    {
        await createChildWithEveryState();
        var before = await catalogState();
        var existing = (await new Table("fk_state.child").FetchExistingAsync(theConnection))!;

        await theConnection.CreateCommand("drop table fk_state.child").ExecuteNonQueryAsync();
        var writer = new StringWriter();
        existing.WriteCreateStatement(new SqlServerMigrator(), writer);
        await theConnection.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();

        (await catalogState()).ShouldBe(before);
    }

    [Fact]
    public async Task the_state_of_each_foreign_key_is_read_back()
    {
        await createChildWithEveryState();

        var keys = (await new Table("fk_state.child").FetchExistingAsync(theConnection))!
            .ForeignKeys.ToDictionary(x => x.Name);

        (keys["fk_plain"].IsDisabled, keys["fk_plain"].IsNotForReplication, keys["fk_plain"].IsNotTrusted)
            .ShouldBe((false, false, false));
        (keys["fk_untrusted"].IsDisabled, keys["fk_untrusted"].IsNotForReplication, keys["fk_untrusted"].IsNotTrusted)
            .ShouldBe((false, false, true));
        (keys["fk_nfr"].IsDisabled, keys["fk_nfr"].IsNotForReplication, keys["fk_nfr"].IsNotTrusted)
            .ShouldBe((false, true, true));
        (keys["fk_inactive"].IsDisabled, keys["fk_inactive"].IsNotForReplication, keys["fk_inactive"].IsNotTrusted)
            .ShouldBe((true, false, true));
    }

    [Fact]
    public void an_unpinned_key_renders_as_before()
    {
        key("plain").ToDDL(childModel()).ShouldBe(
            "IF OBJECT_ID(N'fk_state.fk_plain', N'F') IS NULL\nALTER TABLE fk_state.child\nADD CONSTRAINT fk_plain FOREIGN KEY(plain)\n REFERENCES fk_state.parent(id);\n"
                .ReplaceLineEndings());
    }

    [Fact]
    public void a_pinned_key_renders_its_state()
    {
        var ddl = key("plain", disabled: true, notForReplication: true, notTrusted: true).ToDDL(childModel());

        ddl.ShouldContain("ALTER TABLE fk_state.child WITH NOCHECK");
        ddl.ShouldContain("REFERENCES fk_state.parent(id) NOT FOR REPLICATION;");
        ddl.ShouldContain("ALTER TABLE fk_state.child NOCHECK CONSTRAINT fk_plain;");
    }

    [Fact]
    public async Task an_unpinned_model_is_unchanged_against_every_state()
    {
        await createChildWithEveryState();

        (await everyKeyUnpinned().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_unpinned_model_weasel_created_is_unchanged()
    {
        await apply(everyKeyUnpinned());

        (await everyKeyUnpinned().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_pinned_model_matching_the_database_is_unchanged()
    {
        await createChildWithEveryState();

        var pinned = childModel(
            key("plain", false, false, false),
            key("untrusted", false, false, true),
            key("nfr", false, true, true),
            key("inactive", true, false, true));

        (await pinned.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    public static TheoryData<string, bool?, bool?, bool?, string> PinnedChanges => new()
    {
        { "plain", true, null, null, "fk_plain True False True" },
        { "plain", null, true, null, "fk_plain False True True" },
        { "plain", null, null, true, "fk_plain False False True" }
    };

    [Theory]
    [MemberData(nameof(PinnedChanges))]
    public async Task a_pinned_state_the_database_lacks_is_drift_that_converges(
        string column, bool? disabled, bool? notForReplication, bool? notTrusted, string expectedState)
    {
        await createChildWithPlainKey(column);
        var pinned = childModel(key(column, disabled, notForReplication, notTrusted));

        (await pinned.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await apply(pinned);

        (await catalogState()).ShouldBe([expectedState]);
        (await pinned.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task pinning_a_disabled_key_enabled_and_trusted_converges()
    {
        await createChildWithEveryState();
        var pinned = everyKeyUnpinned();
        pinned.ForeignKeys.Remove(pinned.ForeignKeys.Single(x => x.Name == "fk_inactive"));
        pinned.ForeignKeys.Add(key("inactive", disabled: false, notTrusted: false));

        (await pinned.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await apply(pinned);

        (await catalogState()).ShouldContain("fk_inactive False False False");
        (await pinned.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    public static TheoryData<bool?, bool?> UnreachablePins => new()
    {
        { true, null },
        { null, true },
        { true, true }
    };

    [Theory]
    [MemberData(nameof(UnreachablePins))]
    public void a_trusted_pin_on_a_key_sql_server_keeps_untrusted_is_rejected_when_rendering(bool? disabled, bool? notForReplication)
    {
        var ex = Should.Throw<InvalidOperationException>(
            () => key("plain", disabled, notForReplication, notTrusted: false).ToDDL(childModel()));

        ex.Message.ShouldContain("fk_plain");
        ex.Message.ShouldContain("IsNotTrusted");
    }

    [Theory]
    [MemberData(nameof(UnreachablePins))]
    public async Task a_trusted_pin_on_a_key_sql_server_keeps_untrusted_is_rejected_when_comparing(bool? disabled, bool? notForReplication)
    {
        await createChildWithPlainKey("plain");

        await Should.ThrowAsync<InvalidOperationException>(
            () => childModel(key("plain", disabled, notForReplication, notTrusted: false)).FindDeltaAsync(theConnection));
    }

    [Theory]
    [InlineData(null, true, null)]
    [InlineData(null, true, true)]
    [InlineData(true, null, true)]
    [InlineData(false, false, false)]
    [InlineData(null, null, false)]
    public void a_reachable_pin_renders(bool? disabled, bool? notForReplication, bool? notTrusted)
    {
        Should.NotThrow(() => key("plain", disabled, notForReplication, notTrusted).ToDDL(childModel()));
    }
}
