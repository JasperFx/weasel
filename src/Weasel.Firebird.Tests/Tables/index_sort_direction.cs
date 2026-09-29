using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     A Firebird index is ascending or descending as a whole -- a per-column direction is a syntax
///     error -- so the shared model's two ways of saying "descending" both mean the whole index, and a
///     mix is refused before anything runs. Partial indexes are Firebird 5's alone.
/// </summary>
public class index_sort_direction: IntegrationContext
{
    private static Table triggers(Action<IndexDefinition>? configure = null)
    {
        var table = new Table("qrtz_triggers");
        table.AddColumn("sched_name", "VARCHAR(120)").NotNull().AsPrimaryKey();
        table.AddColumn("trigger_name", "VARCHAR(150)").NotNull().AsPrimaryKey();
        table.AddColumn<long>("next_fire_time");
        table.AddColumn<int>("priority");
        table.AddColumn("trigger_state", "VARCHAR(16)").NotNull();

        var index = new IndexDefinition("idx_qrtz_t_nft_st")
        {
            Columns = ["sched_name", "trigger_state", "next_fire_time"]
        };
        configure?.Invoke(index);
        table.Indexes.Add(index);

        return table;
    }

    [Fact]
    public async Task an_ascending_index_round_trips()
    {
        var table = triggers();
        await ApplyAsync(table);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_descending_index_round_trips()
    {
        var table = triggers(x => x.SortOrder = SortOrder.Desc);
        await ApplyAsync(table);

        (await ScalarAsync<int>("SELECT RDB$INDEX_TYPE FROM RDB$INDICES WHERE RDB$INDEX_NAME = 'IDX_QRTZ_T_NFT_ST'"))
            .ShouldBe(1);
        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task every_column_descending_is_a_descending_index()
    {
        var table = triggers(x =>
        {
            foreach (var column in x.Columns) x.DescendingColumns.Add(column);
        });
        await ApplyAsync(table);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await triggers(x => x.SortOrder = SortOrder.Desc).FindDeltaAsync(theConnection)).Difference
            .ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_direction_is_detected_and_converges()
    {
        await ApplyAsync(triggers());

        var descending = triggers(x => x.SortOrder = SortOrder.Desc);
        (await descending.FindDeltaAsync(theConnection)).Indexes.Different.Count.ShouldBe(1);

        await ApplyAsync(descending);
        (await descending.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Quartz's acquisition query orders NEXT_FIRE_TIME ASC, PRIORITY DESC, which a Firebird index
    ///     cannot say. The model is refused while the migration is rendered, and nothing is created.
    /// </summary>
    [Fact]
    public async Task mixed_directions_are_refused_before_anything_runs()
    {
        var table = triggers(x =>
        {
            x.Columns = ["sched_name", "trigger_state", "next_fire_time", "priority"];
            x.DescendingColumns.Add("priority");
        });

        await Should.ThrowAsync<InvalidOperationException>(() => ApplyAsync(table));

        (await table.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    [Fact]
    public async Task an_expression_index_round_trips()
    {
        var table = triggers();
        table.Indexes.Add(new IndexDefinition("idx_upper_state") { Expression = "UPPER(trigger_state)" });
        await ApplyAsync(table);

        var existing = await table.FetchExistingAsync(theConnection);
        existing!.IndexFor("IDX_UPPER_STATE")!.Expression.ShouldBe("UPPER(trigger_state)");

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_descending_expression_index_round_trips()
    {
        var table = triggers();
        table.Indexes.Add(new IndexDefinition("idx_upper_state")
        {
            Expression = "UPPER(trigger_state)", SortOrder = SortOrder.Desc
        });
        await ApplyAsync(table);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_partial_index_round_trips_on_firebird_5()
    {
        if (!ServerVersion.SupportsPartialIndexes)
        {
            Assert.Skip($"Partial indexes need Firebird 5; this server is Firebird {ServerVersion}");
        }

        var table = triggers(x => x.Predicate = "next_fire_time > 0 AND trigger_state <> 'PAUSED'");
        await ApplyAsync(table);

        var existing = await table.FetchExistingAsync(theConnection);
        existing!.Indexes.Single().Predicate.ShouldBe("next_fire_time > 0 AND trigger_state <> 'PAUSED'");
        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        var changed = triggers(x => x.Predicate = "next_fire_time > 1");
        await ApplyAsync(changed);
        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     On Firebird 3 and 4 the WHERE is a syntax error, which would stop a migration halfway. The
    ///     migrator refuses it before the first statement instead.
    /// </summary>
    [Fact]
    public async Task a_partial_index_is_refused_before_anything_runs_on_firebird_3_and_4()
    {
        if (ServerVersion.SupportsPartialIndexes)
        {
            Assert.Skip($"This server is Firebird {ServerVersion}, which has partial indexes");
        }

        var table = triggers(x => x.Predicate = "next_fire_time > 0");

        var ex = await Should.ThrowAsync<NotSupportedException>(() => ApplyAsync(table));
        ex.Message.ShouldContain("Firebird 5");
        ex.Message.ShouldContain("idx_qrtz_t_nft_st");

        (await table.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    [Fact]
    public void the_partial_index_refusal_only_applies_before_firebird_5()
    {
        var migration = new SchemaMigration(new TableDelta(triggers(x => x.Predicate = "priority > 0"), null));

        FirebirdMigrator.AssertServerSupports(migration, new FirebirdServerVersion(5, 0, 0));
        FirebirdMigrator.AssertServerSupports(migration, null);
        Should.Throw<NotSupportedException>(() =>
            FirebirdMigrator.AssertServerSupports(migration, new FirebirdServerVersion(4, 0, 0)));
    }

    /// <summary>
    ///     Oracle's test of the same name carries a Quartz model, and so does this one: Quartz's
    ///     acquisition index on Firebird is the three-column ascending shape its script creates.
    /// </summary>
    [Fact]
    public async Task quartzs_acquisition_index_created_by_its_script_reads_back_as_no_change()
    {
        await ExecuteAsync("""
            CREATE TABLE QRTZ_TRIGGERS (
                SCHED_NAME VARCHAR(120) NOT NULL,
                TRIGGER_NAME VARCHAR(150) NOT NULL,
                NEXT_FIRE_TIME BIGINT DEFAULT NULL,
                PRIORITY INTEGER DEFAULT NULL,
                TRIGGER_STATE VARCHAR(16) NOT NULL,
                CONSTRAINT PK_QRTZ_TRIGGERS PRIMARY KEY (SCHED_NAME, TRIGGER_NAME)
            );
            CREATE INDEX IDX_QRTZ_T_NFT_ST ON QRTZ_TRIGGERS(SCHED_NAME,TRIGGER_STATE,NEXT_FIRE_TIME);
            """);

        var model = new Table("QRTZ_TRIGGERS");
        model.AddColumn("SCHED_NAME", "VARCHAR(120)").AsPrimaryKey();
        model.AddColumn("TRIGGER_NAME", "VARCHAR(150)").AsPrimaryKey();
        model.AddColumn("NEXT_FIRE_TIME", "BIGINT").DefaultValueByExpression("NULL");
        model.AddColumn("PRIORITY", "INTEGER").DefaultValueByExpression("NULL");
        model.AddColumn("TRIGGER_STATE", "VARCHAR(16)").NotNull();
        model.PrimaryKeyName = "PK_QRTZ_TRIGGERS";
        model.Indexes.Add(new IndexDefinition("IDX_QRTZ_T_NFT_ST")
        {
            Columns = ["SCHED_NAME", "TRIGGER_STATE", "NEXT_FIRE_TIME"]
        });

        (await model.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
