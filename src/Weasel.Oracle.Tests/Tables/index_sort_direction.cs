using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Tables;

/// <summary>
///     Oracle sets an index key's direction per column, and Weasel could only say "descending" for the
///     whole index: <see cref="IndexDefinition.SortOrder" /> appends one trailing <c>DESC</c>, which
///     lands on the last key column. The reader collapsed the other way -- any descending column made
///     the whole index read back as <see cref="SortOrder.Desc" /> -- so an index such as
///     <c>(a, b, c DESC, d)</c> could not be declared at all, and the nearest declaration compared
///     equal to indexes that are not the same.
/// </summary>
/// <remarks>
///     Oracle records a descending key column as a hidden virtual column: <c>ALL_IND_COLUMNS</c> reports a
///     <c>SYS_NC…$</c> name with <c>DESCEND = 'DESC'</c>, <c>ALL_IND_EXPRESSIONS</c> holds the real column
///     as a bare quoted identifier, and <c>ALL_INDEXES</c> calls the index <c>FUNCTION-BASED NORMAL</c>.
/// </remarks>
public class index_sort_direction: IntegrationContext
{
    private const string SchemaName = "idxdir";

    public index_sort_direction(): base(SchemaName)
    {
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static Table TriggersTable(string name)
    {
        var table = new Table($"{SchemaName}.{name}");
        table.AddColumn("sched_name", "VARCHAR2(120)").AsPrimaryKey();
        table.AddColumn("trigger_name", "VARCHAR2(200)").AsPrimaryKey();
        table.AddColumn("trigger_state", "VARCHAR2(16)").NotNull();
        table.AddColumn("next_fire_time", "NUMBER(19)");
        table.AddColumn("priority", "NUMBER(13)");
        table.AddColumn("misfire_instr", "NUMBER(2)");
        return table;
    }

    private async Task ExecuteAsync(string sql)
        => await theConnection.CreateCommand(sql).ExecuteNonQueryAsync(Ct);

    private async Task<string[]> DescendingColumnsInCatalogAsync(string index)
    {
        await using var cmd = theConnection.CreateCommand(
            "SELECT e.column_expression FROM all_ind_columns c JOIN all_ind_expressions e "
            + "ON e.index_owner = c.index_owner AND e.index_name = c.index_name AND e.column_position = c.column_position "
            + "WHERE c.index_owner = 'IDXDIR' AND c.index_name = :idx_name AND c.descend = 'DESC' ORDER BY c.column_position");
        cmd.InitialLONGFetchSize = -1;
        cmd.Parameters.Add(new global::Oracle.ManagedDataAccess.Client.OracleParameter("idx_name", index.ToUpperInvariant()));

        var columns = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct))
        {
            columns.Add(reader.GetString(0).Trim('"'));
        }

        return columns.ToArray();
    }

    [Fact]
    public async Task the_direction_is_read_back_per_column()
    {
        await ResetSchema();

        var table = TriggersTable("read_back");
        await CreateSchemaObjectInDatabase(table);
        await ExecuteAsync(
            $"CREATE INDEX {SchemaName}.idx_read_back ON {SchemaName}.read_back (trigger_name DESC, next_fire_time, priority DESC)");

        var existing = await table.FetchExistingAsync(theConnection, Ct);
        var index = existing!.Indexes.Single();

        index.Columns.ShouldBe(["TRIGGER_NAME", "NEXT_FIRE_TIME", "PRIORITY"]);
        index.DescendingColumns.OrderBy(x => x).ShouldBe(["PRIORITY", "TRIGGER_NAME"]);
        index.ToDDL(existing).ShouldContain("(TRIGGER_NAME DESC, NEXT_FIRE_TIME, PRIORITY DESC)");

        // FUNCTION-BASED NORMAL only because of the descending keys: it is an ordinary B-tree.
        index.IndexType.ShouldBe(OracleIndexType.BTree);
    }

    [Fact]
    public async Task a_mixed_direction_index_is_created_as_declared_and_settles()
    {
        await ResetSchema();

        var table = TriggersTable("mixed");
        var index = new IndexDefinition("idx_mixed")
        {
            Columns = ["sched_name", "trigger_state", "next_fire_time", "priority", "misfire_instr"]
        };
        index.DescendingColumns.Add("priority");
        table.Indexes.Add(index);

        await table.ApplyChangesAsync(theConnection, Ct);

        (await DescendingColumnsInCatalogAsync("idx_mixed")).ShouldBe(["PRIORITY"]);
        (await table.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The same index, read back by the migration path rather than by <see cref="Table.FetchExistingAsync" />.
    ///     A migration batches the table's introspection queries and ODP.NET runs one statement per command,
    ///     so the batch is split — and a split command that does not carry the table's
    ///     <c>InitialLONGFetchSize</c> reads <c>ALL_IND_EXPRESSIONS.COLUMN_EXPRESSION</c>, a LONG, back
    ///     empty. The descending key then reads back as its hidden <c>SYS_NC…$</c> column, and every apply
    ///     drops and recreates the index.
    /// </summary>
    [Fact]
    public async Task a_descending_column_settles_on_the_migration_path()
    {
        await ResetSchema();

        var table = TriggersTable("migrated");
        var index = new IndexDefinition("idx_migrated")
        {
            Columns = ["sched_name", "trigger_state", "next_fire_time", "priority", "misfire_instr"]
        };
        index.DescendingColumns.Add("priority");
        table.Indexes.Add(index);

        (await table.MigrateAsync(theConnection, Ct)).ShouldBeTrue();

        var migration = await SchemaMigration.DetermineAsync(theConnection, new OracleMigrator(), Ct, table);

        var delta = migration.Deltas.Single().ShouldBeOfType<TableDelta>();
        delta.Actual!.Indexes.Single().Columns
            .ShouldBe(["SCHED_NAME", "TRIGGER_STATE", "NEXT_FIRE_TIME", "PRIORITY", "MISFIRE_INSTR"]);
        migration.Difference.ShouldBe(SchemaPatchDifference.None);

        (await table.MigrateAsync(theConnection, Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task changing_the_direction_rebuilds_the_index_as_declared()
    {
        await ResetSchema();

        var table = TriggersTable("changed");
        await CreateSchemaObjectInDatabase(table);
        await ExecuteAsync(
            $"CREATE INDEX {SchemaName}.idx_changed ON {SchemaName}.changed (next_fire_time DESC, priority)");

        var index = new IndexDefinition("idx_changed") { Columns = ["next_fire_time", "priority"] };
        index.DescendingColumns.Add("priority");
        table.Indexes.Add(index);

        (await table.FindDeltaAsync(theConnection, Ct)).Indexes.Different.Count().ShouldBe(1);

        await table.ApplyChangesAsync(theConnection, Ct);

        (await DescendingColumnsInCatalogAsync("idx_changed")).ShouldBe(["PRIORITY"]);
        (await table.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_whole_index_sort_order_still_creates_and_matches_a_trailing_desc()
    {
        // What SortOrder.Desc has always meant on Oracle, pinned so existing databases keep matching:
        // one trailing DESC, on the last key column.
        await ResetSchema();

        var table = TriggersTable("trailing");
        table.Indexes.Add(new IndexDefinition("idx_trailing")
        {
            Columns = ["next_fire_time", "priority"], SortOrder = SortOrder.Desc
        });

        await table.ApplyChangesAsync(theConnection, Ct);

        (await DescendingColumnsInCatalogAsync("idx_trailing")).ShouldBe(["PRIORITY"]);
        (await table.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_whole_index_sort_order_no_longer_matches_a_different_descending_column()
    {
        // The reader used to turn any descending column into SortOrder.Desc, which renders as a
        // trailing DESC -- so (next_fire_time DESC, priority) compared equal to a model that creates
        // (next_fire_time, priority DESC).
        await ResetSchema();

        var table = TriggersTable("first_desc");
        await CreateSchemaObjectInDatabase(table);
        await ExecuteAsync(
            $"CREATE INDEX {SchemaName}.idx_first_desc ON {SchemaName}.first_desc (next_fire_time DESC, priority)");

        table.Indexes.Add(new IndexDefinition("idx_first_desc")
        {
            Columns = ["next_fire_time", "priority"], SortOrder = SortOrder.Desc
        });

        (await table.FindDeltaAsync(theConnection, Ct)).Indexes.Different.Count().ShouldBe(1);
    }

    [Fact]
    public async Task a_descending_column_in_the_database_is_drift_from_an_ascending_model()
    {
        await ResetSchema();

        var table = TriggersTable("db_desc");
        await CreateSchemaObjectInDatabase(table);
        await ExecuteAsync(
            $"CREATE INDEX {SchemaName}.idx_db_desc ON {SchemaName}.db_desc (next_fire_time, priority DESC, misfire_instr)");

        table.Indexes.Add(new IndexDefinition("idx_db_desc")
        {
            Columns = ["next_fire_time", "priority", "misfire_instr"]
        });

        (await table.FindDeltaAsync(theConnection, Ct)).Indexes.Different.Count().ShouldBe(1);
    }

    /// <summary>
    ///     Quartz.NET's job store creates its acquisition index with one descending key in the middle.
    ///     The two tables and the index below are its <c>create_oracle.sql</c> statements verbatim, with
    ///     the table prefix substituted and the index name qualified into the test schema.
    /// </summary>
    [Fact]
    public async Task a_model_of_quartz_triggers_converges_on_the_schema_quartz_creates()
    {
        await ResetSchema();

        await ExecuteAsync(
            "CREATE TABLE idxdir.QRTZ_JOB_DETAILS (SCHED_NAME VARCHAR2(120) NOT NULL, JOB_NAME VARCHAR2(200) NOT NULL, JOB_GROUP VARCHAR2(200) NOT NULL, DESCRIPTION VARCHAR2(250) NULL, JOB_CLASS_NAME VARCHAR2(250) NOT NULL, IS_DURABLE VARCHAR2(1) NOT NULL, IS_NONCONCURRENT VARCHAR2(1) NOT NULL, IS_UPDATE_DATA VARCHAR2(1) NOT NULL, REQUESTS_RECOVERY VARCHAR2(1) NOT NULL, JOB_DATA BLOB NULL, CONSTRAINT QRTZ_JOB_DETAILS_PK PRIMARY KEY (SCHED_NAME,JOB_NAME,JOB_GROUP))");
        await ExecuteAsync(
            "CREATE TABLE idxdir.QRTZ_TRIGGERS (SCHED_NAME VARCHAR2(120) NOT NULL, TRIGGER_NAME VARCHAR2(200) NOT NULL, TRIGGER_GROUP VARCHAR2(200) NOT NULL, JOB_NAME VARCHAR2(200) NOT NULL, JOB_GROUP VARCHAR2(200) NOT NULL, DESCRIPTION VARCHAR2(250) NULL, NEXT_FIRE_TIME NUMBER(19) NULL, PREV_FIRE_TIME NUMBER(19) NULL, PRIORITY NUMBER(13) NULL, TRIGGER_STATE VARCHAR2(16) NOT NULL, TRIGGER_TYPE VARCHAR2(8) NOT NULL, START_TIME NUMBER(19) NOT NULL, END_TIME NUMBER(19) NULL, CALENDAR_NAME VARCHAR2(200) NULL, MISFIRE_INSTR NUMBER(2) NULL, MISFIRE_ORIG_FIRE_TIME NUMBER(19) NULL, EXECUTION_GROUP VARCHAR2(200) NULL, PREFERRED_NODE VARCHAR2(200) NULL, PREFERRED_NODE_AUTO VARCHAR2(1) DEFAULT '0' NOT NULL, RETRY_POLICY VARCHAR2(250) NULL, RETRY_ATTEMPT NUMBER(13) NULL, CONTINUES_TRIGGER_NAME VARCHAR2(200) NULL, CONTINUES_TRIGGER_GROUP VARCHAR2(200) NULL, CONTINUATION_CONDITION NUMBER(13) NULL, OVERLAP_POLICY NUMBER(13) NULL, PAUSE_REASON VARCHAR2(1000) NULL, PAUSED_BY VARCHAR2(800) NULL, PAUSED_AT NUMBER(19) NULL, JOB_DATA BLOB NULL, CONSTRAINT QRTZ_TRIGGERS_PK PRIMARY KEY (SCHED_NAME,TRIGGER_NAME,TRIGGER_GROUP), CONSTRAINT QRTZ_TRIGGER_TO_JOBS_FK FOREIGN KEY (SCHED_NAME,JOB_NAME,JOB_GROUP) REFERENCES idxdir.QRTZ_JOB_DETAILS (SCHED_NAME,JOB_NAME,JOB_GROUP))");
        await ExecuteAsync(
            "CREATE INDEX idxdir.IDX_QRTZ_T_NFT_ST ON idxdir.QRTZ_TRIGGERS(SCHED_NAME,TRIGGER_STATE,NEXT_FIRE_TIME ASC,PRIORITY DESC,MISFIRE_INSTR)");

        var triggers = new Table($"{SchemaName}.QRTZ_TRIGGERS");
        triggers.AddColumn("SCHED_NAME", "VARCHAR2(120)").AsPrimaryKey();
        triggers.AddColumn("TRIGGER_NAME", "VARCHAR2(200)").AsPrimaryKey();
        triggers.AddColumn("TRIGGER_GROUP", "VARCHAR2(200)").AsPrimaryKey();
        triggers.AddColumn("JOB_NAME", "VARCHAR2(200)").NotNull();
        triggers.AddColumn("JOB_GROUP", "VARCHAR2(200)").NotNull();
        triggers.AddColumn("DESCRIPTION", "VARCHAR2(250)");
        triggers.AddColumn("NEXT_FIRE_TIME", "NUMBER(19)");
        triggers.AddColumn("PREV_FIRE_TIME", "NUMBER(19)");
        triggers.AddColumn("PRIORITY", "NUMBER(13)");
        triggers.AddColumn("TRIGGER_STATE", "VARCHAR2(16)").NotNull();
        triggers.AddColumn("TRIGGER_TYPE", "VARCHAR2(8)").NotNull();
        triggers.AddColumn("START_TIME", "NUMBER(19)").NotNull();
        triggers.AddColumn("END_TIME", "NUMBER(19)");
        triggers.AddColumn("CALENDAR_NAME", "VARCHAR2(200)");
        triggers.AddColumn("MISFIRE_INSTR", "NUMBER(2)");
        triggers.AddColumn("MISFIRE_ORIG_FIRE_TIME", "NUMBER(19)");
        triggers.AddColumn("EXECUTION_GROUP", "VARCHAR2(200)");
        triggers.AddColumn("PREFERRED_NODE", "VARCHAR2(200)");
        triggers.AddColumn("PREFERRED_NODE_AUTO", "VARCHAR2(1)").DefaultValueByString("0").NotNull();
        triggers.AddColumn("RETRY_POLICY", "VARCHAR2(250)");
        triggers.AddColumn("RETRY_ATTEMPT", "NUMBER(13)");
        triggers.AddColumn("CONTINUES_TRIGGER_NAME", "VARCHAR2(200)");
        triggers.AddColumn("CONTINUES_TRIGGER_GROUP", "VARCHAR2(200)");
        triggers.AddColumn("CONTINUATION_CONDITION", "NUMBER(13)");
        triggers.AddColumn("OVERLAP_POLICY", "NUMBER(13)");
        triggers.AddColumn("PAUSE_REASON", "VARCHAR2(1000)");
        triggers.AddColumn("PAUSED_BY", "VARCHAR2(800)");
        triggers.AddColumn("PAUSED_AT", "NUMBER(19)");
        triggers.AddColumn("JOB_DATA", "BLOB");
        triggers.PrimaryKeyName = "QRTZ_TRIGGERS_PK";
        triggers.ForeignKeys.Add(new ForeignKey("QRTZ_TRIGGER_TO_JOBS_FK")
        {
            LinkedTable = new OracleObjectName(SchemaName, "QRTZ_JOB_DETAILS"),
            ColumnNames = ["SCHED_NAME", "JOB_NAME", "JOB_GROUP"],
            LinkedNames = ["SCHED_NAME", "JOB_NAME", "JOB_GROUP"]
        });

        var acquisition = new IndexDefinition("IDX_QRTZ_T_NFT_ST")
        {
            Columns = ["SCHED_NAME", "TRIGGER_STATE", "NEXT_FIRE_TIME", "PRIORITY", "MISFIRE_INSTR"]
        };
        acquisition.DescendingColumns.Add("PRIORITY");
        triggers.Indexes.Add(acquisition);

        var delta = await triggers.FindDeltaAsync(theConnection, Ct);

        delta.Indexes.Different.ShouldBeEmpty();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
