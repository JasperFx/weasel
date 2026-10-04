using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     Quartz.NET's Firebird job store schema, both ways round: created by Quartz's own script and read
///     back as no change against a Weasel model written the way Quartz.Weasel writes one, and created by
///     Weasel and read back from the catalog exactly as the script leaves it. Oracle's
///     <c>index_sort_direction</c> carries a Quartz model too; this is the whole schema.
/// </summary>
/// <remarks>
///     The model is written as Quartz writes it: add-only, upper-case names, the script's own
///     <c>PK_</c>, <c>FK_…_1</c> and <c>IDX_</c> names, and <c>DEFAULT NULL</c> on the columns the
///     script declares that way. It runs on a UTF8 database -- whose keys over long <c>VARCHAR</c>
///     columns need 16 KB pages -- and on a NONE one.
/// </remarks>
public abstract class quartz_job_store_schema_round_trips: IntegrationContext
{
    protected quartz_job_store_schema_round_trips(string database, string charset): base(database, charset)
    {
    }

    /// <summary>
    ///     <c>database/tables/tables_firebird.sql</c> from Quartz.NET 4.3, without the drop block.
    /// </summary>
    private const string Script = """
        CREATE TABLE QRTZ_JOB_DETAILS (
            SCHED_NAME         VARCHAR(120) NOT NULL,
            JOB_NAME           VARCHAR(150) NOT NULL,
            JOB_GROUP          VARCHAR(150) NOT NULL,
            DESCRIPTION        VARCHAR(250) default NULL,
            JOB_CLASS_NAME     VARCHAR(250) NOT NULL,
            IS_DURABLE         SMALLINT NOT NULL,
            IS_NONCONCURRENT   SMALLINT NOT NULL,
            IS_UPDATE_DATA     SMALLINT NOT NULL,
            REQUESTS_RECOVERY  SMALLINT NOT NULL,
            JOB_DATA           BLOB DEFAULT NULL,
            CONSTRAINT PK_QRTZ_JOB_DETAILS PRIMARY KEY (SCHED_NAME,JOB_NAME,JOB_GROUP)
        );

        CREATE TABLE QRTZ_TRIGGERS (
            SCHED_NAME      VARCHAR(120) NOT NULL,
            TRIGGER_NAME    VARCHAR(150) NOT NULL,
            TRIGGER_GROUP   VARCHAR(150) NOT NULL,
            JOB_NAME        VARCHAR(150) NOT NULL,
            JOB_GROUP       VARCHAR(150) NOT NULL,
            DESCRIPTION     VARCHAR(250) DEFAULT NULL,
            NEXT_FIRE_TIME  BIGINT DEFAULT NULL,
            PREV_FIRE_TIME  BIGINT DEFAULT NULL,
            PRIORITY        INTEGER DEFAULT NULL,
            TRIGGER_STATE   VARCHAR(16) NOT NULL,
            TRIGGER_TYPE    VARCHAR(8) NOT NULL,
            START_TIME      BIGINT NOT NULL,
            END_TIME        BIGINT DEFAULT NULL,
            CALENDAR_NAME   VARCHAR(200) DEFAULT NULL,
            MISFIRE_INSTR   SMALLINT DEFAULT NULL,
            MISFIRE_ORIG_FIRE_TIME BIGINT DEFAULT NULL,
            EXECUTION_GROUP VARCHAR(200),
            PREFERRED_NODE  VARCHAR(200),
            PREFERRED_NODE_AUTO SMALLINT DEFAULT 0 NOT NULL,
            RETRY_POLICY    VARCHAR(250),
            RETRY_ATTEMPT   INTEGER DEFAULT NULL,
            CONTINUES_TRIGGER_NAME VARCHAR(150) DEFAULT NULL,
            CONTINUES_TRIGGER_GROUP VARCHAR(150) DEFAULT NULL,
            CONTINUATION_CONDITION INTEGER DEFAULT NULL,
            OVERLAP_POLICY INTEGER DEFAULT NULL,
            PAUSE_REASON    VARCHAR(1000) DEFAULT NULL,
            PAUSED_BY       VARCHAR(800) DEFAULT NULL,
            PAUSED_AT       BIGINT DEFAULT NULL,
            JOB_DATA        BLOB DEFAULT NULL,
            CONSTRAINT PK_QRTZ_TRIGGERS PRIMARY KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
            CONSTRAINT FK_QRTZ_TRIGGERS_1 FOREIGN KEY (SCHED_NAME, JOB_NAME, JOB_GROUP)
            REFERENCES QRTZ_JOB_DETAILS(SCHED_NAME, JOB_NAME, JOB_GROUP)
        );

        CREATE TABLE QRTZ_SIMPLE_TRIGGERS (
            SCHED_NAME       VARCHAR(120) NOT NULL,
            TRIGGER_NAME     VARCHAR(150) NOT NULL,
            TRIGGER_GROUP    VARCHAR(150) NOT NULL,
            REPEAT_COUNT     BIGINT NOT NULL,
            REPEAT_INTERVAL  BIGINT NOT NULL,
            TIMES_TRIGGERED  BIGINT NOT NULL,
            CONSTRAINT PK_QRTZ_SIMPLE_TRIGGERS PRIMARY KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
            CONSTRAINT FK_QRTZ_SIMPLE_TRIGGERS_1 FOREIGN KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
            REFERENCES QRTZ_TRIGGERS(SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
        );

        CREATE TABLE QRTZ_CRON_TRIGGERS (
            SCHED_NAME       VARCHAR(120) NOT NULL,
            TRIGGER_NAME     VARCHAR(150) NOT NULL,
            TRIGGER_GROUP    VARCHAR(150) NOT NULL,
            CRON_EXPRESSION  VARCHAR(250) NOT NULL,
            TIME_ZONE_ID     VARCHAR(80),
            CONSTRAINT PK_QRTZ_CRON_TRIGGERS PRIMARY KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
            CONSTRAINT FK_QRTZ_CRON_TRIGGERS_1 FOREIGN KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
            REFERENCES QRTZ_TRIGGERS(SCHED_NAME, TRIGGER_NAME,TRIGGER_GROUP)
        );

        CREATE TABLE QRTZ_SIMPROP_TRIGGERS (
            SCHED_NAME     VARCHAR(120) NOT NULL,
            TRIGGER_NAME   VARCHAR(150) NOT NULL,
            TRIGGER_GROUP  VARCHAR(150) NOT NULL,
            STR_PROP_1     VARCHAR(512) DEFAULT NULL,
            STR_PROP_2     VARCHAR(512) DEFAULT NULL,
            STR_PROP_3     VARCHAR(512) DEFAULT NULL,
            INT_PROP_1     INTEGER DEFAULT NULL,
            INT_PROP_2     INTEGER DEFAULT NULL,
            LONG_PROP_1    BIGINT DEFAULT NULL,
            LONG_PROP_2    BIGINT DEFAULT NULL,
            DEC_PROP_1     NUMERIC(9,0) DEFAULT  NULL,
            DEC_PROP_2     NUMERIC(9,0) DEFAULT NULL,
            BOOL_PROP_1    SMALLINT DEFAULT NULL,
            BOOL_PROP_2    SMALLINT DEFAULT NULL,
            TIME_ZONE_ID   VARCHAR(80) DEFAULT NULL,
            CONSTRAINT PK_QRTZ_SIMPROP_TRIGGERS PRIMARY KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
            CONSTRAINT FK_QRTZ_SIMPROP_TRIGGERS_1 FOREIGN KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
            REFERENCES QRTZ_TRIGGERS(SCHED_NAME, TRIGGER_NAME,TRIGGER_GROUP)
        );

        CREATE TABLE QRTZ_BLOB_TRIGGERS (
            SCHED_NAME     VARCHAR(120) NOT NULL,
            TRIGGER_NAME   VARCHAR(150) NOT NULL,
            TRIGGER_GROUP  VARCHAR(150) NOT NULL,
            BLOB_DATA      BLOB DEFAULT NULL,
            CONSTRAINT PK_QRTZ_BLOB_TRIGGERS PRIMARY KEY (SCHED_NAME, TRIGGER_NAME,TRIGGER_GROUP),
            CONSTRAINT FK_QRTZ_BLOB_TRIGGERS_1 FOREIGN KEY (SCHED_NAME, TRIGGER_NAME,TRIGGER_GROUP)
            REFERENCES QRTZ_TRIGGERS(SCHED_NAME, TRIGGER_NAME,TRIGGER_GROUP)
        );

        CREATE TABLE QRTZ_CALENDARS (
            SCHED_NAME     VARCHAR(120) NOT NULL,
            CALENDAR_NAME  VARCHAR(200) NOT NULL,
            CALENDAR       BLOB NOT NULL,
            CONSTRAINT PK_QRTZ_CALENDARS PRIMARY KEY (SCHED_NAME, CALENDAR_NAME)
        );

        CREATE TABLE QRTZ_PAUSED_TRIGGER_GRPS (
            SCHED_NAME     VARCHAR(120) NOT NULL,
            TRIGGER_GROUP  VARCHAR(150) NOT NULL,
            PAUSE_REASON   VARCHAR(1000) DEFAULT NULL,
            PAUSED_BY      VARCHAR(800) DEFAULT NULL,
            PAUSED_AT      BIGINT DEFAULT NULL,
            CONSTRAINT PK_QRTZ_PAUSED_TRIGGER_GRPS PRIMARY KEY (SCHED_NAME, TRIGGER_GROUP)
        );

        CREATE TABLE QRTZ_PAUSED_JOB_GRPS (
            SCHED_NAME     VARCHAR(120) NOT NULL,
            JOB_GROUP      VARCHAR(150) NOT NULL,
            PAUSE_REASON   VARCHAR(1000) DEFAULT NULL,
            PAUSED_BY      VARCHAR(800) DEFAULT NULL,
            PAUSED_AT      BIGINT DEFAULT NULL,
            CONSTRAINT PK_QRTZ_PAUSED_JOB_GRPS PRIMARY KEY (SCHED_NAME, JOB_GROUP)
        );

        CREATE TABLE QRTZ_FIRED_TRIGGERS (
            SCHED_NAME         VARCHAR(120) NOT NULL,
            ENTRY_ID           VARCHAR(140) NOT NULL,
            TRIGGER_NAME       VARCHAR(150) NOT NULL,
            TRIGGER_GROUP      VARCHAR(150) NOT NULL,
            INSTANCE_NAME      VARCHAR(200) NOT NULL,
            FIRED_TIME         BIGINT NOT NULL,
            SCHED_TIME         BIGINT NOT NULL,
            PRIORITY           INTEGER NOT NULL,
            STATE              VARCHAR(16) NOT NULL,
            JOB_NAME           VARCHAR(150) DEFAULT NULL,
            JOB_GROUP          VARCHAR(150) DEFAULT NULL,
            IS_NONCONCURRENT   SMALLINT NOT NULL,
            REQUESTS_RECOVERY  SMALLINT DEFAULT NULL,
            EXECUTION_GROUP    VARCHAR(200),
            PROGRESS           INTEGER DEFAULT NULL,
            PROGRESS_MESSAGE   VARCHAR(250) DEFAULT NULL,
            CONSTRAINT PK_QRTZ_FIRED_TRIGGERS PRIMARY KEY (SCHED_NAME, ENTRY_ID)
        );

        CREATE TABLE QRTZ_SCHEDULER_STATE (
            SCHED_NAME         VARCHAR(120) NOT NULL,
            INSTANCE_NAME      VARCHAR(200) NOT NULL,
            LAST_CHECKIN_TIME  BIGINT NOT NULL,
            CHECKIN_INTERVAL   BIGINT NOT NULL,
            CONSTRAINT PK_QRTZ_SCHEDULER_STATE PRIMARY KEY (SCHED_NAME, INSTANCE_NAME)
        );

        CREATE TABLE QRTZ_LOCKS (
            SCHED_NAME  VARCHAR(120) NOT NULL,
            LOCK_NAME   VARCHAR(40) NOT NULL,
            CONSTRAINT PK_QRTZ_LOCKS PRIMARY KEY (SCHED_NAME, LOCK_NAME)
        );

        CREATE TABLE QRTZ_EXECUTION_HISTORY (
            SCHED_NAME     VARCHAR(120) NOT NULL,
            ENTRY_ID       VARCHAR(140) NOT NULL,
            INSTANCE_NAME  VARCHAR(200) NOT NULL,
            JOB_NAME       VARCHAR(150) NOT NULL,
            JOB_GROUP      VARCHAR(150) NOT NULL,
            TRIGGER_NAME   VARCHAR(150) NOT NULL,
            TRIGGER_GROUP  VARCHAR(150) NOT NULL,
            FIRED_TIME     BIGINT NOT NULL,
            RUN_TIME       BIGINT NOT NULL,
            SUCCEEDED      SMALLINT NOT NULL,
            ERROR_MESSAGE  VARCHAR(1000) DEFAULT NULL,
            RETRY_ATTEMPT  INTEGER DEFAULT 0 NOT NULL,
            RETRY_SCHEDULED SMALLINT DEFAULT 0 NOT NULL,
            EXECUTION_LOG  BLOB SUB_TYPE TEXT DEFAULT NULL,
            CONSTRAINT PK_QRTZ_EXECUTION_HISTORY PRIMARY KEY (SCHED_NAME, ENTRY_ID)
        );

        CREATE TABLE QRTZ_MISFIRE_HISTORY (
            SCHED_NAME     VARCHAR(120) NOT NULL,
            ENTRY_ID       VARCHAR(140) NOT NULL,
            INSTANCE_NAME  VARCHAR(200) NOT NULL,
            TRIGGER_NAME   VARCHAR(150) NOT NULL,
            TRIGGER_GROUP  VARCHAR(150) NOT NULL,
            JOB_NAME       VARCHAR(150) DEFAULT NULL,
            JOB_GROUP      VARCHAR(150) DEFAULT NULL,
            MISFIRE_TIME   BIGINT NOT NULL,
            SCHED_TIME     BIGINT DEFAULT NULL,
            REASON         INTEGER DEFAULT NULL,
            CONSTRAINT PK_QRTZ_MISFIRE_HISTORY PRIMARY KEY (SCHED_NAME, ENTRY_ID)
        );

        CREATE INDEX IDX_QRTZ_J_G_N ON QRTZ_JOB_DETAILS(SCHED_NAME,JOB_GROUP,JOB_NAME);

        CREATE INDEX IDX_QRTZ_T_J ON QRTZ_TRIGGERS(SCHED_NAME,JOB_NAME,JOB_GROUP);
        CREATE INDEX IDX_QRTZ_T_C ON QRTZ_TRIGGERS(SCHED_NAME,CALENDAR_NAME);
        CREATE INDEX IDX_QRTZ_T_G_N ON QRTZ_TRIGGERS(SCHED_NAME,TRIGGER_GROUP,TRIGGER_NAME);
        CREATE INDEX IDX_QRTZ_T_NFT_ST ON QRTZ_TRIGGERS(SCHED_NAME,TRIGGER_STATE,NEXT_FIRE_TIME);

        CREATE INDEX IDX_QRTZ_FT_INST_JOB_REQ_RCVRY ON QRTZ_FIRED_TRIGGERS(SCHED_NAME,INSTANCE_NAME,REQUESTS_RECOVERY);
        CREATE INDEX IDX_QRTZ_FT_J_G ON QRTZ_FIRED_TRIGGERS(SCHED_NAME,JOB_NAME,JOB_GROUP);
        CREATE INDEX IDX_QRTZ_FT_T_G ON QRTZ_FIRED_TRIGGERS(SCHED_NAME,TRIGGER_NAME,TRIGGER_GROUP);

        CREATE INDEX IDX_QRTZ_EH_FIRED_TIME ON QRTZ_EXECUTION_HISTORY(SCHED_NAME,FIRED_TIME);
        CREATE INDEX IDX_QRTZ_EH_INST ON QRTZ_EXECUTION_HISTORY(SCHED_NAME,INSTANCE_NAME);
        CREATE INDEX IDX_QRTZ_MH_MISFIRE_TIME ON QRTZ_MISFIRE_HISTORY(SCHED_NAME,MISFIRE_TIME);
        CREATE INDEX IDX_QRTZ_MH_INST ON QRTZ_MISFIRE_HISTORY(SCHED_NAME,INSTANCE_NAME);

        COMMIT;
        """;

    /// <summary>
    ///     The columns Quartz 4.3 added -- pause reasons, progress, execution log -- which a 4.2 database
    ///     does not have yet.
    /// </summary>
    private static readonly (string Table, string Column)[] AddedIn43 =
    [
        ("QRTZ_TRIGGERS", "PAUSE_REASON"),
        ("QRTZ_TRIGGERS", "PAUSED_BY"),
        ("QRTZ_TRIGGERS", "PAUSED_AT"),
        ("QRTZ_PAUSED_TRIGGER_GRPS", "PAUSE_REASON"),
        ("QRTZ_PAUSED_TRIGGER_GRPS", "PAUSED_BY"),
        ("QRTZ_PAUSED_TRIGGER_GRPS", "PAUSED_AT"),
        ("QRTZ_PAUSED_JOB_GRPS", "PAUSE_REASON"),
        ("QRTZ_PAUSED_JOB_GRPS", "PAUSED_BY"),
        ("QRTZ_PAUSED_JOB_GRPS", "PAUSED_AT"),
        ("QRTZ_FIRED_TRIGGERS", "PROGRESS"),
        ("QRTZ_FIRED_TRIGGERS", "PROGRESS_MESSAGE"),
        ("QRTZ_EXECUTION_HISTORY", "EXECUTION_LOG")
    ];

    /// <summary>
    ///     The schema, written the way Quartz.Weasel writes it.
    /// </summary>
    internal static Table[] QuartzModel()
    {
        Table table(string name, string primaryKey, Action<Table> columns, params string[] keyColumns)
        {
            var t = new Table(name) { AddOnlyMigrations = true };
            columns(t);
            foreach (var key in keyColumns)
            {
                t.ModifyColumn(key).AsPrimaryKey();
            }

            t.PrimaryKeyName = primaryKey;
            return t;
        }

        void notNull(Table t, string name, string type) => t.AddColumn(name, type).NotNull();
        void nullDefault(Table t, string name, string type) => t.AddColumn(name, type).DefaultValueByExpression("NULL");
        void nullable(Table t, string name, string type) => t.AddColumn(name, type);

        void index(Table t, string name, params string[] columns)
            => t.Indexes.Add(new IndexDefinition(name) { Columns = columns });

        void foreignKey(Table t, string name, string linked, params string[] columns)
            => t.ForeignKeys.Add(new ForeignKey(name)
            {
                LinkedTable = new FirebirdObjectName(linked), ColumnNames = columns, LinkedNames = columns
            });

        string[] triggerKey = ["SCHED_NAME", "TRIGGER_NAME", "TRIGGER_GROUP"];

        var jobDetails = table("QRTZ_JOB_DETAILS", "PK_QRTZ_JOB_DETAILS", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, "JOB_NAME", "VARCHAR(150)");
            notNull(t, "JOB_GROUP", "VARCHAR(150)");
            nullDefault(t, "DESCRIPTION", "VARCHAR(250)");
            notNull(t, "JOB_CLASS_NAME", "VARCHAR(250)");
            notNull(t, "IS_DURABLE", "SMALLINT");
            notNull(t, "IS_NONCONCURRENT", "SMALLINT");
            notNull(t, "IS_UPDATE_DATA", "SMALLINT");
            notNull(t, "REQUESTS_RECOVERY", "SMALLINT");
            nullDefault(t, "JOB_DATA", "BLOB");
            index(t, "IDX_QRTZ_J_G_N", "SCHED_NAME", "JOB_GROUP", "JOB_NAME");
        }, "SCHED_NAME", "JOB_NAME", "JOB_GROUP");

        var triggers = table("QRTZ_TRIGGERS", "PK_QRTZ_TRIGGERS", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, "TRIGGER_NAME", "VARCHAR(150)");
            notNull(t, "TRIGGER_GROUP", "VARCHAR(150)");
            notNull(t, "JOB_NAME", "VARCHAR(150)");
            notNull(t, "JOB_GROUP", "VARCHAR(150)");
            nullDefault(t, "DESCRIPTION", "VARCHAR(250)");
            nullDefault(t, "NEXT_FIRE_TIME", "BIGINT");
            nullDefault(t, "PREV_FIRE_TIME", "BIGINT");
            nullDefault(t, "PRIORITY", "INTEGER");
            notNull(t, "TRIGGER_STATE", "VARCHAR(16)");
            notNull(t, "TRIGGER_TYPE", "VARCHAR(8)");
            notNull(t, "START_TIME", "BIGINT");
            nullDefault(t, "END_TIME", "BIGINT");
            nullDefault(t, "CALENDAR_NAME", "VARCHAR(200)");
            nullDefault(t, "MISFIRE_INSTR", "SMALLINT");
            nullDefault(t, "MISFIRE_ORIG_FIRE_TIME", "BIGINT");
            nullable(t, "EXECUTION_GROUP", "VARCHAR(200)");
            nullable(t, "PREFERRED_NODE", "VARCHAR(200)");
            t.AddColumn("PREFERRED_NODE_AUTO", "SMALLINT").NotNull().DefaultValue(0);
            nullable(t, "RETRY_POLICY", "VARCHAR(250)");
            nullDefault(t, "RETRY_ATTEMPT", "INTEGER");
            nullDefault(t, "CONTINUES_TRIGGER_NAME", "VARCHAR(150)");
            nullDefault(t, "CONTINUES_TRIGGER_GROUP", "VARCHAR(150)");
            nullDefault(t, "CONTINUATION_CONDITION", "INTEGER");
            nullDefault(t, "OVERLAP_POLICY", "INTEGER");
            nullDefault(t, "PAUSE_REASON", "VARCHAR(1000)");
            nullDefault(t, "PAUSED_BY", "VARCHAR(800)");
            nullDefault(t, "PAUSED_AT", "BIGINT");
            nullDefault(t, "JOB_DATA", "BLOB");
            foreignKey(t, "FK_QRTZ_TRIGGERS_1", "QRTZ_JOB_DETAILS", "SCHED_NAME", "JOB_NAME", "JOB_GROUP");
            index(t, "IDX_QRTZ_T_J", "SCHED_NAME", "JOB_NAME", "JOB_GROUP");
            index(t, "IDX_QRTZ_T_C", "SCHED_NAME", "CALENDAR_NAME");
            index(t, "IDX_QRTZ_T_G_N", "SCHED_NAME", "TRIGGER_GROUP", "TRIGGER_NAME");
            index(t, "IDX_QRTZ_T_NFT_ST", "SCHED_NAME", "TRIGGER_STATE", "NEXT_FIRE_TIME");
        }, triggerKey);

        var simple = table("QRTZ_SIMPLE_TRIGGERS", "PK_QRTZ_SIMPLE_TRIGGERS", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, "TRIGGER_NAME", "VARCHAR(150)");
            notNull(t, "TRIGGER_GROUP", "VARCHAR(150)");
            notNull(t, "REPEAT_COUNT", "BIGINT");
            notNull(t, "REPEAT_INTERVAL", "BIGINT");
            notNull(t, "TIMES_TRIGGERED", "BIGINT");
            foreignKey(t, "FK_QRTZ_SIMPLE_TRIGGERS_1", "QRTZ_TRIGGERS", triggerKey);
        }, triggerKey);

        var cron = table("QRTZ_CRON_TRIGGERS", "PK_QRTZ_CRON_TRIGGERS", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, "TRIGGER_NAME", "VARCHAR(150)");
            notNull(t, "TRIGGER_GROUP", "VARCHAR(150)");
            notNull(t, "CRON_EXPRESSION", "VARCHAR(250)");
            nullable(t, "TIME_ZONE_ID", "VARCHAR(80)");
            foreignKey(t, "FK_QRTZ_CRON_TRIGGERS_1", "QRTZ_TRIGGERS", triggerKey);
        }, triggerKey);

        var simprop = table("QRTZ_SIMPROP_TRIGGERS", "PK_QRTZ_SIMPROP_TRIGGERS", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, "TRIGGER_NAME", "VARCHAR(150)");
            notNull(t, "TRIGGER_GROUP", "VARCHAR(150)");
            nullDefault(t, "STR_PROP_1", "VARCHAR(512)");
            nullDefault(t, "STR_PROP_2", "VARCHAR(512)");
            nullDefault(t, "STR_PROP_3", "VARCHAR(512)");
            nullDefault(t, "INT_PROP_1", "INTEGER");
            nullDefault(t, "INT_PROP_2", "INTEGER");
            nullDefault(t, "LONG_PROP_1", "BIGINT");
            nullDefault(t, "LONG_PROP_2", "BIGINT");
            nullDefault(t, "DEC_PROP_1", "NUMERIC(9,0)");
            nullDefault(t, "DEC_PROP_2", "NUMERIC(9,0)");
            nullDefault(t, "BOOL_PROP_1", "SMALLINT");
            nullDefault(t, "BOOL_PROP_2", "SMALLINT");
            nullDefault(t, "TIME_ZONE_ID", "VARCHAR(80)");
            foreignKey(t, "FK_QRTZ_SIMPROP_TRIGGERS_1", "QRTZ_TRIGGERS", triggerKey);
        }, triggerKey);

        var blob = table("QRTZ_BLOB_TRIGGERS", "PK_QRTZ_BLOB_TRIGGERS", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, "TRIGGER_NAME", "VARCHAR(150)");
            notNull(t, "TRIGGER_GROUP", "VARCHAR(150)");
            nullDefault(t, "BLOB_DATA", "BLOB");
            foreignKey(t, "FK_QRTZ_BLOB_TRIGGERS_1", "QRTZ_TRIGGERS", triggerKey);
        }, triggerKey);

        var calendars = table("QRTZ_CALENDARS", "PK_QRTZ_CALENDARS", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, "CALENDAR_NAME", "VARCHAR(200)");
            notNull(t, "CALENDAR", "BLOB");
        }, "SCHED_NAME", "CALENDAR_NAME");

        Table pausedGroups(string name, string group) => table(name, $"PK_{name}", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, group, "VARCHAR(150)");
            nullDefault(t, "PAUSE_REASON", "VARCHAR(1000)");
            nullDefault(t, "PAUSED_BY", "VARCHAR(800)");
            nullDefault(t, "PAUSED_AT", "BIGINT");
        }, "SCHED_NAME", group);

        var fired = table("QRTZ_FIRED_TRIGGERS", "PK_QRTZ_FIRED_TRIGGERS", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, "ENTRY_ID", "VARCHAR(140)");
            notNull(t, "TRIGGER_NAME", "VARCHAR(150)");
            notNull(t, "TRIGGER_GROUP", "VARCHAR(150)");
            notNull(t, "INSTANCE_NAME", "VARCHAR(200)");
            notNull(t, "FIRED_TIME", "BIGINT");
            notNull(t, "SCHED_TIME", "BIGINT");
            notNull(t, "PRIORITY", "INTEGER");
            notNull(t, "STATE", "VARCHAR(16)");
            nullDefault(t, "JOB_NAME", "VARCHAR(150)");
            nullDefault(t, "JOB_GROUP", "VARCHAR(150)");
            notNull(t, "IS_NONCONCURRENT", "SMALLINT");
            nullDefault(t, "REQUESTS_RECOVERY", "SMALLINT");
            nullable(t, "EXECUTION_GROUP", "VARCHAR(200)");
            nullDefault(t, "PROGRESS", "INTEGER");
            nullDefault(t, "PROGRESS_MESSAGE", "VARCHAR(250)");
            index(t, "IDX_QRTZ_FT_INST_JOB_REQ_RCVRY", "SCHED_NAME", "INSTANCE_NAME", "REQUESTS_RECOVERY");
            index(t, "IDX_QRTZ_FT_J_G", "SCHED_NAME", "JOB_NAME", "JOB_GROUP");
            index(t, "IDX_QRTZ_FT_T_G", "SCHED_NAME", "TRIGGER_NAME", "TRIGGER_GROUP");
        }, "SCHED_NAME", "ENTRY_ID");

        var state = table("QRTZ_SCHEDULER_STATE", "PK_QRTZ_SCHEDULER_STATE", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, "INSTANCE_NAME", "VARCHAR(200)");
            notNull(t, "LAST_CHECKIN_TIME", "BIGINT");
            notNull(t, "CHECKIN_INTERVAL", "BIGINT");
        }, "SCHED_NAME", "INSTANCE_NAME");

        var locks = table("QRTZ_LOCKS", "PK_QRTZ_LOCKS", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, "LOCK_NAME", "VARCHAR(40)");
        }, "SCHED_NAME", "LOCK_NAME");

        var executions = table("QRTZ_EXECUTION_HISTORY", "PK_QRTZ_EXECUTION_HISTORY", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, "ENTRY_ID", "VARCHAR(140)");
            notNull(t, "INSTANCE_NAME", "VARCHAR(200)");
            notNull(t, "JOB_NAME", "VARCHAR(150)");
            notNull(t, "JOB_GROUP", "VARCHAR(150)");
            notNull(t, "TRIGGER_NAME", "VARCHAR(150)");
            notNull(t, "TRIGGER_GROUP", "VARCHAR(150)");
            notNull(t, "FIRED_TIME", "BIGINT");
            notNull(t, "RUN_TIME", "BIGINT");
            notNull(t, "SUCCEEDED", "SMALLINT");
            nullDefault(t, "ERROR_MESSAGE", "VARCHAR(1000)");
            t.AddColumn("RETRY_ATTEMPT", "INTEGER").NotNull().DefaultValue(0);
            t.AddColumn("RETRY_SCHEDULED", "SMALLINT").NotNull().DefaultValue(0);
            nullDefault(t, "EXECUTION_LOG", "BLOB SUB_TYPE TEXT");
            index(t, "IDX_QRTZ_EH_FIRED_TIME", "SCHED_NAME", "FIRED_TIME");
            index(t, "IDX_QRTZ_EH_INST", "SCHED_NAME", "INSTANCE_NAME");
        }, "SCHED_NAME", "ENTRY_ID");

        var misfires = table("QRTZ_MISFIRE_HISTORY", "PK_QRTZ_MISFIRE_HISTORY", t =>
        {
            notNull(t, "SCHED_NAME", "VARCHAR(120)");
            notNull(t, "ENTRY_ID", "VARCHAR(140)");
            notNull(t, "INSTANCE_NAME", "VARCHAR(200)");
            notNull(t, "TRIGGER_NAME", "VARCHAR(150)");
            notNull(t, "TRIGGER_GROUP", "VARCHAR(150)");
            nullDefault(t, "JOB_NAME", "VARCHAR(150)");
            nullDefault(t, "JOB_GROUP", "VARCHAR(150)");
            notNull(t, "MISFIRE_TIME", "BIGINT");
            nullDefault(t, "SCHED_TIME", "BIGINT");
            nullDefault(t, "REASON", "INTEGER");
            index(t, "IDX_QRTZ_MH_MISFIRE_TIME", "SCHED_NAME", "MISFIRE_TIME");
            index(t, "IDX_QRTZ_MH_INST", "SCHED_NAME", "INSTANCE_NAME");
        }, "SCHED_NAME", "ENTRY_ID");

        return
        [
            jobDetails, triggers, simple, cron, simprop, blob, calendars,
            pausedGroups("QRTZ_PAUSED_TRIGGER_GRPS", "TRIGGER_GROUP"),
            pausedGroups("QRTZ_PAUSED_JOB_GRPS", "JOB_GROUP"),
            fired, state, locks, executions, misfires
        ];
    }

    /// <summary>
    ///     What the catalog holds for the Quartz tables, as text: every column with its stored type,
    ///     nullability and normalized default; every key, index and foreign key with its segments and
    ///     rules. Two databases that print the same are the same schema.
    /// </summary>
    private async Task<string[]> catalogAsync()
    {
        var columns = await ListAsync("""
            SELECT TRIM(rf.RDB$RELATION_NAME) || '.' || TRIM(rf.RDB$FIELD_NAME) || ' #' || rf.RDB$FIELD_POSITION
                || ' ' || f.RDB$FIELD_TYPE || '/' || COALESCE(f.RDB$FIELD_SUB_TYPE, 0)
                || ' p' || COALESCE(f.RDB$FIELD_PRECISION, 0) || ' s' || COALESCE(f.RDB$FIELD_SCALE, 0)
                || ' l' || COALESCE(f.RDB$CHARACTER_LENGTH, 0) || ' cs' || COALESCE(f.RDB$CHARACTER_SET_ID, -1)
                || ' nn' || COALESCE(rf.RDB$NULL_FLAG, f.RDB$NULL_FLAG, 0)
                || ' d:' || UPPER(COALESCE(CAST(rf.RDB$DEFAULT_SOURCE AS VARCHAR(200)), ''))
            FROM RDB$RELATION_FIELDS rf JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE
            WHERE rf.RDB$RELATION_NAME STARTING WITH 'QRTZ_'
            ORDER BY rf.RDB$RELATION_NAME, rf.RDB$FIELD_POSITION
            """);

        var indexes = await ListAsync("""
            SELECT TRIM(i.RDB$RELATION_NAME) || ' ' || TRIM(i.RDB$INDEX_NAME) || ' u' || COALESCE(i.RDB$UNIQUE_FLAG, 0)
                || ' t' || COALESCE(i.RDB$INDEX_TYPE, 0) || ' ' || TRIM(COALESCE(s.RDB$FIELD_NAME, '')) || ' @' || COALESCE(s.RDB$FIELD_POSITION, -1)
                || ' ' || COALESCE(TRIM(rc.RDB$CONSTRAINT_TYPE), '') || ' ' || COALESCE(TRIM(ref.RDB$UPDATE_RULE), '') || '/' || COALESCE(TRIM(ref.RDB$DELETE_RULE), '')
            FROM RDB$INDICES i
            LEFT JOIN RDB$INDEX_SEGMENTS s ON s.RDB$INDEX_NAME = i.RDB$INDEX_NAME
            LEFT JOIN RDB$RELATION_CONSTRAINTS rc ON rc.RDB$INDEX_NAME = i.RDB$INDEX_NAME
            LEFT JOIN RDB$REF_CONSTRAINTS ref ON ref.RDB$CONSTRAINT_NAME = rc.RDB$CONSTRAINT_NAME
            WHERE i.RDB$RELATION_NAME STARTING WITH 'QRTZ_'
            ORDER BY i.RDB$RELATION_NAME, i.RDB$INDEX_NAME, s.RDB$FIELD_POSITION
            """);

        // The script writes "default NULL" and "DEFAULT  NULL", and Weasel "DEFAULT NULL": the catalog
        // keeps the text as written, so case and spacing are folded before comparing.
        return columns
            .Select(x => System.Text.RegularExpressions.Regex.Replace(x, @"(?<= d:.*)\s+", " "))
            .Concat(indexes)
            .ToArray();
    }

    [Fact]
    public async Task the_scripts_schema_reads_back_as_no_change()
    {
        await ExecuteAsync(Script);

        var migration = await DetermineAsync(QuartzModel());

        migration.Deltas.Where(x => x.Difference != SchemaPatchDifference.None)
            .Select(x => $"{x.SchemaObject.Identifier}: {x.Difference}")
            .ShouldBeEmpty();
    }

    [Fact]
    public async Task weasels_schema_is_the_scripts_schema()
    {
        await ExecuteAsync(Script);
        var scripted = await catalogAsync();

        await theConnection.DropSchemaAsync();

        await ApplyAsync(QuartzModel());
        var applied = await catalogAsync();

        var onlyScripted = scripted.Except(applied).ToArray();
        var onlyApplied = applied.Except(scripted).ToArray();
        onlyScripted.ShouldBeEmpty(
            $"the script's catalog has these and Weasel's does not; Weasel's has instead:{Environment.NewLine}{string.Join(Environment.NewLine, onlyApplied)}");
        onlyApplied.ShouldBeEmpty();
        applied.ShouldBe(scripted, "the same rows, in the same order");

        (await DetermineAsync(QuartzModel())).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     A 4.2 database upgraded by the 4.3 model: the new columns are added, the rows already there
    ///     survive, and the next comparison is no change.
    /// </summary>
    [Fact]
    public async Task a_42_database_upgrades_by_adding_columns_and_keeps_its_rows()
    {
        await ExecuteAsync(Script);
        foreach (var (table, column) in AddedIn43)
        {
            await ExecuteAsync($"ALTER TABLE {table} DROP {column}");
        }

        await ExecuteAsync("""
            INSERT INTO QRTZ_JOB_DETAILS (SCHED_NAME, JOB_NAME, JOB_GROUP, JOB_CLASS_NAME, IS_DURABLE, IS_NONCONCURRENT, IS_UPDATE_DATA, REQUESTS_RECOVERY)
                VALUES ('s', 'job', 'g', 'MyJob', 1, 0, 0, 0);
            INSERT INTO QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, JOB_NAME, JOB_GROUP, TRIGGER_STATE, TRIGGER_TYPE, START_TIME, NEXT_FIRE_TIME)
                VALUES ('s', 't', 'g', 'job', 'g', 'WAITING', 'CRON', 1, 100);
            INSERT INTO QRTZ_CRON_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, CRON_EXPRESSION)
                VALUES ('s', 't', 'g', '0 0 * * * ?');
            INSERT INTO QRTZ_PAUSED_TRIGGER_GRPS (SCHED_NAME, TRIGGER_GROUP) VALUES ('s', 'paused');
            INSERT INTO QRTZ_LOCKS (SCHED_NAME, LOCK_NAME) VALUES ('s', 'TRIGGER_ACCESS');
            """);

        var migration = await DetermineAsync(QuartzModel());
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        await new FirebirdMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        (await DetermineAsync(QuartzModel())).Difference.ShouldBe(SchemaPatchDifference.None);
        (await ScalarAsync<int>("SELECT COUNT(*) FROM QRTZ_TRIGGERS WHERE NEXT_FIRE_TIME = 100 AND PAUSE_REASON IS NULL"))
            .ShouldBe(1);
        (await ScalarAsync<int>("SELECT COUNT(*) FROM QRTZ_CRON_TRIGGERS")).ShouldBe(1);
        (await ScalarAsync<int>("SELECT COUNT(*) FROM QRTZ_PAUSED_TRIGGER_GRPS")).ShouldBe(1);
        (await ScalarAsync<int>("SELECT COUNT(*) FROM QRTZ_LOCKS")).ShouldBe(1);
    }

    /// <summary>
    ///     Quartz's tables live beside an application's, and an add-only model leaves the application's
    ///     columns alone.
    /// </summary>
    [Fact]
    public async Task an_add_only_model_leaves_a_column_it_does_not_declare()
    {
        await ExecuteAsync(Script);
        await ExecuteAsync("ALTER TABLE QRTZ_JOB_DETAILS ADD TENANT VARCHAR(20)");

        var migration = await DetermineAsync(QuartzModel());

        migration.Difference.ShouldBe(SchemaPatchDifference.None);
        ((TableDelta)migration.Deltas[0]).WithheldDrops.ShouldBe(["column TENANT"]);
    }

    [Fact]
    public void every_name_fits_firebird_3()
    {
        var migrator = new FirebirdMigrator();

        foreach (var table in QuartzModel())
        {
            foreach (var name in table.AllNames())
            {
                migrator.AssertValidIdentifier(name.Name);
            }

            foreach (var name in table.LocalIdentifiers())
            {
                migrator.AssertValidLocalIdentifier(name);
            }
        }
    }
}

public class quartz_job_store_schema_round_trips_on_a_utf8_database(): quartz_job_store_schema_round_trips("quartz_utf8", "UTF8");

public class quartz_job_store_schema_round_trips_on_a_none_database(): quartz_job_store_schema_round_trips("quartz_none", "NONE");
