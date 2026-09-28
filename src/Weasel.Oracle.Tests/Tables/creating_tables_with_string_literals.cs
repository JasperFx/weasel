using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Tables;

/// <summary>
///     The default <see cref="CreationStyle.CreateIfNotExists" /> path creates an Oracle table from
///     inside a PL/SQL block, as the text of an <c>EXECUTE IMMEDIATE '…'</c> string literal. The
///     column declarations used to be written into that literal as they were, so the first single
///     quote in one — any <c>DEFAULT '…'</c> — ended the literal early and the block failed to
///     compile (PLS-00103). A string default is not exotic: it is how a flag column is declared.
/// </summary>
public class creating_tables_with_string_literals: IntegrationContext
{
    private const string SchemaName = "literals";

    public creating_tables_with_string_literals(): base(SchemaName)
    {
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void the_guarded_create_doubles_the_quotes_it_embeds()
    {
        var table = new Table($"{SchemaName}.flags");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("is_enabled", "VARCHAR2(1)").DefaultValueByString("0").NotNull();

        var ddl = table.ToBasicCreateTableSql();

        ddl.ShouldContain("DEFAULT ''0''");
        ddl.ShouldNotContain("DEFAULT '0'");
    }

    [Fact]
    public void drop_then_create_is_not_a_literal_and_is_left_alone()
    {
        var table = new Table($"{SchemaName}.flags");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("is_enabled", "VARCHAR2(1)").DefaultValueByString("0").NotNull();

        var writer = new StringWriter();
        table.WriteCreateStatement(new OracleMigrator { TableCreation = CreationStyle.DropThenCreate }, writer);

        writer.ToString().ShouldContain("DEFAULT '0'");
    }

    [Fact]
    public async Task a_string_default_is_created_and_applied()
    {
        await ResetSchema();

        var table = new Table($"{SchemaName}.flags");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("is_enabled", "VARCHAR2(1)").DefaultValueByString("0").NotNull();

        await CreateSchemaObjectInDatabase(table);

        await theConnection.CreateCommand($"INSERT INTO {table.Identifier} (id) VALUES (1)").ExecuteNonQueryAsync(Ct);

        var value = await theConnection.CreateCommand($"SELECT is_enabled FROM {table.Identifier} WHERE id = 1")
            .ExecuteScalarAsync(Ct);

        value.ShouldBe("0");
    }

    [Fact]
    public async Task a_default_that_itself_contains_a_quote_round_trips()
    {
        await ResetSchema();

        var table = new Table($"{SchemaName}.greetings");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("greeting", "VARCHAR2(50)").DefaultValueByExpression("'it''s here'");

        await CreateSchemaObjectInDatabase(table);

        await theConnection.CreateCommand($"INSERT INTO {table.Identifier} (id) VALUES (1)").ExecuteNonQueryAsync(Ct);

        var value = await theConnection.CreateCommand($"SELECT greeting FROM {table.Identifier} WHERE id = 1")
            .ExecuteScalarAsync(Ct);

        value.ShouldBe("it's here");
    }

    /// <summary>
    ///     A column added to an existing table goes out as a plain <c>ALTER TABLE … ADD</c>, not inside
    ///     a literal, so it was never affected. Pinned so the two paths stay told apart.
    /// </summary>
    [Fact]
    public async Task a_string_defaulted_column_added_later_is_created()
    {
        await ResetSchema();

        var before = new Table($"{SchemaName}.flags");
        before.AddColumn<int>("id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(before);

        var after = new Table($"{SchemaName}.flags");
        after.AddColumn<int>("id").AsPrimaryKey();
        after.AddColumn("is_enabled", "VARCHAR2(1)").DefaultValueByString("0").NotNull();

        await after.ApplyChangesAsync(theConnection, Ct);

        (await after.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Quartz.NET's execution-history table as its job store declares it: one numeric and one
    ///     string default, created through the migration path and then checked for drift.
    /// </summary>
    [Fact]
    public async Task a_real_world_table_migrates_and_settles()
    {
        await ResetSchema();

        static Table ExecutionHistory()
        {
            var table = new Table($"{SchemaName}.qrtz_execution_history");
            table.AddColumn("sched_name", "VARCHAR2(120)").AsPrimaryKey();
            table.AddColumn("entry_id", "VARCHAR2(140)").AsPrimaryKey();
            table.AddColumn("instance_name", "VARCHAR2(200)").NotNull();
            table.AddColumn("job_name", "VARCHAR2(200)").NotNull();
            table.AddColumn("job_group", "VARCHAR2(200)").NotNull();
            table.AddColumn("trigger_name", "VARCHAR2(200)").NotNull();
            table.AddColumn("trigger_group", "VARCHAR2(200)").NotNull();
            table.AddColumn("fired_time", "NUMBER(19)").NotNull();
            table.AddColumn("run_time", "NUMBER(19)").NotNull();
            table.AddColumn("succeeded", "VARCHAR2(1)").NotNull();
            table.AddColumn("error_message", "VARCHAR2(4000)");
            table.AddColumn("retry_attempt", "NUMBER(13)").DefaultValue(0).NotNull();
            table.AddColumn("retry_scheduled", "VARCHAR2(1)").DefaultValueByString("0").NotNull();
            table.AddColumn("execution_log", "CLOB");
            return table;
        }

        (await ExecutionHistory().MigrateAsync(theConnection)).ShouldBeTrue();

        (await ExecutionHistory().FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await ExecutionHistory().MigrateAsync(theConnection)).ShouldBeFalse();
    }
}
