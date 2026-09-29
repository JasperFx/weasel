using System.Data.Common;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     Firebird executes one statement per command and applies DDL at commit, so the shape of what the
///     migrator sends matters as much as what it says: one statement per command, one transaction per
///     statement, and a script isql can run with a commit after every statement.
/// </summary>
public class firebird_delta_statement_shape: IntegrationContext
{
    private sealed class RecordingLogger: IMigrationLogger
    {
        public List<string> Statements { get; } = new();
        public List<Exception> Failures { get; } = new();

        public void SchemaChange(string sql) => Statements.Add(sql);

        public void OnFailure(DbCommand command, Exception ex) => Failures.Add(ex);
    }

    private static Table people()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").AddIndex();
        table.AddColumn<int>("state_id").ForeignKeyTo("states", "id");
        return table;
    }

    private static Table states()
    {
        var table = new Table("states");
        table.AddColumn<int>("id").AsPrimaryKey();
        return table;
    }

    [Fact]
    public async Task every_statement_is_sent_as_a_command_of_its_own()
    {
        var logger = new RecordingLogger();
        var migrator = new FirebirdMigrator();
        var migration = await SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None, states(), people());

        await migrator.ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate, logger);

        // Two tables, an index and a foreign key: four commands, each a guarded EXECUTE BLOCK and none
        // carrying the isql terminator directives, which the server would reject.
        logger.Statements.Count.ShouldBe(4);
        logger.Statements.ShouldAllBe(x => x.StartsWith("EXECUTE BLOCK"));
        logger.Statements.ShouldAllBe(x => !x.Contains("SET TERM") && !x.TrimEnd().EndsWith('^'));
    }

    [Fact]
    public async Task every_statement_of_an_update_and_its_rollback_is_one_statement()
    {
        await ApplyAsync(states(), people());

        var changed = people();
        changed.AddColumn<int>("age").DefaultValue(0);
        changed.Indexes.Clear();
        changed.ModifyColumn("age").AddIndex();

        var delta = await changed.FindDeltaAsync(theConnection);
        var update = new StringWriter();
        delta.WriteUpdate(new FirebirdMigrator(), update);
        var rollback = new StringWriter();
        delta.WriteRollback(new FirebirdMigrator(), rollback);

        FirebirdScript.Split(update.ToString()).Count.ShouldBe(3);
        FirebirdScript.Split(rollback.ToString()).Count.ShouldBe(3);
    }

    /// <summary>
    ///     Each statement commits on its own, so a failure part way leaves the statements before it in
    ///     place -- and the attachment usable, because the failed statement's transaction is rolled back
    ///     rather than left open.
    /// </summary>
    [Fact]
    public async Task a_failed_statement_leaves_earlier_ones_committed_and_the_connection_usable()
    {
        var first = new Table("first_one");
        first.AddColumn<int>("id").AsPrimaryKey();

        var second = new Table("second_one");
        second.AddColumn<int>("id").AsPrimaryKey();
        second.AddColumn("x", "NOT_A_TYPE");

        await Should.ThrowAsync<FirebirdSql.Data.FirebirdClient.FbException>(() => ApplyAsync(first, second));

        (await first.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await second.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();

        await ExecuteAsync("CREATE TABLE afterwards (id INTEGER)");
        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'AFTERWARDS'")).ShouldBe(1);
    }

    [Fact]
    public async Task a_logger_that_is_not_the_default_is_told_of_a_failure_instead()
    {
        var model = new Table("people");
        model.AddColumn<int>("id").AsPrimaryKey();
        model.AddColumn<int>("elsewhere_id").ForeignKeyTo("nowhere", "id");

        var logger = new RecordingLogger();
        var migrator = new FirebirdMigrator();
        var migration = await SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None, model);

        await migrator.ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate, logger);

        logger.Failures.Single().ShouldBeOfType<FirebirdSql.Data.FirebirdClient.FbException>();
        (await model.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue("the statements before the failure ran");
    }

    /// <summary>
    ///     A3: the patch file is the isql form -- SET TERM around PSQL, COMMIT after every statement --
    ///     and runs as a script.
    /// </summary>
    [Fact]
    public async Task a_migration_file_is_an_isql_script_with_a_commit_after_every_statement()
    {
        var migration = await DetermineAsync(states(), people());
        var file = Path.Combine(Path.GetTempPath(), $"weasel-firebird-{Guid.NewGuid():N}.sql");

        try
        {
            await new FirebirdMigrator().WriteMigrationFileAsync(file, migration);

            var script = await File.ReadAllTextAsync(file);
            script.ShouldStartWith("SET TERM ^ ;");

            var statements = FirebirdScript.Split(script);
            statements.Count.ShouldBe(8);
            statements.Where((_, i) => i % 2 == 1).ShouldAllBe(x => x == "COMMIT");

            await ExecuteAsync(script);
            (await DetermineAsync(states(), people())).Difference.ShouldBe(SchemaPatchDifference.None);

            var drop = await File.ReadAllTextAsync(SchemaMigration.ToDropFileName(file));
            await ExecuteAsync(drop);
            (await people().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        }
        finally
        {
            File.Delete(file);
            File.Delete(SchemaMigration.ToDropFileName(file));
        }
    }

    /// <summary>
    ///     weasel#620: every CREATE and ADD is guarded, so a patch runs again cleanly.
    /// </summary>
    [Fact]
    public async Task a_patch_runs_twice()
    {
        var migration = await DetermineAsync(states(), people());
        var writer = new StringWriter();
        new FirebirdMigrator().WriteScript(writer,
            (m, w) => migration.WriteAllUpdates(w, m, AutoCreate.CreateOrUpdate));

        await ExecuteAsync(writer.ToString());
        await ExecuteAsync(writer.ToString());

        (await DetermineAsync(states(), people())).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_update_patch_runs_twice()
    {
        await ApplyAsync(states(), people());

        var changed = people();
        changed.AddColumn<int>("age");
        changed.ModifyColumn("age").AddIndex();
        changed.ForeignKeys.Clear();

        var migration = await DetermineAsync(changed);
        var writer = new StringWriter();
        new FirebirdMigrator().WriteScript(writer,
            (m, w) => migration.WriteAllUpdates(w, m, AutoCreate.CreateOrUpdate));

        await ExecuteAsync(writer.ToString());
        await ExecuteAsync(writer.ToString());

        (await DetermineAsync(changed)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
