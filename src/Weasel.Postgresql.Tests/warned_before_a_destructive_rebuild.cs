using System.Data.Common;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     weasel#600, end to end against a real table with rows in it. Adding a NOT NULL column with
///     no default to an existing table cannot be done with <c>ALTER TABLE</c>, so the delta is
///     <see cref="SchemaPatchDifference.Invalid" /> and <c>AutoCreate.All</c> answers it by
///     dropping the table and creating it again -- taking the rows. That is the migrator's only
///     data-destroying branch, and it used to happen with no notice but the DDL appearing as it
///     ran.
/// </summary>
[Collection("destructive_rebuild")]
public class warned_before_a_destructive_rebuild: IntegrationContext
{
    public warned_before_a_destructive_rebuild(): base("destructive_rebuild")
    {
    }

    private sealed class RecordingLogger: IMigrationLogger
    {
        public List<string> Warnings { get; } = [];
        public List<string> Statements { get; } = [];

        public void SchemaChange(string sql) => Statements.Add(sql);
        public void OnFailure(DbCommand command, Exception ex) => throw ex;
        public void DestructiveChange(string description) => Warnings.Add(description);
    }

    private async Task<SchemaMigration> ArrangeRebuildAsync()
    {
        await ResetSchema();

        var initial = new Table(new PostgresqlObjectName(SchemaName, "documents"));
        initial.AddColumn<int>("id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(initial);

        await theConnection.CreateCommand($"insert into {SchemaName}.documents (id) values (1)")
            .ExecuteNonQueryAsync();

        var changed = new Table(new PostgresqlObjectName(SchemaName, "documents"));
        changed.AddColumn<int>("id").AsPrimaryKey();
        changed.AddColumn<string>("name").NotNull();

        return await SchemaMigration.DetermineAsync(theConnection, changed);
    }

    [Fact]
    public async Task the_delta_says_why_it_cannot_be_applied_incrementally()
    {
        var migration = await ArrangeRebuildAsync();

        migration.Difference.ShouldBe(SchemaPatchDifference.Invalid);

        var delta = migration.Deltas.OfType<TableDelta>().Single();
        delta.InvalidReason.ShouldBe("column 'name' cannot be added to an existing table");
    }

    [Fact]
    public async Task warns_before_running_the_drop()
    {
        var migration = await ArrangeRebuildAsync();
        var logger = new RecordingLogger();

        await new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.All, logger);

        var warning = logger.Warnings.ShouldHaveSingleItem();
        warning.ShouldContain($"{SchemaName}.documents");
        warning.ShouldContain("column 'name' cannot be added to an existing table");
        warning.ShouldContain("any rows in it will be lost");

        // Which they were -- this is what the warning is for.
        var count = await theConnection.CreateCommand($"select count(*) from {SchemaName}.documents")
            .ExecuteScalarAsync();
        Convert.ToInt32(count).ShouldBe(0);
    }

    [Fact]
    public async Task refuses_the_rebuild_when_the_migrator_is_told_to()
    {
        var migration = await ArrangeRebuildAsync();

        var migrator = new PostgresqlMigrator { RefuseDestructiveChanges = true };

        var ex = await Should.ThrowAsync<SchemaMigrationException>(
            () => migrator.ApplyAllAsync(theConnection, migration, AutoCreate.All, new RecordingLogger()));

        ex.Message.ShouldContain("column 'name' cannot be added to an existing table");

        // And the row is still there, which is the whole point of the flag.
        var count = await theConnection.CreateCommand($"select count(*) from {SchemaName}.documents")
            .ExecuteScalarAsync();
        Convert.ToInt32(count).ShouldBe(1);
    }
}
