using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     weasel#601. <see cref="IDatabase.AssertDatabaseMatchesConfigurationAsync" /> wrote its DDL
///     with <c>AutoCreate.CreateOrUpdate</c>, and <c>WriteAllUpdates</c> runs
///     <c>AssertPatchingIsValid</c> first -- so a delta that is <c>Invalid</c> and cannot rebuild
///     in place aborted the assert with a <see cref="SchemaMigrationException" /> instead of a
///     <see cref="DatabaseValidationException" />. <c>db-assert</c> then printed the generic
///     "Failed to assert database '{id}'!" headline rather than "does not match the
///     configuration!", and a caller doing <c>catch (DatabaseValidationException)</c> missed the
///     one case where the drift is worst.
/// </summary>
[Collection("assert_unappliable")]
public class assert_reports_unappliable_drift_as_validation_failure: IntegrationContext
{
    public assert_reports_unappliable_drift_as_validation_failure(): base("assert_unappliable")
    {
    }

    /// <summary>
    ///     Adding a NOT NULL column with no default to an existing table cannot be done with
    ///     ALTER TABLE, so the delta is Invalid and there is no rebuild for it on PostgreSQL.
    /// </summary>
    private async Task<DatabaseWithTables> ArrangeUnappliableDriftAsync()
    {
        await ResetSchema();

        var applied = new DatabaseWithTables("assert_unappliable", theDataSource, AutoCreate.All);
        applied.AddTable(new PostgresqlObjectName(SchemaName, "documents"))
            .AddPrimaryKeyColumn("id", typeof(int));
        await applied.ApplyAllConfiguredChangesToDatabaseAsync();

        var changed = new Weasel.Postgresql.Tables.Table(new PostgresqlObjectName(SchemaName, "documents"));
        changed.AddColumn<int>("id").AsPrimaryKey();
        changed.AddColumn<string>("name").NotNull();

        var db = new DatabaseWithTables("assert_unappliable", theDataSource, AutoCreate.None);
        db.AddTable(changed);

        return db;
    }

    [Fact]
    public async Task throws_a_validation_exception_rather_than_a_migration_exception()
    {
        var db = await ArrangeUnappliableDriftAsync();

        (await db.CreateMigrationAsync()).Difference.ShouldBe(SchemaPatchDifference.Invalid);

        // Pre-fix this was a SchemaMigrationException, which is not a DatabaseValidationException,
        // so this call would have failed with the wrong type escaping.
        var ex = await Should.ThrowAsync<DatabaseValidationException>(
            () => db.AssertDatabaseMatchesConfigurationAsync());

        ex.ShouldBeOfType<DatabaseValidationException>();
    }

    [Fact]
    public async Task says_that_the_drift_cannot_be_applied_incrementally_and_keeps_the_refusal()
    {
        var db = await ArrangeUnappliableDriftAsync();

        var ex = await Should.ThrowAsync<DatabaseValidationException>(
            () => db.AssertDatabaseMatchesConfigurationAsync());

        ex.Message.ShouldContain("cannot be applied incrementally");
        ex.InnerException.ShouldBeOfType<SchemaMigrationException>();

        // The assert is a report, not an apply, so showing the drop-and-recreate DDL is fine --
        // and it is the only thing that tells the reader what the drift would actually cost.
        ex.Message.ShouldContain("drop table");
    }

    /// <summary>
    ///     Ordinary drift is unchanged: same exception, same body, no "cannot be applied"
    ///     preamble, and no inner exception.
    /// </summary>
    [Fact]
    public async Task ordinary_drift_still_reads_exactly_as_it_did()
    {
        await ResetSchema();

        var db = new DatabaseWithTables("assert_unappliable", theDataSource, AutoCreate.None);
        db.AddTable(new PostgresqlObjectName(SchemaName, "documents")).AddPrimaryKeyColumn("id", typeof(int));

        var ex = await Should.ThrowAsync<DatabaseValidationException>(
            () => db.AssertDatabaseMatchesConfigurationAsync());

        ex.Message.ShouldContain("These changes detected:");
        ex.Message.ShouldNotContain("cannot be applied incrementally");
        ex.InnerException.ShouldBeNull();
    }
}
