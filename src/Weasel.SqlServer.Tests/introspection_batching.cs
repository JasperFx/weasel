using System.Data.Common;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     SQL Server refuses a request carrying more than 2100 parameters. A Table's introspection
///     query binds two, so a database of more than 1050 of them put every object's query into one
///     command and had it rejected outright, before any comparison happened.
/// </summary>
public class introspection_batching: IntegrationContext
{
    public introspection_batching(): base("batching")
    {
    }

    private static ISchemaObject[] tables(int count)
        => Enumerable.Range(0, count).Select(i =>
        {
            var table = new Table($"batching.thing{i}");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("name").AllowNulls();
            return (ISchemaObject)table;
        }).ToArray();

    /// <summary>
    ///     Binds <paramref name="parameterCount" /> parameters against a trivial query, so that the
    ///     server's limit is reached in one round trip rather than by building a thousand tables.
    /// </summary>
    private sealed class BindsManyParameters(int parameterCount): ISchemaObject
    {
        public DbObjectName Identifier { get; } = new("batching", "wide");

        public void ConfigureQueryCommand(DbCommandBuilder builder)
        {
            for (var i = 0; i < parameterCount; i++) builder.AddParameter(i);
            builder.Append("select 1;");
        }

        public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
        {
            await reader.ReadAsync(ct);
            return new SchemaObjectDelta(this, SchemaPatchDifference.None);
        }

        public void WriteCreateStatement(Migrator migrator, TextWriter writer) => throw new NotSupportedException();
        public void WriteDropStatement(Migrator rules, TextWriter writer) => throw new NotSupportedException();

        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;
        }
    }

    private static ISchemaObject[] wideObjects() =>
        Enumerable.Range(0, 6).Select(_ => (ISchemaObject)new BindsManyParameters(400)).ToArray();

    [Fact]
    public async Task one_command_for_everything_is_rejected_by_the_server()
    {
        // 2400 parameters in a single command. This is what the migration path did for every object
        // it had, and it is the overload that still does not batch, because the caller supplies the
        // one builder it can use.
        await ResetSchema();

        var tooMany = await Should.ThrowAsync<SqlException>(async () =>
            await SchemaMigration.DetermineAsync(
                theConnection, new DbCommandBuilder(theConnection), default, wideObjects()));

        // By number, not message text: SQL Server localizes the text.
        tooMany.Number.ShouldBe(8003);
    }

    [Fact]
    public async Task the_same_objects_go_through_once_the_batch_respects_the_limit()
    {
        await ResetSchema();

        var migration = await SchemaMigration.DetermineAsync(
            theConnection, new SqlServerMigrator(), default, wideObjects());

        migration.Deltas.Count.ShouldBe(6);
    }

    [Fact]
    public async Task every_object_is_reported_when_the_batch_has_to_split()
    {
        await ResetSchema();

        // Two parameters per table, so this is one table per batch.
        var migration = await SchemaMigration.DetermineAsync(theConnection, default, 2, tables(10));

        migration.Deltas.Count.ShouldBe(10);
        migration.Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public async Task splitting_reports_what_a_single_batch_reports()
    {
        await ResetSchema();

        // Create half of them, so the run has a mix of Create and None to get wrong.
        foreach (var table in tables(10).Take(5))
        {
            await CreateSchemaObjectInDatabase(table);
        }

        var single = await SchemaMigration.DetermineAsync(theConnection, default, int.MaxValue, tables(10));
        var split = await SchemaMigration.DetermineAsync(theConnection, default, 4, tables(10));

        split.Difference.ShouldBe(single.Difference);
        split.Deltas.Select(x => x.Difference).ShouldBe(single.Deltas.Select(x => x.Difference));
    }

    /// <summary>
    ///     The public <c>MigrateAsync</c> extension takes an unbounded array and reached the same
    ///     one-command-for-everything path, so batching the migration path alone left the bug live on
    ///     the entry point a caller outside Weasel is most likely to hold.
    /// </summary>
    [Fact]
    public async Task the_public_migrate_extension_batches_too()
    {
        await ResetSchema();

        // 2400 parameters. Pre-fix this threw SqlException 8003 out of the introspection query,
        // before any comparison happened. Nothing has drifted, so nothing is migrated.
        var migrated = await wideObjects().MigrateAsync(theConnection);

        migrated.ShouldBeFalse();
    }

    /// <summary>
    ///     And it still applies what it finds. The batching sits in front of the comparison, so a real
    ///     delta has to survive the split and reach the database.
    /// </summary>
    [Fact]
    public async Task the_public_migrate_extension_still_applies_the_delta_it_finds()
    {
        await ResetSchema();

        var expected = tables(3);

        (await expected.MigrateAsync(theConnection)).ShouldBeTrue();

        // Second pass over the same objects: everything is there now, so there is nothing to do.
        (await expected.MigrateAsync(theConnection)).ShouldBeFalse();
    }

    [Fact]
    public void each_dialect_supplies_its_own_limit()
    {
        // SQL Server's 2100 is the tightest of the five; the base default suits the rest and is not
        // dragged down to it.
        new SqlServerMigrator().MaxParametersPerCommand.ShouldBe(2000);
        SchemaMigration.DefaultParameterBudget.ShouldBe(2000);
    }
}
