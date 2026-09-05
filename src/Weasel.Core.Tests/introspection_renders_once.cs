using System.Data.Common;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     Pricing the parameter budget used to be a separate pass: a throwaway
///     <see cref="DbCommandBuilder" /> per schema object, running the object's whole
///     <c>ConfigureQueryCommand</c> just to read <c>Parameters.Count</c> off it, before the real
///     pass rendered everything again (weasel#557). The costs are now recorded off the render
///     that has to happen anyway, so a set that fits the budget renders exactly once, and only a
///     set that genuinely needs splitting renders twice.
///     <para>
///     Runs against an in-memory SQLite connection — a real execution path, no containers.
///     </para>
/// </summary>
public class introspection_renders_once
{
    private static async Task<SqliteConnection> openConnectionAsync()
    {
        var conn = new SqliteConnection("Data Source=:memory:");
        await conn.OpenAsync();
        return conn;
    }

    private static CountingSchemaObject[] objects(int count)
        => Enumerable.Range(0, count).Select(i => new CountingSchemaObject($"thing{i}")).ToArray();

    [Fact]
    public async Task a_set_within_the_budget_configures_every_object_exactly_once()
    {
        await using var conn = await openConnectionAsync();
        var all = objects(10);

        // 10 objects * 1 parameter each, well within the budget
        var migration = await SchemaMigration.DetermineAsync(conn, default, 2000,
            all.Cast<ISchemaObject>().ToArray());

        migration.Deltas.Count.ShouldBe(10);
        all.ShouldAllBe(x => x.ConfigureCount == 1);
    }

    [Fact]
    public async Task a_set_over_the_budget_still_batches_and_configures_at_most_twice()
    {
        await using var conn = await openConnectionAsync();
        var all = objects(10);

        // 1 parameter per object against a budget of 3: forced to split into batches of 3
        var migration = await SchemaMigration.DetermineAsync(conn, default, 3,
            all.Cast<ISchemaObject>().ToArray());

        migration.Deltas.Count.ShouldBe(10);

        // Once for the pricing render that was discarded, once inside its own batch -- never more
        all.ShouldAllBe(x => x.ConfigureCount == 2);
    }

    [Fact]
    public async Task deltas_come_back_in_order_whichever_path_runs()
    {
        await using var conn = await openConnectionAsync();

        foreach (var budget in new[] { 2000, 3 })
        {
            var all = objects(7);

            var migration = await SchemaMigration.DetermineAsync(conn, default, budget,
                all.Cast<ISchemaObject>().ToArray());

            migration.Deltas.Select(x => x.SchemaObject).ShouldBe(all);
        }
    }

    [Fact]
    public void recorded_costs_batch_identically_to_the_priced_func()
    {
        var all = objects(9).Cast<ISchemaObject>().ToArray();
        var costs = new[] { 2, 2, 2, 5, 1, 1, 1, 1, 4 };

        var byArray = SchemaMigration.BatchByParameterBudget(all, costs, 6)
            .Select(b => b.Length).ToArray();
        var byFunc = SchemaMigration.BatchByParameterBudget(all, x => costs[Array.IndexOf(all, x)], 6)
            .Select(b => b.Length).ToArray();

        byArray.ShouldBe(byFunc);

        // 2+2+2 = 6 | 5+1 = 6 | 1+1+1 = 3 (a 4 would overflow) | 4
        byArray.ShouldBe(new[] { 3, 2, 3, 1 });
    }

    /// <summary>
    ///     A real, executable schema object: binds one parameter, reads one count row — the same
    ///     shape as the "exists / does not exist" objects — and counts how many times its query
    ///     was rendered.
    /// </summary>
    private class CountingSchemaObject(string name): ISchemaObject
    {
        public int ConfigureCount { get; private set; }

        public DbObjectName Identifier { get; } = new("main", name);

        public void ConfigureQueryCommand(DbCommandBuilder builder)
        {
            ConfigureCount++;

            var param = builder.AddParameter(Identifier.Name).ParameterName;
            builder.Append($"select count(*) from sqlite_master where name = @{param};");
        }

        public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
        {
            await reader.ReadAsync(ct);
            var count = await reader.GetFieldValueAsync<long>(0, ct);

            return new SchemaObjectDelta(this,
                count == 0 ? SchemaPatchDifference.Create : SchemaPatchDifference.None);
        }

        public void WriteCreateStatement(Migrator migrator, TextWriter writer) => throw new NotSupportedException();
        public void WriteDropStatement(Migrator rules, TextWriter writer) => throw new NotSupportedException();

        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;
        }
    }
}
