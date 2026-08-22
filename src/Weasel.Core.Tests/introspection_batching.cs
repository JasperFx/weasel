using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     Introspection queries are grouped so that no single command binds more parameters than the
///     driver accepts. SQL Server refuses a request carrying more than 2100, and a table's query
///     binds two, so a database of more than 1050 tables had every object's query put into one
///     command and rejected outright, before any comparison happened.
/// </summary>
public class introspection_batching
{
    private static ISchemaObject[] objects(int count)
        => Enumerable.Range(0, count).Select(i => (ISchemaObject)new FakeSchemaObject(i)).ToArray();

    private static IReadOnlyList<ISchemaObject[]> batch(int count, int costEach, int budget)
        => SchemaMigration.BatchByParameterBudget(objects(count), _ => costEach, budget).ToList();

    [Fact]
    public void no_batch_exceeds_the_budget()
    {
        var batches = batch(1100, 2, 2000);

        batches.ShouldAllBe(b => b.Length * 2 <= 2000);
    }

    [Fact]
    public void every_object_lands_in_exactly_one_batch_in_order()
    {
        var all = objects(1100);

        var flattened = SchemaMigration.BatchByParameterBudget(all, _ => 2, 2000)
            .SelectMany(x => x).ToArray();

        flattened.ShouldBe(all);
    }

    [Fact]
    public void a_set_that_fits_is_left_as_one_batch()
    {
        batch(500, 2, 2000).Count.ShouldBe(1);
    }

    [Fact]
    public void an_object_costing_more_than_the_whole_budget_gets_its_own_batch()
    {
        // The guard against yielding an empty batch forever. Such an object cannot be made to fit,
        // so it is sent on its own and the driver decides.
        var batches = SchemaMigration.BatchByParameterBudget(objects(3), _ => 5000, 2000);

        batches.Select(x => x.Length).ShouldBe(new[] { 1, 1, 1 });
    }

    [Fact]
    public void an_object_that_binds_nothing_never_forces_a_split()
    {
        // SQLite interpolates rather than binding, so every object prices at zero.
        batch(10_000, 0, 2000).Count.ShouldBe(1);
    }

    [Fact]
    public void a_dialect_that_does_not_answer_falls_back_to_the_default_budget()
    {
        // A Migrator test double reports 0 for MaxParametersPerCommand. Taking that literally would
        // send every object in its own command.
        batch(500, 2, 0).Count.ShouldBe(1);
    }

    [Fact]
    public void no_objects_is_no_batches()
    {
        SchemaMigration.BatchByParameterBudget([], _ => 2, 2000).ShouldBeEmpty();
    }

    private class FakeSchemaObject(int index): ISchemaObject
    {
        public DbObjectName Identifier { get; } = new("dbo", $"thing{index}");

        public void WriteCreateStatement(Migrator migrator, TextWriter writer) => throw new NotSupportedException();
        public void WriteDropStatement(Migrator rules, TextWriter writer) => throw new NotSupportedException();
        public void ConfigureQueryCommand(DbCommandBuilder builder) => throw new NotSupportedException();

        public Task<ISchemaObjectDelta> CreateDeltaAsync(System.Data.Common.DbDataReader reader,
            CancellationToken ct = default) => throw new NotSupportedException();

        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;
        }
    }
}
