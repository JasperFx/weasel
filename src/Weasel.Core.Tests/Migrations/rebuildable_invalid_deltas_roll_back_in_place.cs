using System.Data.Common;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests.Migrations;

/// <summary>
///     <see cref="SchemaMigration.WriteAllRollbacks" /> answered every
///     <see cref="SchemaPatchDifference.Invalid" /> delta by dropping the object and recreating its
///     previous shape, without asking whether the forward migration had rebuilt it in place.
/// </summary>
/// <remarks>
///     weasel#477 taught the apply paths to honour
///     <see cref="ISchemaObjectDeltaWithRebuild.CanRebuildInPlace" />, and weasel#538 the gate above
///     them. The rollback was the one path left, so the drop file <c>db-patch</c> writes for a rebuilt
///     table recreated it empty. The end-to-end proof is in
///     <c>Weasel.Sqlite.Tests/Tables/rebuild_rollback_keeps_the_rows.cs</c>.
/// </remarks>
public class rebuildable_invalid_deltas_roll_back_in_place
{
    private static string rollbackOf(ISchemaObjectDelta delta)
    {
        var writer = new StringWriter();
        new SchemaMigration(delta).WriteAllRollbacks(writer, null!);
        return writer.ToString();
    }

    [Fact]
    public void a_rebuilt_object_is_rolled_back_by_its_own_reverse_rebuild()
    {
        var rollback = rollbackOf(new RebuildingDelta(canRebuildInPlace: true));

        rollback.ShouldContain("reverse rebuild");
        rollback.ShouldNotContain("drop");
        rollback.ShouldNotContain("create previous");
    }

    /// <summary>
    ///     Narrowed, not changed: an <c>Invalid</c> delta that cannot rebuild was dropped and created
    ///     going forward, so going back is still the drop and the previous shape.
    /// </summary>
    [Fact]
    public void an_invalid_delta_that_cannot_rebuild_is_still_dropped_and_recreated()
    {
        var rollback = rollbackOf(new RebuildingDelta(canRebuildInPlace: false));

        rollback.ShouldContain("drop");
        rollback.ShouldContain("create previous");
        rollback.ShouldNotContain("reverse rebuild");
    }

    private class RebuildingDelta: ISchemaObjectDeltaWithRebuild
    {
        public RebuildingDelta(bool canRebuildInPlace)
        {
            CanRebuildInPlace = canRebuildInPlace;
        }

        public bool CanRebuildInPlace { get; }
        public ISchemaObject SchemaObject { get; } = new DroppableObject();
        public SchemaPatchDifference Difference => SchemaPatchDifference.Invalid;

        public void WriteUpdate(Migrator rules, TextWriter writer) => writer.WriteLine("rebuild");
        public void WriteRollback(Migrator rules, TextWriter writer) => writer.WriteLine("reverse rebuild");

        public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
            => writer.WriteLine("create previous");
    }

    private class DroppableObject: ISchemaObject
    {
        public DbObjectName Identifier { get; } = new("public", "things");

        public void WriteCreateStatement(Migrator migrator, TextWriter writer) => writer.WriteLine("create");
        public void WriteDropStatement(Migrator rules, TextWriter writer) => writer.WriteLine("drop");
        public void ConfigureQueryCommand(DbCommandBuilder builder) { }

        public Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;
        }
    }
}
