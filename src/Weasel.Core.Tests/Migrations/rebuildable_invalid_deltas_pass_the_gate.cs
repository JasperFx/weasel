using System.Data.Common;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests.Migrations;

/// <summary>
///     weasel#538: <see cref="SchemaMigration.AssertPatchingIsValid" /> refused every
///     <see cref="SchemaPatchDifference.Invalid" /> delta below <see cref="AutoCreate.All" />
///     without asking whether the delta could actually carry the change out.
/// </summary>
/// <remarks>
///     <para>
///         weasel#477 taught both apply paths — <see cref="SchemaMigration.WriteAllUpdates" /> and
///         <see cref="Migrator.WriteUpdate(TextWriter, ISchemaObjectDelta)" /> — to honour
///         <see cref="ISchemaObjectDeltaWithRebuild.CanRebuildInPlace" /> and rebuild the object
///         instead of dropping it. The gate above them was not, so it rejected migrations the
///         machinery it guards would have applied correctly, and with the data intact.
///     </para>
///     <para>
///         These are at the Core level with fakes because the rule is a Core rule. The end-to-end
///         proof against a real database is in
///         <c>Weasel.Sqlite.Tests/Tables/rebuildable_invalid_is_permitted.cs</c>; SQLite is the only
///         provider implementing the interface today.
///     </para>
/// </remarks>
public class rebuildable_invalid_deltas_pass_the_gate
{
    [Theory]
    [InlineData(AutoCreate.All)]
    [InlineData(AutoCreate.CreateOrUpdate)]
    public void a_rebuildable_invalid_is_permitted(AutoCreate autoCreate)
    {
        var migration = new SchemaMigration(new RebuildableDelta(canRebuildInPlace: true));

        Should.NotThrow(() => migration.AssertPatchingIsValid(autoCreate));
    }

    /// <summary>
    ///     A rebuild recreates an object that is already there, which is an update however you look
    ///     at it, so <see cref="AutoCreate.CreateOnly" /> still refuses. This is the question
    ///     weasel#538 left open; it falls out of the existing <c>CreateOnly</c> branch rather than
    ///     needing a special case, but it is a decision and so it gets a test.
    /// </summary>
    [Fact]
    public void create_only_still_refuses_a_rebuildable_invalid()
    {
        var migration = new SchemaMigration(new RebuildableDelta(canRebuildInPlace: true));

        Should.Throw<SchemaMigrationException>(
            () => migration.AssertPatchingIsValid(AutoCreate.CreateOnly));
    }

    /// <summary>
    ///     The gate is narrowed, not opened: an <c>Invalid</c> delta that cannot rebuild is still
    ///     refused, because for it the apply path really would drop and recreate.
    /// </summary>
    [Theory]
    [InlineData(AutoCreate.CreateOrUpdate)]
    [InlineData(AutoCreate.CreateOnly)]
    public void an_invalid_delta_that_cannot_rebuild_is_still_refused(AutoCreate autoCreate)
    {
        var migration = new SchemaMigration(new RebuildableDelta(canRebuildInPlace: false));

        Should.Throw<SchemaMigrationException>(
            () => migration.AssertPatchingIsValid(autoCreate));
    }

    /// <summary>
    ///     A delta that does not implement the interface at all is the common case and must not have
    ///     moved.
    /// </summary>
    [Fact]
    public void a_plain_invalid_delta_is_still_refused()
    {
        var migration = new SchemaMigration(
            new SchemaObjectDelta(new FakeSchemaObject(), SchemaPatchDifference.Invalid));

        Should.Throw<SchemaMigrationException>(
            () => migration.AssertPatchingIsValid(AutoCreate.CreateOrUpdate));
    }

    /// <summary>
    ///     A migration mixing a rebuildable <c>Invalid</c> with one that cannot rebuild is still
    ///     refused — and the message names only the one that is genuinely stuck.
    /// </summary>
    [Fact]
    public void a_mixed_migration_is_refused_and_names_only_the_stuck_delta()
    {
        var migration = new SchemaMigration(new ISchemaObjectDelta[]
        {
            new RebuildableDelta(canRebuildInPlace: true, name: "rebuildable"),
            new RebuildableDelta(canRebuildInPlace: false, name: "stuck")
        });

        var ex = Should.Throw<SchemaMigrationException>(
            () => migration.AssertPatchingIsValid(AutoCreate.CreateOrUpdate));

        ex.Message.ShouldContain("stuck");
        ex.Message.ShouldNotContain("rebuildable");
    }

    public class RebuildableDelta: ISchemaObjectDeltaWithRebuild
    {
        private readonly string _name;

        public RebuildableDelta(bool canRebuildInPlace, string name = "fake")
        {
            CanRebuildInPlace = canRebuildInPlace;
            _name = name;
            SchemaObject = new FakeSchemaObject(name);
        }

        public bool CanRebuildInPlace { get; }
        public ISchemaObject SchemaObject { get; }
        public SchemaPatchDifference Difference => SchemaPatchDifference.Invalid;

        public void WriteUpdate(Migrator rules, TextWriter writer) => writer.WriteLine("rebuild");
        public void WriteRollback(Migrator rules, TextWriter writer) { }
        public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer) { }

        public override string ToString() => _name;
    }

    public class FakeSchemaObject: ISchemaObject
    {
        public FakeSchemaObject(string name = "fake")
        {
            Identifier = new DbObjectName("public", name);
        }

        public DbObjectName Identifier { get; }

        public void WriteCreateStatement(Migrator migrator, TextWriter writer) { }
        public void WriteDropStatement(Migrator rules, TextWriter writer) { }
        public void ConfigureQueryCommand(DbCommandBuilder builder) { }

        public Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;
        }
    }
}
