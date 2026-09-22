using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     weasel#600. <see cref="Migrator.WriteUpdate" />'s <see cref="SchemaPatchDifference.Invalid" />
///     branch writes a DROP followed by a CREATE, which for a table is the table's data. It is the
///     only branch in the migrator that destroys anything, it is only reachable under
///     <see cref="AutoCreate.All" />, and nothing at runtime said it was about to happen -- the
///     migration logger printed the DDL as it executed and that was the whole warning.
/// </summary>
public class destructive_change_warnings
{
    private sealed class FakeObject: ISchemaObject
    {
        public DbObjectName Identifier { get; } = new("things", "documents");
        public void WriteCreateStatement(Migrator migrator, TextWriter writer) => writer.WriteLine("CREATE TABLE things.documents;");
        public void WriteDropStatement(Migrator rules, TextWriter writer) => writer.WriteLine("DROP TABLE things.documents;");
        public void ConfigureQueryCommand(DbCommandBuilder builder) => throw new NotSupportedException();
        public Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default) => throw new NotSupportedException();
        public IEnumerable<DbObjectName> AllNames() => [Identifier];
    }

    private class FakeDelta: ISchemaObjectDelta
    {
        public FakeDelta(SchemaPatchDifference difference) => Difference = difference;

        public ISchemaObject SchemaObject { get; } = new FakeObject();
        public SchemaPatchDifference Difference { get; }
        public void WriteUpdate(Migrator rules, TextWriter writer) => writer.WriteLine("-- rebuilt in place");
        public void WriteRollback(Migrator rules, TextWriter writer) { }
        public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer) { }
        public override string ToString() => "FakeDelta for things.documents";
    }

    private sealed class ReasonedDelta: FakeDelta, ISchemaObjectDeltaWithReason
    {
        public ReasonedDelta(): base(SchemaPatchDifference.Invalid)
        {
        }

        public string? InvalidReason => "column 'amount' cannot be altered in place";
    }

    private sealed class RebuildableDelta: FakeDelta, ISchemaObjectDeltaWithRebuild
    {
        public RebuildableDelta(): base(SchemaPatchDifference.Invalid)
        {
        }

        public bool CanRebuildInPlace => true;
    }

    private sealed class RecordingLogger: IMigrationLogger
    {
        public List<string> Events { get; } = [];
        public void SchemaChange(string sql) => Events.Add("SQL: " + sql);
        public void OnFailure(DbCommand command, Exception ex) => throw ex;
        public void DestructiveChange(string description) => Events.Add("WARN: " + description);
    }

    private sealed class RecordingMigrator: TestMigrator
    {
        public IMigrationLogger? Logger { get; private set; }

        protected override Task executeDelta(SchemaMigration migration, DbConnection conn, AutoCreate autoCreate,
            IMigrationLogger logger, CancellationToken ct = default)
        {
            Logger = logger;
            logger.SchemaChange("DROP TABLE things.documents;");
            return Task.CompletedTask;
        }
    }

    [Fact]
    public void a_rebuildable_delta_is_not_a_destructive_change()
    {
        DestructiveChange.DropsAndRecreates(new RebuildableDelta()).ShouldBeFalse();
        DestructiveChange.DropsAndRecreates(new FakeDelta(SchemaPatchDifference.Update)).ShouldBeFalse();
        DestructiveChange.DropsAndRecreates(new FakeDelta(SchemaPatchDifference.Invalid)).ShouldBeTrue();
    }

    [Fact]
    public void the_warning_names_the_object_the_reason_and_the_way_out()
    {
        var description = DestructiveChange.Describe(new ReasonedDelta());

        description.ShouldContain("things.documents");
        description.ShouldContain("column 'amount' cannot be altered in place");
        description.ShouldContain("any rows in it will be lost");
        description.ShouldContain("db-patch");
        description.ShouldContain("AutoCreate.CreateOrUpdate");
    }

    [Fact]
    public void a_delta_with_no_reason_still_gets_a_description()
    {
        DestructiveChange.Describe(new FakeDelta(SchemaPatchDifference.Invalid))
            .ShouldContain("the change cannot be expressed as an ALTER");
    }

    [Fact]
    public async Task the_warning_is_logged_before_any_statement_runs()
    {
        var migrator = new RecordingMigrator();
        var logger = new RecordingLogger();

        await migrator.ApplyAllAsync(
            new FakeConnection(), new SchemaMigration(new ReasonedDelta()), AutoCreate.All, logger);

        // Before, not alongside: the DDL used to be the only notice, and by then the DROP had run.
        logger.Events[0].ShouldStartWith("WARN: ");
        logger.Events[0].ShouldContain("things.documents");
        logger.Events[1].ShouldStartWith("SQL: ");
    }

    [Fact]
    public async Task nothing_is_warned_about_a_delta_that_rebuilds_in_place()
    {
        var logger = new RecordingLogger();

        await new RecordingMigrator().ApplyAllAsync(
            new FakeConnection(), new SchemaMigration(new RebuildableDelta()), AutoCreate.All, logger);

        logger.Events.ShouldNotContain(x => x.StartsWith("WARN: "));
    }

    [Fact]
    public void refuse_destructive_changes_turns_the_drop_into_a_refusal()
    {
        var migrator = new TestMigrator { RefuseDestructiveChanges = true };

        var ex = Should.Throw<SchemaMigrationException>(
            () => migrator.WriteUpdate(new StringWriter(), new ReasonedDelta()));

        ex.Message.ShouldContain("things.documents");
        ex.Message.ShouldContain("column 'amount' cannot be altered in place");
        ex.Message.ShouldContain(nameof(Migrator.RefuseDestructiveChanges));
    }

    /// <summary>
    ///     The flag is about data loss, not about Invalid. A delta that can rebuild in place loses
    ///     nothing, so it is still applied.
    /// </summary>
    [Fact]
    public void refuse_destructive_changes_still_allows_a_rebuild_in_place()
    {
        var writer = new StringWriter();

        new TestMigrator { RefuseDestructiveChanges = true }.WriteUpdate(writer, new RebuildableDelta());

        writer.ToString().ShouldContain("rebuilt in place");
    }

    [Fact]
    public void off_by_default()
    {
        var writer = new StringWriter();
        new TestMigrator().RefuseDestructiveChanges.ShouldBeFalse();

        new TestMigrator().WriteUpdate(writer, new ReasonedDelta());

        writer.ToString().ShouldContain("DROP TABLE");
        writer.ToString().ShouldContain("CREATE TABLE");
    }

    /// <summary>
    ///     The refusal every other AutoCreate already produced named the objects and not the
    ///     reason, so the reader had to diff the table by hand to find which change was stuck.
    /// </summary>
    [Fact]
    public void the_refusal_message_carries_each_reason()
    {
        var message = Should.Throw<SchemaMigrationException>(
                () => new SchemaMigration(new ReasonedDelta()).AssertPatchingIsValid(AutoCreate.CreateOrUpdate))
            .Message;

        message.ShouldContain("FakeDelta for things.documents");
        message.ShouldContain("column 'amount' cannot be altered in place");
    }

    private sealed class FakeConnection: DbConnection
    {
        public override string ConnectionString { get; set; } = "";
        public override string Database => "things";
        public override string DataSource => "localhost";
        public override string ServerVersion => "0";
        public override System.Data.ConnectionState State => System.Data.ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        protected override DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    private class TestMigrator: Migrator
    {
        public TestMigrator(): base("things")
        {
        }

        public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null) =>
            throw new NotSupportedException();

        public override bool MatchesConnection(DbConnection connection) => throw new NotSupportedException();
        public override IDatabaseProvider Provider => throw new NotSupportedException();
        public override ITable CreateTable(DbObjectName identifier) => throw new NotSupportedException();

        public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep) =>
            throw new NotSupportedException();

        public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer) { }
        public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer) { }

        protected override Task executeDelta(SchemaMigration migration, DbConnection conn, AutoCreate autoCreate,
            IMigrationLogger logger, CancellationToken ct = default) => Task.CompletedTask;

        public override string ToExecuteScriptLine(string scriptName) => throw new NotSupportedException();
        public override void AssertValidIdentifier(string name) { }

        public override string GenerateDeleteAllSql(IReadOnlyList<DbObjectName> tables, bool resetIdentity = true) =>
            throw new NotSupportedException();
    }
}
