using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     weasel#598. A role that lacks privilege used to get the raw provider exception and nothing
///     else -- a SQLSTATE in an inner exception, next to DDL the migration logger had already
///     printed. These cover the neutral half: the message shape, and the rule that a failure the
///     provider does not recognise as a permission refusal is handed back untouched so the caller
///     can still <c>throw;</c> with its original stack.
/// </summary>
public class insufficient_privilege_translation
{
    private class PrivilegeMigrator: Migrator
    {
        private readonly bool _isPrivilege;

        public PrivilegeMigrator(bool isPrivilege): base("fake")
        {
            _isPrivilege = isPrivilege;
        }

        public override bool IsInsufficientPrivilege(Exception exception) => _isPrivilege;

        public Exception Translate(DbConnection conn, string statement, Exception e)
            => TranslateMigrationFailure(conn, statement, e);

        public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null) =>
            throw new NotSupportedException();

        public override bool MatchesConnection(DbConnection connection) => throw new NotSupportedException();
        public override IDatabaseProvider Provider => throw new NotSupportedException();
        public override ITable CreateTable(DbObjectName identifier) => throw new NotSupportedException();

        public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep) =>
            throw new NotSupportedException();

        public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer) =>
            throw new NotSupportedException();

        public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer) =>
            throw new NotSupportedException();

        protected override Task executeDelta(SchemaMigration migration, DbConnection conn, AutoCreate autoCreate,
            IMigrationLogger logger, CancellationToken ct = default) => throw new NotSupportedException();

        public override string ToExecuteScriptLine(string scriptName) => throw new NotSupportedException();
        public override void AssertValidIdentifier(string name) => throw new NotSupportedException();

        public override string GenerateDeleteAllSql(IReadOnlyList<DbObjectName> tables, bool resetIdentity = true) =>
            throw new NotSupportedException();
    }

    private sealed class FakeConnection: DbConnection
    {
        public override string ConnectionString { get; set; } =
            "Host=localhost;Database=things;Username=app_writer;Password=nope";

        public override string Database => "things";
        public override string DataSource => "localhost";
        public override string ServerVersion => "0";
        public override ConnectionState State => ConnectionState.Closed;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    [Fact]
    public void a_failure_the_provider_does_not_recognise_comes_back_unchanged()
    {
        var original = new Exception("relation does not exist");

        new PrivilegeMigrator(false).Translate(new FakeConnection(), "create table foo ()", original)
            .ShouldBeSameAs(original);
    }

    [Fact]
    public void a_permission_failure_is_named_and_keeps_the_original()
    {
        var original = new Exception("permission denied for schema things");

        var translated = new PrivilegeMigrator(true)
            .Translate(new FakeConnection(), "create table things.foo ()", original)
            .ShouldBeOfType<InsufficientDatabasePrivilegeException>();

        translated.InnerException.ShouldBeSameAs(original);
        translated.Role.ShouldBe("app_writer");
        translated.Database.ShouldBe("things");
        translated.Statement.ShouldBe("create table things.foo ()");
    }

    [Fact]
    public void the_message_names_the_role_the_statement_and_both_remedies()
    {
        var message = InsufficientDatabasePrivilegeException.ToMessage(
            "app_writer", "things", "create table things.foo ()", "permission denied for schema things");

        message.ShouldContain("app_writer");
        message.ShouldContain("things");
        message.ShouldContain("permission denied for schema things");
        message.ShouldContain("create table things.foo ()");

        // The two remedies, because which one applies is a deployment decision.
        message.ShouldContain("grant the role");
        message.ShouldContain("db-patch");
        message.ShouldContain("AutoCreate.None");
    }

    [Fact]
    public void the_message_still_reads_when_there_is_no_role_to_name()
    {
        // Integrated / managed-identity authentication: there is no user name in the connection
        // string, and saying "The role ''" would be worse than not naming one.
        var message = InsufficientDatabasePrivilegeException.ToMessage(
            null, null, null, "permission denied");

        message.ShouldStartWith("The connection's role does not have permission");
        message.ShouldNotContain("Statement:");
    }
}
