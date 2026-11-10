using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using NSubstitute;
using Shouldly;
using Weasel.Core;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests.StorageHelpers;

public class queued_sql_operation
{
    private readonly IStorageSession theSession = Substitute.For<IStorageSession>();

    private SqliteCommand compile(QueuedSqlOperation operation)
    {
        var builder = new Weasel.Sqlite.CommandBuilder();
        operation.ConfigureCommand(builder, theSession);
        return builder.Compile();
    }

    [Fact]
    public void each_placeholder_becomes_a_parameter_bound_in_order()
    {
        var command = compile(new QueuedSqlOperation("insert into t (a, b, c) values (?, ?, ?);", '?', 1, "two", null));

        command.CommandText.ShouldNotContain("?");
        command.CommandText.ShouldNotEndWith(";");
        command.Parameters.Cast<DbParameter>().Select(x => x.Value).ShouldBe([1, "two", DBNull.Value]);
    }

    [Fact]
    public void another_placeholder_leaves_a_literal_question_mark_alone()
    {
        var command = compile(new QueuedSqlOperation("select '?' where 1 = ^", '^', 1));

        command.CommandText.ShouldContain("'?'");
        command.Parameters.Count.ShouldBe(1);
    }

    [Fact]
    public void a_count_mismatch_is_refused()
    {
        Should.Throw<InvalidOperationException>(() => compile(new QueuedSqlOperation("select ?, ?", '?', 1)))
            .Message.ShouldContain("2 '?' placeholders and 1 values");
    }

    [Fact]
    public void a_store_can_bind_values_its_own_way()
    {
        var command = compile(new AnsiQueuedSql("select ?", "x"));

        command.Parameters[0].DbType.ShouldBe(DbType.AnsiString);
        command.Parameters[0].Value.ShouldBe("x");
    }

    [Fact]
    public void it_names_no_document_type_and_returns_no_data()
    {
        var operation = new QueuedSqlOperation("select 1", '?');

        operation.DocumentType.ShouldBe(typeof(object));
        operation.Role().ShouldBe(OperationRole.Other);
        operation.ShouldBeAssignableTo<NoDataReturnedCall>();

        new QueuedSqlOperation("select 1", '?') { DocumentType = typeof(string) }.DocumentType.ShouldBe(typeof(string));
    }

    private sealed class AnsiQueuedSql(string sql, params object?[] values): QueuedSqlOperation(sql, '?', values)
    {
        protected override void BindParameter(DbParameter parameter, object? value)
        {
            parameter.DbType = DbType.AnsiString;
            parameter.Value = value;
        }
    }
}
