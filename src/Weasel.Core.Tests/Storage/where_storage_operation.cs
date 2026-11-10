using System.Data;
using System.Data.Common;
using NSubstitute;
using Shouldly;
using Weasel.Core;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests.StorageHelpers;

public class where_storage_operation
{
    private readonly IStorageSession theSession = Substitute.For<IStorageSession>();

    private sealed class Head(string sql, OperationRole role): IOperationFragment
    {
        public void Apply(ICommandBuilder builder) => builder.Append(sql);
        public OperationRole Role() => role;
    }

    private sealed class Predicate(string sql): Weasel.Core.SqlGeneration.ISqlFragment
    {
        public void Apply(ICommandBuilder builder) => builder.Append(sql);
    }

    private (string Sql, DbParameter[] Parameters) render(WhereStorageOperation operation)
    {
        var builder = new Weasel.Sqlite.CommandBuilder();
        operation.ConfigureCommand(builder, theSession);
        var command = builder.Compile();
        return (command.CommandText, command.Parameters.Cast<DbParameter>().ToArray());
    }

    [Fact]
    public void tenant_then_guard_then_the_caller_s_predicate()
    {
        var (sql, parameters) = render(new WhereStorageOperation(new Head("update docs set is_deleted = 1", OperationRole.Deletion),
            typeof(string), new Predicate("(a = 1 or b = 2)"), "tenant-a", "is_deleted = 0", OperationRole.Deletion));

        sql.ShouldStartWith("update docs set is_deleted = 1 where tenant_id = ");
        sql.ShouldEndWith(" and is_deleted = 0 and (a = 1 or b = 2)");
        parameters.ShouldHaveSingleItem().Value.ShouldBe("tenant-a");
    }

    [Fact]
    public void a_single_tenant_hard_delete_is_just_the_predicate()
    {
        var operation = new WhereStorageOperation(new Head("delete from docs", OperationRole.Deletion),
            typeof(string), new Predicate("a = 1"), null, null, OperationRole.Deletion);

        var (sql, parameters) = render(operation);

        sql.ShouldBe("delete from docs where a = 1");
        parameters.ShouldBeEmpty();
        operation.Role().ShouldBe(OperationRole.Deletion);
        operation.DocumentType.ShouldBe(typeof(string));
    }

    [Fact]
    public void the_tenant_column_and_its_binding_belong_to_the_store()
    {
        var (sql, parameters) = render(new VarcharTenant(new Head("delete from docs", OperationRole.Deletion),
            new Predicate("a = 1")) { TenantIdColumn = "d.tenant" });

        sql.ShouldStartWith("delete from docs where d.tenant = ");
        parameters.ShouldHaveSingleItem().DbType.ShouldBe(DbType.AnsiString);
    }

    private sealed class VarcharTenant(IOperationFragment head, Weasel.Core.SqlGeneration.ISqlFragment where)
        : WhereStorageOperation(head, typeof(string), where, "t", null, OperationRole.Deletion)
    {
        protected override DbParameter AppendTenantId(ICommandBuilder builder, string tenantId)
        {
            var parameter = base.AppendTenantId(builder, tenantId);
            parameter.DbType = DbType.AnsiString;
            return parameter;
        }
    }
}
