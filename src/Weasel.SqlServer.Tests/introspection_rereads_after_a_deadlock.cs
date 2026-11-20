using System.Data.Common;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     weasel#900. A catalog read that SQL Server picks as a deadlock victim (1205) is read again, through the
///     same re-read <c>SchemaMigration</c> already does for Firebird (weasel#723). It came up in a schema apply at
///     startup while other connections ran DDL against the same server (wolverine#5676).
/// </summary>
/// <remarks>
///     <see cref="SqlException" /> has no public constructor, so the 1205 here is a real one: two connections
///     update two rows in opposite orders, and the one at <c>DEADLOCK_PRIORITY LOW</c> loses. SQL Server's
///     deadlock monitor finds it within about five seconds.
/// </remarks>
public class introspection_rereads_after_a_deadlock
{
    private static CancellationToken ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task a_deadlock_victim_is_a_concurrent_introspection_failure()
    {
        var deadlock = await provokeDeadlockAsync();
        deadlock.Number.ShouldBe(1205);

        var migrator = new SqlServerMigrator();
        migrator.IsConcurrentIntrospectionFailure(deadlock).ShouldBeTrue();

        // A provider's exception transformer may have wrapped it
        migrator.IsConcurrentIntrospectionFailure(new InvalidOperationException("wrapped", deadlock))
            .ShouldBeTrue();
    }

    [Fact]
    public async Task any_other_sql_server_error_is_not()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("select 1 / 0", conn);

        var divideByZero = await Should.ThrowAsync<SqlException>(() => cmd.ExecuteScalarAsync(ct));
        divideByZero.Number.ShouldBe(8134);

        var migrator = new SqlServerMigrator();
        migrator.IsConcurrentIntrospectionFailure(divideByZero).ShouldBeFalse();
        migrator.IsConcurrentIntrospectionFailure(new InvalidOperationException("wrapped", divideByZero))
            .ShouldBeFalse();
        migrator.IsConcurrentIntrospectionFailure(new TimeoutException()).ShouldBeFalse();
    }

    [Fact]
    public async Task a_catalog_read_that_lost_a_deadlock_is_read_again()
    {
        var deadlock = await provokeDeadlockAsync();

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(ct);
        var thing = new DeadlockedSchemaObject("weasel_900_thing", deadlock, failures: 1);

        var migration = await SchemaMigration.DetermineAsync(conn, new SqlServerMigrator(), ct, thing);

        migration.Deltas.Count.ShouldBe(1);
        thing.Attempts.ShouldBe(2, "the deadlocked read should have been made a second time");
    }

    [Fact]
    public async Task a_catalog_read_that_fails_otherwise_is_read_once()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(ct);
        var thing = new DeadlockedSchemaObject("weasel_900_thing", new InvalidOperationException("boom"),
            failures: 1);

        await Should.ThrowAsync<InvalidOperationException>(() =>
            SchemaMigration.DetermineAsync(conn, new SqlServerMigrator(), ct, thing));

        thing.Attempts.ShouldBe(1);
    }

    /// <summary>
    ///     A real 1205. The victim and the survivor each lock one row, then ask for the other's.
    /// </summary>
    private static async Task<SqlException> provokeDeadlockAsync()
    {
        var table = $"dbo.weasel_900_deadlock_{Guid.NewGuid():N}";

        await using var admin = new SqlConnection(ConnectionSource.ConnectionString);
        await admin.OpenAsync(ct);
        await executeAsync(admin, null,
            $"create table {table} (id int primary key, value int not null); insert into {table} values (1, 0), (2, 0);");

        try
        {
            await using var victim = new SqlConnection(ConnectionSource.ConnectionString);
            await victim.OpenAsync(ct);
            await executeAsync(victim, null, "set deadlock_priority low;");

            await using var survivor = new SqlConnection(ConnectionSource.ConnectionString);
            await survivor.OpenAsync(ct);

            await using var victimTx = (SqlTransaction)await victim.BeginTransactionAsync(ct);
            await using var survivorTx = (SqlTransaction)await survivor.BeginTransactionAsync(ct);

            await executeAsync(victim, victimTx, $"update {table} set value = 1 where id = 1;");
            await executeAsync(survivor, survivorTx, $"update {table} set value = 1 where id = 2;");

            var victimWaits = executeAsync(victim, victimTx, $"update {table} set value = 2 where id = 2;");
            var survivorWaits = executeAsync(survivor, survivorTx, $"update {table} set value = 2 where id = 1;");

            var deadlock = await Should.ThrowAsync<SqlException>(() => victimWaits);

            // SQL Server rolled the victim back, so the survivor's update goes through
            await survivorWaits;
            await survivorTx.RollbackAsync(ct);

            return deadlock;
        }
        finally
        {
            await executeAsync(admin, null, $"drop table if exists {table};");
        }
    }

    private static async Task executeAsync(SqlConnection conn, SqlTransaction? tx, string sql)
    {
        await using var cmd = new SqlCommand(sql, conn, tx);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    ///     A real schema object whose first <paramref name="failures" /> catalog reads fail with
    ///     <paramref name="failure" />: the query runs, and reading the result throws.
    /// </summary>
    private class DeadlockedSchemaObject(string name, Exception failure, int failures): ISchemaObject
    {
        public int Attempts { get; private set; }

        public DbObjectName Identifier { get; } = new SqlServerObjectName("dbo", name);

        public void ConfigureQueryCommand(Weasel.Core.DbCommandBuilder builder)
        {
            var param = builder.AddParameter(Identifier.Name).ParameterName;
            builder.Append($"select count(*) from sys.tables where name = @{param};");
        }

        public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
        {
            Attempts++;

            await reader.ReadAsync(ct);
            await reader.GetFieldValueAsync<int>(0, ct);

            if (Attempts <= failures)
            {
                throw failure;
            }

            return new SchemaObjectDelta(this, SchemaPatchDifference.None);
        }

        public void WriteCreateStatement(Migrator migrator, TextWriter writer) => throw new NotSupportedException();
        public void WriteDropStatement(Migrator rules, TextWriter writer) => throw new NotSupportedException();

        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;
        }
    }
}
