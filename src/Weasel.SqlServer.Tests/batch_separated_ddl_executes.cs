using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     Rendered DDL that carries a <c>GO</c> separator has to reach SqlClient one batch at a time.
///     Handed over whole, SqlClient raises "Incorrect syntax near 'GO'" (weasel#593).
/// </summary>
public class batch_separated_ddl_executes: IntegrationContext
{
    public batch_separated_ddl_executes(): base("batches")
    {
    }

    [Fact]
    public async Task create_async_runs_every_batch()
    {
        await ResetSchema();

        await new GoSeparatedSchemaObject().CreateAsync(theConnection);

        await assertBothTablesExist();
    }

    [Fact]
    public async Task apply_all_runs_every_batch()
    {
        await ResetSchema();

        var migration = new SchemaMigration(
            new SchemaObjectDelta(new GoSeparatedSchemaObject(), SchemaPatchDifference.Create));

        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        await assertBothTablesExist();
    }

    [Fact]
    public async Task drop_runs_every_batch()
    {
        await ResetSchema();

        var theObject = new GoSeparatedSchemaObject();
        await theObject.CreateAsync(theConnection);
        await assertBothTablesExist();

        await theObject.Drop(theConnection);

        await assertNoTablesRemain();
    }

    [Fact]
    public async Task rollback_all_runs_every_batch()
    {
        await ResetSchema();

        await new GoSeparatedSchemaObject().CreateAsync(theConnection);
        await assertBothTablesExist();

        // A Create delta rolls back through WriteDropStatement, which is where the GO lives.
        var migration = new SchemaMigration(
            new SchemaObjectDelta(new GoSeparatedSchemaObject(), SchemaPatchDifference.Create));

        await migration.RollbackAllAsync(theConnection, new SqlServerMigrator());

        await assertNoTablesRemain();
    }

    private async Task assertBothTablesExist()
    {
        (await batchesTables()).ShouldBe(["a", "b"]);
    }

    private async Task assertNoTablesRemain()
    {
        (await batchesTables()).ShouldBeEmpty();
    }

    private async Task<string[]> batchesTables()
    {
        var tables = await theConnection.ExistingTables();

        return tables.Where(x => x.Schema.EqualsIgnoreCase("batches"))
            .Select(x => x.Name.ToLowerInvariant())
            .OrderBy(x => x)
            .ToArray();
    }
}

/// <summary>
///     Stands in for a schema object whose definition has to be its own batch, which is what a
///     stored procedure's <c>CREATE OR ALTER</c> needs. Only the writing half is used: the tests
///     apply it rather than diffing it.
/// </summary>
internal class GoSeparatedSchemaObject: ISchemaObject
{
    public DbObjectName Identifier { get; } = new SqlServerObjectName("batches", "a");

    public void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine("create table batches.a (id int);");
        writer.WriteLine("GO");
        writer.WriteLine("create table batches.b (id int);");
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine("drop table if exists batches.b;");
        writer.WriteLine("GO");
        writer.WriteLine("drop table if exists batches.a;");
    }

    public void ConfigureQueryCommand(Weasel.Core.DbCommandBuilder builder)
    {
        throw new NotSupportedException();
    }

    public Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        throw new NotSupportedException();
    }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;
    }
}
