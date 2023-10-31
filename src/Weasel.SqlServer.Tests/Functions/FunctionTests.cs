using JasperFx.Core.Reflection;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Functions;
using Xunit;

namespace Weasel.SqlServer.Tests.Functions;

[Collection("functions")]
public class FunctionTests: IntegrationContext
{
    private readonly string theFunctionBody = @"
CREATE OR ALTER FUNCTION functions.add_10(@value int) RETURNS int AS
BEGIN
    return @value + 10;
END;";

    private readonly string theDifferentBody = @"
CREATE OR ALTER FUNCTION functions.add_10(@value int) RETURNS int AS
BEGIN
    return @value + 20;
END;";

    private readonly string theEvenDifferentBody = @"
CREATE OR ALTER FUNCTION functions.add_10(@value int) RETURNS int AS
BEGIN
    return @value + 30;
END;";

    public FunctionTests(): base("functions")
    {
    }

    protected async Task AssertNoDeltasAfterPatching(Function func)
    {
        await func.ApplyChangesAsync(theConnection);

        var delta = await func.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void for_removal()
    {
        var function = Function.ForRemoval("functions.add_10");
        function.IsRemoved.ShouldBeTrue();
        function.Identifier.QualifiedName.ShouldBe("functions.add_10");
    }

    [Fact]
    public void can_read_the_function_identifier_from_a_function_body()
    {
        Function.ParseIdentifier(theFunctionBody)
            .ShouldBe(new SqlServerObjectName("functions", "add_10"));
    }

    [Fact]
    public void can_derive_the_drop_statement_from_the_body()
    {
        var function = new Function(new SqlServerObjectName("functions", "add_10"), theFunctionBody);
        function.DropStatements().Single().ShouldBe("drop function if exists functions.add_10;");
    }

    [Fact]
    public void can_build_function_object_from_body()
    {
        var function = Function.ForSql(theFunctionBody);
        function.Identifier.ShouldBe(new SqlServerObjectName("functions", "add_10"));

        function.DropStatements().Single()
            .ShouldBe("drop function if exists functions.add_10;");
    }

    [Fact]
    public async Task can_build_function_in_database()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);

        await CreateSchemaObjectInDatabase(function);

        var next = await theConnection.CreateCommand("select functions.add_10(20);")
            .ExecuteScalarAsync();

        next!.As<int>().ShouldBe(30);

        await theConnection.FunctionExistsAsync(function.Identifier);
    }


    [Fact]
    public async Task existing_functions_extension_method()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);

        await CreateSchemaObjectInDatabase(function);

        var functions = await theConnection.ExistingFunctionsAsync();

        functions.ShouldContain(function.Identifier);

        var existing = await Function.FetchExistingAsync(theConnection, function.Identifier);
        existing.ShouldNotBeNull();
    }

    [Fact]
    public async Task can_fetch_the_existing()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);

        await CreateSchemaObjectInDatabase(function);

        var existing = await function.FetchExistingAsync(theConnection);

        existing!.Identifier.ShouldBe(new SqlServerObjectName("functions", "add_10"));
        existing.DropStatements().Single()
            .ShouldBe("drop function if exists functions.add_10;");
        existing.Body().ShouldNotBeNull();
    }

    [Fact]
    public async Task can_fetch_the_delta_when_the_existing_does_not_exist()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);

        (await function.FetchExistingAsync(theConnection)).ShouldBeNull();

        var delta = await function.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public async Task for_removal_mechanics()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);

        await CreateSchemaObjectInDatabase(function);

        var toRemove = Function.ForRemoval(function.Identifier);

        var delta = await toRemove.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(toRemove);

        (await theConnection.FunctionExistsAsync(toRemove.Identifier)).ShouldBeFalse();
    }

    [Fact]
    public async Task rollback_existing()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);

        await CreateSchemaObjectInDatabase(function);

        var toRemove = Function.ForRemoval(function.Identifier);

        var delta = await toRemove.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        var migration = new SchemaMigration(delta);

        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        await migration.RollbackAllAsync(theConnection, new SqlServerMigrator());

        (await theConnection.FunctionExistsAsync(toRemove.Identifier)).ShouldBeTrue();
    }

    [Fact]
    public async Task can_fetch_the_delta_when_the_existing_matches()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);

        await CreateSchemaObjectInDatabase(function);

        var delta = await function.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task can_detect_a_change_in_body()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);
        await CreateSchemaObjectInDatabase(function);

        var different = Function.ForSql(theDifferentBody);

        var delta = await different.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task can_apply_the_creation_delta()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);

        await AssertNoDeltasAfterPatching(function);
    }

    [Fact]
    public async Task can_apply_a_changed_delta()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);
        await CreateSchemaObjectInDatabase(function);

        var different = Function.ForSql(theDifferentBody);

        await AssertNoDeltasAfterPatching(different);
    }

    [Fact]
    public async Task can_apply_a_changed_delta_with_different_signature()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);
        await CreateSchemaObjectInDatabase(function);

        var different = Function.ForSql(theEvenDifferentBody);

        await AssertNoDeltasAfterPatching(different);
    }

    [Fact]
    public async Task can_rollback_a_function_change()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);
        await CreateSchemaObjectInDatabase(function);

        var different = Function.ForSql(theEvenDifferentBody);

        var delta = await different.FindDeltaAsync(theConnection);

        var migration = new SchemaMigration(delta);

        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        await migration.RollbackAllAsync(theConnection, new SqlServerMigrator());

        // Should be back to the original function

        var lastDelta = await function.FindDeltaAsync(theConnection);

        lastDelta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task rollback_a_function_creation()
    {
        await ResetSchema();

        var function = Function.ForSql(theFunctionBody);

        var delta = await function.FindDeltaAsync(theConnection);
        var migration = new SchemaMigration(delta);

        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        (await theConnection.FunctionExistsAsync(function.Identifier)).ShouldBeTrue();

        await migration.RollbackAllAsync(theConnection, new SqlServerMigrator());

        (await theConnection.FunctionExistsAsync(function.Identifier)).ShouldBeFalse();
    }
}
