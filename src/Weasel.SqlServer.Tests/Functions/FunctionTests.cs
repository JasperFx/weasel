using System.Linq;
using System.Threading.Tasks;
using Baseline;
using Shouldly;
using Weasel.SqlServer.Functions;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Functions
{
    [Collection("functions")]
    public class FunctionTests : IntegrationContext
    {
        private readonly string theFunctionBody = @"
CREATE OR REPLACE FUNCTION functions.mt_get_next_hi(entity varchar) RETURNS integer AS
$$
DECLARE
    current_value bigint;
    next_value bigint;
BEGIN
    select hi_value into current_value from functions.mt_hilo where entity_name = entity;
    IF current_value is null THEN
        insert into functions.mt_hilo (entity_name, hi_value) values (entity, 0);
        next_value := 0;
    ELSE
        next_value := current_value + 1;
        update functions.mt_hilo set hi_value = next_value where entity_name = entity and hi_value = current_value;

        IF NOT FOUND THEN
            next_value := -1;
        END IF;
    END IF;

    return next_value;
END

$$ LANGUAGE plpgsql;
";
        
        private readonly string theDifferentBody = @"
CREATE OR REPLACE FUNCTION functions.mt_get_next_hi(entity varchar) RETURNS integer AS
$$
DECLARE
    current_value bigint;
    next_value bigint;
BEGIN
    update functions.mt_hilo set hi_value = next_value where entity_name = entity and hi_value = current_value;

    return 1;
END

$$ LANGUAGE plpgsql;
";
        
        private readonly string theEvenDifferentBody = @"
CREATE OR REPLACE FUNCTION functions.mt_get_next_hi(entity varchar, name varchar) RETURNS integer AS
$$
DECLARE
    current_value bigint;
    next_value bigint;
BEGIN
    update functions.mt_hilo set hi_value = next_value where entity_name = entity and hi_value = current_value;

    return 1;
END

$$ LANGUAGE plpgsql;
";

        private Table theHiloTable;

        public FunctionTests() : base("functions")
        {
            theHiloTable = new Table("functions.mt_hilo");
            theHiloTable.AddColumn<string>("entity_name").AsPrimaryKey();
            theHiloTable.AddColumn<int>("next_value");
            theHiloTable.AddColumn<int>("hi_value");
        }
        
        protected async Task AssertNoDeltasAfterPatching(Function func)
        {
            await func.ApplyChanges(theConnection);

            var delta = await func.FindDelta(theConnection);
            
            delta.Difference.ShouldBe(SchemaPatchDifference.None);
        }

        [Fact]
        public void for_removal()
        {
            var function = Function.ForRemoval("functions.mt_hilo");
            function.IsRemoved.ShouldBeTrue();
            function.Identifier.QualifiedName.ShouldBe("functions.mt_hilo");
        }

        [Fact]
        public void can_read_the_function_identifier_from_a_function_body()
        {
            Function.ParseIdentifier(theFunctionBody)
                .ShouldBe(new DbObjectName("functions", "mt_get_next_hi"));
        }

        [Fact]
        public void can_derive_the_drop_statement_from_the_body()
        {
            var function = new Function(DbObjectName.Parse("functions.mt_get_next_hi"), theFunctionBody);
            function.DropStatements().Single().ShouldBe("drop function if exists functions.mt_get_next_hi(varchar);");
        }

        [Fact]
        public void can_build_function_object_from_body()
        {
            var function = Function.ForSql(theFunctionBody);
            function.Identifier.ShouldBe(new DbObjectName("functions", "mt_get_next_hi"));
            
            function.DropStatements().Single()
                .ShouldBe("drop function if exists functions.mt_get_next_hi(varchar);");
        }

        [Fact]
        public async Task can_build_function_in_database()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);

            var function = Function.ForSql(theFunctionBody);

            await CreateSchemaObjectInDatabase(function);

            var next = await theConnection.CreateCommand("select functions.mt_get_next_hi('foo');")
                .ExecuteScalarAsync();
            
            next.As<int>().ShouldBe(0);

            await theConnection.FunctionExists(function.Identifier);
        }
        
        

        [Fact]
        public async Task existing_functions_extension_method()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);
            
            var function = Function.ForSql(theFunctionBody);

            await CreateSchemaObjectInDatabase(function);

            var functions = await theConnection.ExistingFunctions();
            
            functions.ShouldContain(function.Identifier);

            var existing = await theConnection.FindExistingFunction(function.Identifier);
            existing.ShouldNotBeNull();
        }

        [Fact]
        public async Task can_fetch_the_existing()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);

            var function = Function.ForSql(theFunctionBody);
            
            await CreateSchemaObjectInDatabase(function);

            var existing = await function.FetchExisting(theConnection);
            
            existing.Identifier.ShouldBe(new DbObjectName("functions", "mt_get_next_hi"));
            existing.DropStatements().Single().ShouldBe("DROP FUNCTION IF EXISTS functions.mt_get_next_hi(entity character varying);");
            existing.Body().ShouldNotBeNull();
        }

        [Fact]
        public async Task can_fetch_the_delta_when_the_existing_does_not_exist()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);

            var function = Function.ForSql(theFunctionBody);
            
            (await function.FetchExisting(theConnection)).ShouldBeNull();

            var delta = await function.FindDelta(theConnection);
            
            delta.Difference.ShouldBe(SchemaPatchDifference.Create);
        }

        [Fact]
        public async Task for_removal_mechanics()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);

            var function = Function.ForSql(theFunctionBody);

            await CreateSchemaObjectInDatabase(function);

            var toRemove = Function.ForRemoval(function.Identifier);

            var delta = await toRemove.FindDelta(theConnection);
            delta.Difference.ShouldBe(SchemaPatchDifference.Update);

            await AssertNoDeltasAfterPatching(toRemove);
            
            (await theConnection.FunctionExists(toRemove.Identifier)).ShouldBeFalse();
        }
        
        [Fact]
        public async Task rollback_existing()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);

            var function = Function.ForSql(theFunctionBody);

            await CreateSchemaObjectInDatabase(function);

            var toRemove = Function.ForRemoval(function.Identifier);

            var delta = await toRemove.FindDelta(theConnection);
            delta.Difference.ShouldBe(SchemaPatchDifference.Update);

            var migration = new SchemaMigration(delta);

            await migration.ApplyAll(theConnection, new DdlRules(), AutoCreate.CreateOrUpdate);

            await migration.RollbackAll(theConnection, new DdlRules());
            
            (await theConnection.FunctionExists(toRemove.Identifier)).ShouldBeTrue();
        }
        
        [Fact]
        public async Task can_fetch_the_delta_when_the_existing_matches()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);

            var function = Function.ForSql(theFunctionBody);

            await CreateSchemaObjectInDatabase(function);

            var delta = await function.FindDelta(theConnection);
            
            delta.Difference.ShouldBe(SchemaPatchDifference.None);
        }

        [Fact]
        public async Task can_detect_a_change_in_body()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);

            var function = Function.ForSql(theFunctionBody);
            await CreateSchemaObjectInDatabase(function);

            var different = Function.ForSql(theDifferentBody);

            var delta = await different.FindDelta(theConnection);
            
            delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        }

        [Fact]
        public async Task can_apply_the_creation_delta()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);
            
            var function = Function.ForSql(theFunctionBody);

            await AssertNoDeltasAfterPatching(function);
        }

        [Fact]
        public async Task can_apply_a_changed_delta()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);

            var function = Function.ForSql(theFunctionBody);
            await CreateSchemaObjectInDatabase(function);

            var different = Function.ForSql(theDifferentBody);

            await AssertNoDeltasAfterPatching(different);
        }
        
        [Fact]
        public async Task can_apply_a_changed_delta_with_different_signature()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);

            var function = Function.ForSql(theFunctionBody);
            await CreateSchemaObjectInDatabase(function);

            var different = Function.ForSql(theEvenDifferentBody);

            await AssertNoDeltasAfterPatching(different);
        }

        [Fact]
        public async Task can_rollback_a_function_change()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);

            var function = Function.ForSql(theFunctionBody);
            await CreateSchemaObjectInDatabase(function);

            var different = Function.ForSql(theEvenDifferentBody);

            var delta = await different.FindDelta(theConnection);

            var migration = new SchemaMigration(delta);

            await migration.ApplyAll(theConnection, new DdlRules(), AutoCreate.CreateOrUpdate);

            await migration.RollbackAll(theConnection, new DdlRules());
            
            // Should be back to the original function

            var lastDelta = await function.FindDelta(theConnection);
             
            lastDelta.Difference.ShouldBe(SchemaPatchDifference.None);
        }

        [Fact]
        public async Task rollback_a_function_creation()
        {
            await ResetSchema();

            await CreateSchemaObjectInDatabase(theHiloTable);

            var function = Function.ForSql(theFunctionBody);
            
            var delta = await function.FindDelta(theConnection);
            var migration = new SchemaMigration(delta);
            
            await migration.ApplyAll(theConnection, new DdlRules(), AutoCreate.CreateOrUpdate);
            
            (await theConnection.FunctionExists(function.Identifier)).ShouldBeTrue();

            await migration.RollbackAll(theConnection, new DdlRules());
            
            (await theConnection.FunctionExists(function.Identifier)).ShouldBeFalse();
        }
    }

}