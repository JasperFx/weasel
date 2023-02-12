using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Functions;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Functions;

[Collection("functions")]
public class UpsertFunctionTests: IntegrationContext
{
    private Table theHiloTable;
    private readonly UpsertFunction theFunction;

    public UpsertFunctionTests(): base("functions")
    {
        theHiloTable = new Table("functions.mt_hilo");
        theHiloTable.AddColumn("entity_name", "varchar(200)").AsPrimaryKey();
        theHiloTable.AddColumn<int>("next_value");
        theHiloTable.AddColumn<int>("hi_value");

        theFunction = new UpsertFunction(new DbObjectName("functions", "upsert_mt_hilo"), theHiloTable,
            "next_value", "hi_value");
    }

    public override async Task InitializeAsync()
    {
        await ResetSchema();
        await CreateSchemaObjectInDatabase(theHiloTable);
    }

    [Fact]
    public async Task can_create_the_function()
    {
        await CreateSchemaObjectInDatabase(theFunction);
    }

    public class HiloEntity
    {
        public int Next { get; }
        public int High { get; }

        public HiloEntity(int next, int high)
        {
            Next = next;
            High = high;
        }
    }

    private async Task<HiloEntity> readEntity(string entityName)
    {
        var entities = await theConnection
            .CreateCommand("select next_value, hi_value from functions.mt_hilo where entity_name = :name")
            .With("name", entityName)
            .FetchListAsync(async reader =>
            {
                var next = await reader.GetFieldValueAsync<int>(0);
                var high = await reader.GetFieldValueAsync<int>(1);

                return new HiloEntity(next, high);
            });

        return entities.Single();
    }

    [Fact]
    public async Task can_execute_the_function()
    {
        await CreateSchemaObjectInDatabase(theFunction);

        var entityName = Guid.NewGuid().ToString();

        await theConnection.CallFunction(theFunction.Identifier, "p_entity_name", "p_next_value", "p_hi_value")
            .With("p_entity_name", entityName)
            .With("p_next_value", 5)
            .With("p_hi_value", 10)
            .ExecuteNonQueryAsync();

        var data = await readEntity(entityName);
        data.Next.ShouldBe(5);
        data.High.ShouldBe(10);

        await theConnection.CallFunction(theFunction.Identifier, "p_entity_name", "p_next_value", "p_hi_value")
            .With("p_entity_name", entityName)
            .With("p_next_value", 15)
            .With("p_hi_value", 25)
            .ExecuteNonQueryAsync();

        await theConnection.CallFunction(theFunction.Identifier, "p_entity_name", "p_next_value", "p_hi_value")
            .With("p_entity_name", Guid.NewGuid().ToString())
            .With("p_next_value", 16)
            .With("p_hi_value", 26)
            .ExecuteNonQueryAsync();

        data = await readEntity(entityName);
        data.Next.ShouldBe(15);
        data.High.ShouldBe(25);
    }
}
