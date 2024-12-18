using Npgsql;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests;

[Collection("batching")]
public class BatchBuilderTests : IntegrationContext
{
    public BatchBuilderTests() : base("batching")
    {
    }

    [Fact]
    public async Task can_use_the_batcher()
    {
        var table = new Table("batching.thing");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("tag");
        table.AddColumn<int>("age");

        await ResetSchema();

        await CreateSchemaObjectInDatabase(table);

        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);

        await using var batch = source.CreateBatch();
        var batcher = new BatchBuilder(batch);

        batcher.Append("insert into batching.thing (id, tag, age) values (");
        batcher.AppendParameters(4, "blue", 10);
        batcher.Append(")");
        batcher.StartNewCommand();
        batcher.Append("insert into batching.thing (id, tag, age) values (");
        batcher.AppendParameters(5, "green", 11);
        batcher.Append(")");
        batcher.StartNewCommand();
        batcher.Append("insert into batching.thing (id, tag, age) values (:id, :tag, :age)");
        batcher.AddParameters(new { id = 6, tag = "yellow", age = 12 });
        batcher.StartNewCommand();
        batcher.Append("insert into batching.thing (id, tag, age) values (:id, :tag, :age)");
        batcher.AddParameters((object)new Dictionary<string, object?> { { "id", 7 }, { "tag", "red" }, { "age", 13 } });
        batcher.StartNewCommand();
        batcher.Append("insert into batching.thing (id, tag, age) values (:id, :tag, :age)");
        batcher.AddParameters((object)new Dictionary<string, int> { { "id", 8 }, { "age", 14 } });
        batcher.AddParameters((object)new Dictionary<string, string> { { "tag", "purple" } });
        batcher.Compile();

        await batch.ExecuteNonQueryAsync();

        await using var reader = await theConnection.CreateCommand("select id, tag, age from batching.thing order by id")
            .ExecuteReaderAsync();

        await reader.ReadAsync();

        (await reader.GetFieldValueAsync<int>(0)).ShouldBe(4);
        (await reader.GetFieldValueAsync<string>(1)).ShouldBe("blue");
        (await reader.GetFieldValueAsync<int>(2)).ShouldBe(10);

        await reader.ReadAsync();

        (await reader.GetFieldValueAsync<int>(0)).ShouldBe(5);
        (await reader.GetFieldValueAsync<string>(1)).ShouldBe("green");
        (await reader.GetFieldValueAsync<int>(2)).ShouldBe(11);

        await reader.ReadAsync();

        (await reader.GetFieldValueAsync<int>(0)).ShouldBe(6);
        (await reader.GetFieldValueAsync<string>(1)).ShouldBe("yellow");
        (await reader.GetFieldValueAsync<int>(2)).ShouldBe(12);

        await reader.ReadAsync();

        (await reader.GetFieldValueAsync<int>(0)).ShouldBe(7);
        (await reader.GetFieldValueAsync<string>(1)).ShouldBe("red");
        (await reader.GetFieldValueAsync<int>(2)).ShouldBe(13);

        await reader.ReadAsync();

        (await reader.GetFieldValueAsync<int>(0)).ShouldBe(8);
        (await reader.GetFieldValueAsync<string>(1)).ShouldBe("purple");
        (await reader.GetFieldValueAsync<int>(2)).ShouldBe(14);
    }
}
