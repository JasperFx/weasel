using Npgsql;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.Indexes;

[Collection("fts_deltas")]
public class FullTextIndexDeltasTests(): IndexDeltasDetectionContext("fts_deltas", "users")
{
    [PgVersionTargetedFact(MinimumVersion = "10.0")]
    public Task wholedoc_fts_index_comparison_works()
    {
        theTable.ModifyColumn("data").AddFullTextIndex();

        return AssertNoDeltasAfterPatching();
    }

    [PgVersionTargetedFact(MinimumVersion = "10.0")]
    public Task fts_index_comparison_must_take_into_account_automatic_cast()
    {
        theTable.ModifyColumn("data").AddFullTextIndex(documentConfig: "(data ->> 'Name')");

        return AssertNoDeltasAfterPatching();
    }

    [PgVersionTargetedFact(MinimumVersion = "10.0")]
    public Task multifield_fts_index_comparison_must_take_into_account_automatic_cast()
    {
        theTable.ModifyColumn("data")
            .AddFullTextIndex(documentConfig: "((data ->> 'FirstName') || ' ' || (data ->> 'LastName'))");

        return AssertNoDeltasAfterPatching();
    }

    [PgVersionTargetedFact(MinimumVersion = "10.0")]
    public async Task modified_fts_index_comparison_must_generate_index_update()
    {
        theTable.ModifyColumn("data").AddFullTextIndex(documentConfig: "(data ->> 'Name')");

        await CreateSchemaObjectInDatabase(theTable);

        theTable.Indexes.Clear();

        theTable.ModifyColumn("data")
            .AddFullTextIndex(documentConfig: "((data ->> 'FirstName') || ' ' || (data ->> 'LastName'))");

        await AssertIndexUpdate($"{theTable.Identifier.Name}_idx_fts");
    }

    [PgVersionTargetedFact(MinimumVersion = "10.0")]
    public async Task modified_fts_index_by_regConfig_comparison_must_generate_index_update()
    {
        const string documentConfig = "(data ->> 'Name')";
        theTable.ModifyColumn("data").AddFullTextIndex(documentConfig: documentConfig);

        await CreateSchemaObjectInDatabase(theTable);

        theTable.Indexes.Clear();

        const string newRegConfig = "italian";
        theTable.ModifyColumn("data")
            .AddFullTextIndex(newRegConfig, documentConfig);

        await AssertIndexRecreation(
            $"{theTable.Identifier.Name}_{newRegConfig}_idx_fts",
            $"{theTable.Identifier.Name}_idx_fts"
        );
    }


    [PgVersionTargetedFact(MinimumVersion = "10.0")]
    public async Task modified_fts_index_with_customIndex_by_regConfig_comparison_must_generate_index_update()
    {
        const string documentConfig = "(data ->> 'Name')";
        const string indexName = "custom_index_name";

        theTable.ModifyColumn("data").AddFullTextIndex(documentConfig: documentConfig, indexName: indexName);

        await CreateSchemaObjectInDatabase(theTable);

        theTable.Indexes.Clear();

        const string newRegConfig = "italian";
        theTable.ModifyColumn("data")
            .AddFullTextIndex(newRegConfig, documentConfig, indexName: indexName);

        await AssertIndexUpdate(indexName);
    }

    [PgVersionTargetedFact(MinimumVersion = "10.0")]
    public async Task migration_from_v3_to_v4_should_not_result_in_schema_difference()
    {
        await CreateSchemaObjectInDatabase(theTable);

        // create index with a sql statement not containing `::regconfig`
        await using (var conn = new NpgsqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync();
            await conn.CreateCommand(
                    $"CREATE INDEX {theTable.Identifier.Name}_idx_fts ON {theTable.Identifier} USING gin (( to_tsvector('english', data) ))")
                .ExecuteNonQueryAsync();
        }

        theTable.ModifyColumn("data").AddFullTextIndex();

        await AssertNoDeltasAfterPatching();
    }
}
