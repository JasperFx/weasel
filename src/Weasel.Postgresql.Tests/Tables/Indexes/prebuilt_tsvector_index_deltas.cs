using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables.Indexes;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.Indexes;

/// <summary>
///     weasel#541 against a real server. The unit tests say what DDL comes out; these say PostgreSQL
///     accepts it, and that Weasel reads it back as the same index rather than as a change.
/// </summary>
/// <remarks>
///     Both halves matter. A weighted index is built with <c>||</c>, and PostgreSQL lets an index
///     expression go bare only when it is a function call or a column reference — so the DDL is a
///     syntax error unless the expression carries parentheses of its own. And an index Weasel
///     re-reads as different is dropped and recreated on every single migration, which on a table
///     big enough to want ranked search is the expensive kind of wrong.
/// </remarks>
[Collection("fts_tsvector_deltas")]
public class prebuilt_tsvector_index_deltas(): IndexDeltasDetectionContext("fts_tsvector_deltas", "achievements")
{
    private const string Weighted =
        "setweight(to_tsvector('english', coalesce(data ->> 'Title', '')), 'A') || " +
        "setweight(to_tsvector('english', coalesce(data ->> 'Description', '')), 'C')";

    [PgVersionTargetedFact(MinimumVersion = "10.0")]
    public Task a_weighted_tsvector_index_is_accepted_and_round_trips()
    {
        theTable.Indexes.Add(FullTextIndexDefinition.ForTsVector(
            PostgresqlObjectName.From(theTable.Identifier), Weighted));

        return AssertNoDeltasAfterPatching();
    }

    /// <summary>
    ///     A single <c>setweight</c> — a function call at the top level, so it would be legal
    ///     unparenthesized too. Here to prove the wrapping does not break the case that never needed
    ///     it.
    /// </summary>
    [PgVersionTargetedFact(MinimumVersion = "10.0")]
    public Task a_single_weighted_member_round_trips()
    {
        theTable.Indexes.Add(FullTextIndexDefinition.ForTsVector(
            PostgresqlObjectName.From(theTable.Identifier),
            "setweight(to_tsvector('english', coalesce(data ->> 'Title', '')), 'A')"));

        return AssertNoDeltasAfterPatching();
    }

    /// <summary>
    ///     An expression the caller already parenthesized must not be double-counted into a
    ///     difference.
    /// </summary>
    [PgVersionTargetedFact(MinimumVersion = "10.0")]
    public Task an_already_parenthesized_expression_round_trips()
    {
        theTable.Indexes.Add(FullTextIndexDefinition.ForTsVector(
            PostgresqlObjectName.From(theTable.Identifier), $"({Weighted})"));

        return AssertNoDeltasAfterPatching();
    }

    /// <summary>
    ///     Changing the weights is a real change and has to be seen as one — that is the whole point
    ///     of the expression being readable back off the definition.
    /// </summary>
    [PgVersionTargetedFact(MinimumVersion = "10.0")]
    public async Task changing_a_weight_is_detected_as_an_update()
    {
        var name = PostgresqlObjectName.From(theTable.Identifier);
        theTable.Indexes.Add(FullTextIndexDefinition.ForTsVector(name, Weighted));

        await CreateSchemaObjectInDatabase(theTable);

        theTable.Indexes.Clear();
        theTable.Indexes.Add(FullTextIndexDefinition.ForTsVector(
            name, Weighted.Replace("'C')", "'B')")));

        await AssertIndexUpdate($"{theTable.Identifier.Name}_idx_fts");
    }

    /// <summary>
    ///     The weighting actually works once the index is there — <c>ts_rank</c> puts a title match
    ///     above a description match. Without this the tests would prove only that Weasel emits and
    ///     re-reads a string, not that the string means what weasel#541 was raised to make it mean.
    /// </summary>
    [PgVersionTargetedFact(MinimumVersion = "10.0")]
    public async Task the_weighting_ranks_a_title_match_above_a_description_match()
    {
        theTable.Indexes.Add(FullTextIndexDefinition.ForTsVector(
            PostgresqlObjectName.From(theTable.Identifier), Weighted));

        await CreateSchemaObjectInDatabase(theTable);

        await theConnection.CreateCommand(
                $"insert into {theTable.Identifier} (id, data) values (1, '{{\"Title\": \"ordinary\", \"Description\": \"marmot sighting\"}}'::jsonb)")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand(
                $"insert into {theTable.Identifier} (id, data) values (2, '{{\"Title\": \"marmot\", \"Description\": \"ordinary\"}}'::jsonb)")
            .ExecuteNonQueryAsync();

        var indexed = ((FullTextIndexDefinition)theTable.Indexes[0]).IndexedTsVector;

        var ranked = await theConnection.CreateCommand(
                $"select id from {theTable.Identifier} where {indexed} @@ plainto_tsquery('english', 'marmot') "
                + $"order by ts_rank({indexed}, plainto_tsquery('english', 'marmot')) desc")
            .FetchListAsync<int>();

        ranked.ShouldBe(new[] { 2, 1 });
    }
}
