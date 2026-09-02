using Shouldly;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Indexes;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.Indexes;

/// <summary>
///     weasel#541: <see cref="FullTextIndexDefinition" /> wrapped whatever it was given in
///     <c>to_tsvector</c>, so an expression that is <em>already</em> a <c>tsvector</c> could not be
///     indexed — and per-member <c>setweight</c> weighting with it.
/// </summary>
/// <remarks>
///     PostgreSQL's weighting labels each member's vector and concatenates the vectors, not the
///     text. Passed as <see cref="FullTextIndexDefinition.DocumentConfig" /> that expression came
///     out as <c>to_tsvector('english', setweight(…) || setweight(…))</c>, which is a type error
///     rather than a weighted index. Raised from JasperFx/marten#5298.
/// </remarks>
public class full_text_index_over_a_prebuilt_tsvector
{
    private const string TablePrefix = "mt_";
    private static readonly PostgresqlObjectName TableName = new("public", "mt_doc_achievement");
    private readonly Table parent = new(TableName);

    private const string Weighted =
        "setweight(to_tsvector('english', coalesce(data ->> 'Title', '')), 'A') || " +
        "setweight(to_tsvector('english', coalesce(data ->> 'Description', '')), 'C')";

    [Fact]
    public void the_expression_is_indexed_without_a_to_tsvector_wrapper()
    {
        var index = FullTextIndexDefinition.ForTsVector(TableName, Weighted);

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX {TableName.Name}_idx_fts ON {TableName.QualifiedName} USING gin (({Weighted}));");
    }

    /// <summary>
    ///     The point of the whole exercise: the emitted DDL is the shape PostgreSQL documents for
    ///     weighting, with the <c>tsvector</c> at the top level rather than inside a conversion.
    /// </summary>
    [Fact]
    public void the_ddl_does_not_convert_the_vector_again()
    {
        var ddl = FullTextIndexDefinition.ForTsVector(TableName, Weighted).ToDDL(parent);

        ddl.ShouldNotContain("to_tsvector('english',setweight");
        ddl.ShouldContain("gin ((setweight(");
    }

    [Fact]
    public void the_index_name_still_derives_and_still_takes_a_prefix()
    {
        FullTextIndexDefinition.ForTsVector(TableName, Weighted)
            .Name.ShouldBe($"{TableName.Name}_idx_fts");

        FullTextIndexDefinition.ForTsVector(TableName, Weighted, "ranked", TablePrefix)
            .Name.ShouldBe($"{TablePrefix}ranked");
    }

    /// <summary>
    ///     One property that both the DDL and a consumer's query-side filter read, so the indexed
    ///     vector and the searched vector cannot drift apart. Marten's <c>FullTextWhereFragment</c>
    ///     reads the expression back off the index definition for exactly this reason, and ranking
    ///     makes the coupling stricter still.
    /// </summary>
    [Fact]
    public void the_indexed_vector_is_readable_back_off_the_definition()
    {
        FullTextIndexDefinition.ForTsVector(TableName, Weighted)
            .IndexedTsVector.ShouldBe($"({Weighted})");

        new FullTextIndexDefinition(TableName, "data")
            .IndexedTsVector.ShouldBe("to_tsvector('english',data)");

        new FullTextIndexDefinition(TableName, "data", "italian")
            .IndexedTsVector.ShouldBe("to_tsvector('italian',data)");
    }

    [Fact]
    public void the_indexed_vector_is_exactly_what_lands_in_the_ddl()
    {
        foreach (var index in new[]
                 {
                     FullTextIndexDefinition.ForTsVector(TableName, Weighted),
                     new FullTextIndexDefinition(TableName, "data"),
                     new FullTextIndexDefinition(TableName, "(data ->> 'Name')", "italian")
                 })
        {
            index.ToDDL(parent).ShouldContain($"USING gin ({index.IndexedTsVector});");
        }
    }

    [Fact]
    public void an_empty_tsvector_expression_is_refused()
    {
        Should.Throw<ArgumentOutOfRangeException>(
            () => FullTextIndexDefinition.ForTsVector(TableName, "  "));
    }

    /// <summary>
    ///     Setting the property back to nothing returns the definition to the ordinary
    ///     <c>DocumentConfig</c> behaviour rather than leaving it in a half-configured state.
    /// </summary>
    [Fact]
    public void clearing_the_expression_falls_back_to_the_document_config()
    {
        var index = new FullTextIndexDefinition(TableName, "data") { TsVectorExpression = Weighted };

        index.IndexedTsVector.ShouldBe($"({Weighted})");

        index.TsVectorExpression = "   ";

        index.TsVectorExpression.ShouldBeNull();
        index.IndexedTsVector.ShouldBe("to_tsvector('english',data)");
    }

    /// <summary>
    ///     The whole reason for a second property rather than a reinterpretation of the first: every
    ///     existing definition has to emit the same bytes it always did. A changed index expression
    ///     makes Weasel drop and recreate the index, which on a large table is an outage rather than
    ///     a migration — so a diff here would be charged to people who never opted in.
    /// </summary>
    [Theory]
    [InlineData("data", null)]
    [InlineData("data", "italian")]
    [InlineData("(data ->> 'AnotherString' || ' ' || 'test')", null)]
    [InlineData("((data ->> 'FirstName') || ' ' || (data ->> 'LastName'))", "french")]
    public void an_untouched_definition_emits_exactly_what_it_always_did(string documentConfig, string? regConfig)
    {
        var index = new FullTextIndexDefinition(TableName, documentConfig, regConfig);

        var expectedRegConfig = regConfig ?? FullTextIndexDefinition.DefaultRegConfig;
        var expectedName = regConfig == null
            ? $"{TableName.Name}_idx_fts"
            : $"{TableName.Name}_{regConfig}_idx_fts";

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX {expectedName} ON {TableName.QualifiedName} USING gin (to_tsvector('{expectedRegConfig}',{documentConfig}));");
    }
}
