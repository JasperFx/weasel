using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     SQL Server 2025 JSON indexes as first-class schema objects (weasel#658).
///     <para>
///         The bug this closes is not only "Weasel cannot declare one". A JSON index IS in
///         <c>sys.indexes</c> with <c>type_desc = 'JSON'</c>, while its <c>sys.index_columns</c> row
///         carries <c>key_ordinal = 0</c> — which the ordinary column read filters out. So before
///         this, an existing JSON index was read as an index with NO columns, matched nothing
///         declared, landed in <c>Indexes.Extras</c>, and <c>TableDelta</c> emitted a DROP for it.
///         Any consumer on SQL Server 2025 with a JSON index had a migration that silently removed it.
///     </para>
/// </summary>
[Collection("integration")]
public class json_index_definition_tests: IntegrationContext
{
    public json_index_definition_tests(): base("json_index")
    {
    }

    /// <summary>
    ///     Does the server under test have JSON indexes at all? Gated at RUNTIME rather than with a
    ///     <c>Skip</c> constant, because the same suite runs against whatever
    ///     <c>weasel_sqlserver_testing_database</c> points at — 2025 in a JSON-capable lane, an older
    ///     server elsewhere — and a compile-time skip would silently stop covering this on the lane
    ///     that CAN run it.
    /// </summary>
    private async Task<bool> ServerHasJsonIndexes()
    {
        if (theConnection.State == System.Data.ConnectionState.Closed)
        {
            await theConnection.OpenAsync();
        }

        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = "select case when object_id('sys.json_indexes') is null then 0 else 1 end;";
        return (int)(await cmd.ExecuteScalarAsync())! == 1;
    }

    private static Table TableWithJsonIndex(bool withIndex = true, string[]? paths = null,
        bool optimizeForArraySearch = false)
    {
        var table = new Table("json_index.docs");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn("data", "json").NotNull();

        if (withIndex)
        {
            table.Indexes.Add(new JsonIndexDefinition("jidx_docs", "data")
            {
                JsonPaths = paths ?? ["$.name", "$.city"],
                OptimizeForArraySearch = optimizeForArraySearch
            });
        }

        return table;
    }

    [Fact]
    public void renders_the_create_json_index_grammar()
    {
        var table = TableWithJsonIndex();
        var index = (JsonIndexDefinition)table.Indexes.Single();

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("CREATE JSON INDEX");
        // QuoteName brackets only where an identifier needs it, matching IndexDefinition.
        ddl.ShouldContain("jidx_docs");
        ddl.ShouldContain("(data)");
        ddl.ShouldContain("FOR ('$.name', '$.city')");
    }

    [Fact]
    public void an_empty_path_list_indexes_the_whole_document()
    {
        var table = TableWithJsonIndex(paths: []);
        var index = (JsonIndexDefinition)table.Indexes.Single();

        index.ToDDL(table).ShouldNotContain("FOR");
    }

    [Fact]
    public void the_options_that_json_indexes_do_not_have_are_refused_not_ignored()
    {
        var table = TableWithJsonIndex();
        var index = (JsonIndexDefinition)table.Indexes.Single();
        index.IsUnique = true;

        // Rendering a dropped option would build a DIFFERENT index than the model describes, and the
        // model would keep reporting a match.
        var ex = Should.Throw<InvalidOperationException>(() => index.ToDDL(table));
        ex.Message.ShouldContain("IsUnique");
    }

    [Fact]
    public async Task creates_and_reads_back_as_a_json_index()
    {
        if (!await ServerHasJsonIndexes()) return;

        await ResetSchema();

        var table = TableWithJsonIndex();
        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        var index = existing!.Indexes.OfType<JsonIndexDefinition>().ShouldHaveSingleItem();
        index.Name.ShouldBe("jidx_docs");
        index.ColumnName.ShouldBe("data");
        index.JsonPaths.OrderBy(x => x).ShouldBe(["$.city", "$.name"]);
    }

    /// <summary>
    ///     The fixed point. An applied table must then report no difference — the trap weasel#637 hit,
    ///     where a declaration that cannot canonicalize against the catalog reports drift on every pass
    ///     and then tries to drop and recreate.
    /// </summary>
    [Fact]
    public async Task an_applied_json_index_reaches_a_fixed_point()
    {
        if (!await ServerHasJsonIndexes()) return;

        await ResetSchema();

        var table = TableWithJsonIndex();
        await CreateSchemaObjectInDatabase(table);

        var delta = await table.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The regression this issue is really about: an existing JSON index that the model does NOT
    ///     declare must not be reported as an extra index and dropped. Before the fetch change it was
    ///     read as a zero-column index and <c>TableDelta</c> emitted <c>DROP INDEX</c> for it.
    /// </summary>
    [Fact]
    public async Task an_existing_json_index_is_not_read_as_a_phantom_to_be_dropped()
    {
        if (!await ServerHasJsonIndexes()) return;

        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWithJsonIndex());

        // The same table WITHOUT the JSON index declared.
        var undeclared = TableWithJsonIndex(withIndex: false);
        var existing = await undeclared.FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        var phantoms = existing!.Indexes
            .Where(x => x.GetType() == typeof(IndexDefinition) && x.Name == "jidx_docs")
            .ToList();

        phantoms.ShouldBeEmpty("a JSON index must never arrive as an ordinary zero-column IndexDefinition");

        // And it IS still seen — as itself, so a model that declares it can match it.
        existing.Indexes.OfType<JsonIndexDefinition>().ShouldHaveSingleItem().Name.ShouldBe("jidx_docs");
    }

    /// <summary>
    ///     A changed path list is a real difference, not a silently-equal one. Without this the model
    ///     could claim a match over an index covering entirely different paths.
    /// </summary>
    [Fact]
    public async Task changing_the_indexed_paths_is_a_difference()
    {
        if (!await ServerHasJsonIndexes()) return;

        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWithJsonIndex(paths: ["$.name"]));

        var changed = TableWithJsonIndex(paths: ["$.name", "$.city"]);
        var delta = await changed.FindDeltaAsync(theConnection);

        delta.Difference.ShouldNotBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task optimize_for_array_search_round_trips()
    {
        if (!await ServerHasJsonIndexes()) return;

        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWithJsonIndex(optimizeForArraySearch: true));

        var existing = await TableWithJsonIndex(optimizeForArraySearch: true)
            .FetchExistingAsync(theConnection);

        existing!.Indexes.OfType<JsonIndexDefinition>().Single()
            .OptimizeForArraySearch.ShouldBeTrue();
    }
}
