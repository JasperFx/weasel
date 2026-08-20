using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Synonyms;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Synonyms;

/// <summary>
///     Oracle private synonyms (weasel#453). Public synonyms are modelled but not covered here —
///     creating one needs CREATE PUBLIC SYNONYM, which the test user is not granted.
/// </summary>
[Collection("integration")]
public class SynonymTests: IntegrationContext
{
    private const string SchemaName = "WEASEL";

    public SynonymTests(): base(SchemaName)
    {
    }

    private static Table SourceTable(string name = "syn_orders")
    {
        var table = new Table($"{SchemaName}.{name}");
        table.AddColumn<int>("id").AsPrimaryKey();
        return table;
    }

    private static Synonym NewSynonym(string target = "syn_orders")
        => new($"{SchemaName}.syn_alias", $"{SchemaName}.{target}");

    [Fact]
    public void an_unqualified_target_is_qualified_with_the_synonym_own_schema()
    {
        var synonym = new Synonym($"{SchemaName}.a", "orders");

        synonym.Normalize("orders").ShouldBe($"{SchemaName}.ORDERS");
    }

    [Fact]
    public async Task a_synonym_round_trips_and_reports_no_delta()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);

        var synonym = NewSynonym();
        await synonym.ApplyChangesAsync(theConnection);

        (await synonym.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await synonym.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_unchanged_synonym_does_not_report_permanent_drift()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewSynonym().ApplyChangesAsync(theConnection);

        (await NewSynonym().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await NewSynonym().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_synonym_actually_resolves()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewSynonym().ApplyChangesAsync(theConnection);

        await theConnection.CreateCommand($"INSERT INTO {SchemaName}.syn_orders (id) VALUES (1)")
            .ExecuteNonQueryAsync();

        var count = await theConnection.CreateCommand($"SELECT COUNT(*) FROM {SchemaName}.syn_alias")
            .ExecuteScalarAsync();

        Convert.ToInt32(count).ShouldBe(1);
    }

    [Fact]
    public async Task a_retargeted_synonym_reports_update_and_applying_it_converges()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await SourceTable("syn_archive").ApplyChangesAsync(theConnection);
        await NewSynonym().ApplyChangesAsync(theConnection);

        var retargeted = NewSynonym("syn_archive");

        (await retargeted.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await retargeted.ApplyChangesAsync(theConnection);

        (await retargeted.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Oracle's teardown learned synonyms in weasel#469, before anything could create one. This
    ///     is the test that arms it.
    /// </summary>
    [Fact]
    public async Task dropping_the_schema_takes_its_synonyms_with_it()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewSynonym().ApplyChangesAsync(theConnection);

        await ResetSchema();

        (await NewSynonym().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
