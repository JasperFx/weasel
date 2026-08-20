using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Synonyms;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Synonyms;

/// <summary>
///     SQL Server synonyms (weasel#453).
/// </summary>
[Collection("integration")]
public class SynonymTests: IntegrationContext
{
    public SynonymTests(): base("synonyms")
    {
    }

    private static Table SourceTable(string name = "orders")
    {
        var table = new Table($"synonyms.{name}");
        table.AddColumn<int>("id").AsPrimaryKey();
        return table;
    }

    private static Synonym NewSynonym(string target = "synonyms.orders")
        => new("synonyms.order_alias", target);

    /// <summary>
    ///     <c>sys.synonyms</c> brackets and qualifies the target whatever the caller wrote, so a
    ///     bare name and a bracketed one have to compare equal or every synonym reports drift.
    /// </summary>
    [Fact]
    public void the_target_comparison_ignores_bracketing()
    {
        Synonym.Normalize("[synonyms].[orders]").ShouldBe(Synonym.Normalize("synonyms.orders"));
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

        await theConnection.CreateCommand("insert into synonyms.orders (id) values (1)").ExecuteNonQueryAsync();

        var count = await theConnection.CreateCommand("select count(*) from synonyms.order_alias")
            .ExecuteScalarAsync();

        Convert.ToInt32(count).ShouldBe(1);
    }

    [Fact]
    public async Task a_retargeted_synonym_reports_update_and_applying_it_converges()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await SourceTable("archive").ApplyChangesAsync(theConnection);
        await NewSynonym().ApplyChangesAsync(theConnection);

        var retargeted = NewSynonym("synonyms.archive");

        (await retargeted.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await retargeted.ApplyChangesAsync(theConnection);

        (await retargeted.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     SQL Server's teardown enumerates object types by hand, so a new one has to be added to it
    ///     — the rule CLAUDE.md gained in weasel#469, and the trap weasel#464 fell into with views.
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
