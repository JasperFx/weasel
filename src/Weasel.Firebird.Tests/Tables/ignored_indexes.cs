using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     weasel#642: an index Weasel was told to leave alone -- because a third party owns it -- is
///     neither dropped as an extra nor reported as drift.
/// </summary>
public class ignored_indexes: IntegrationContext
{
    private static Table people()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        return table;
    }

    [Fact]
    public async Task an_ignored_index_is_not_dropped()
    {
        await CreateSchemaObjectInDatabase(people());
        await ExecuteAsync("CREATE INDEX theirs ON people (name)");

        var model = people();
        model.IgnoreIndex("theirs");

        (await model.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        await ApplyAsync(model);

        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$INDICES WHERE RDB$INDEX_NAME = 'THEIRS'")).ShouldBe(1);
    }

    [Fact]
    public async Task an_ignored_index_matches_ignoring_case()
    {
        await CreateSchemaObjectInDatabase(people());
        await ExecuteAsync("CREATE INDEX theirs ON people (name)");

        var model = people();
        model.IgnoreIndex("THEIRS");

        (await model.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_index_that_is_not_ignored_is_still_dropped()
    {
        await CreateSchemaObjectInDatabase(people());
        await ExecuteAsync("CREATE INDEX theirs ON people (name)");

        var model = people();
        model.IgnoreIndex("somebody_elses");

        (await model.FindDeltaAsync(theConnection)).Indexes.Extras.Single().Name.ShouldBe("THEIRS");
    }

    [Fact]
    public void a_declared_index_cannot_be_ignored()
    {
        var model = people();
        model.ModifyColumn("name").AddIndex();

        Should.Throw<ArgumentException>(() => model.IgnoreIndex("idx_people_name"));
    }
}
