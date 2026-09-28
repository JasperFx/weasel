using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Tables;

/// <summary>
///     <see cref="IndexDefinition.Tablespace" /> is written into the index DDL, and index comparison
///     compares the DDL both sides render. The reader never read the tablespace back, so an index
///     that declared one never matched the index it created: every migration dropped and recreated it.
/// </summary>
public class detecting_index_tablespaces: IntegrationContext
{
    private const string SchemaName = "tablespaces";

    public detecting_index_tablespaces(): base(SchemaName)
    {
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static Table TableWith(Action<IndexDefinition>? configure = null)
    {
        var table = new Table($"{SchemaName}.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");

        var index = new IndexDefinition("idx_people_name") { Columns = ["name"] };
        configure?.Invoke(index);
        table.Indexes.Add(index);

        return table;
    }

    [Fact]
    public async Task an_index_with_a_declared_tablespace_matches_the_index_it_created()
    {
        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWith(x => x.Tablespace = "USERS"));

        var delta = await TableWith(x => x.Tablespace = "USERS").FindDeltaAsync(theConnection, Ct);

        delta.Indexes.Different.ShouldBeEmpty();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_migration_path_settles_on_an_index_with_a_tablespace()
    {
        await ResetSchema();

        (await TableWith(x => x.Tablespace = "USERS").MigrateAsync(theConnection)).ShouldBeTrue();
        (await TableWith(x => x.Tablespace = "USERS").MigrateAsync(theConnection)).ShouldBeFalse();
    }

    [Fact]
    public async Task an_index_that_declares_no_tablespace_does_not_compare_the_one_oracle_chose()
    {
        // Every index has a tablespace in the catalog. A model that does not name one has no opinion
        // about it, and must keep matching whatever the database defaulted to.
        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWith());

        (await TableWith().FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_tablespace_is_detected()
    {
        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWith(x => x.Tablespace = "USERS"));

        var delta = await TableWith(x => x.Tablespace = "SYSAUX").FindDeltaAsync(theConnection, Ct);

        delta.Indexes.Different.Count().ShouldBe(1);
    }

    [Fact]
    public async Task the_existing_index_reports_its_tablespace()
    {
        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWith(x => x.Tablespace = "USERS"));

        var existing = await TableWith().FetchExistingAsync(theConnection, Ct);

        existing.ShouldNotBeNull();
        existing.Indexes.Single().Tablespace.ShouldBe("USERS");
    }
}
