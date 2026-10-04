using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

public class identity_columns: IntegrationContext
{
    private static Table events()
    {
        var table = new Table("events");
        table.AddColumn<long>("id").AsPrimaryKey().AutoIncrement();
        table.AddColumn<string>("name");
        return table;
    }

    [Fact]
    public async Task an_identity_column_generates_its_values()
    {
        await CreateSchemaObjectInDatabase(events());

        await ExecuteAsync("INSERT INTO events (name) VALUES ('a')");
        await ExecuteAsync("INSERT INTO events (name) VALUES ('b')");

        (await ListAsync("SELECT id FROM events ORDER BY id")).ShouldBe(["1", "2"]);
    }

    [Fact]
    public async Task an_identity_column_reads_back_as_one_and_with_no_delta()
    {
        await CreateSchemaObjectInDatabase(events());

        var existing = await events().FetchExistingAsync(theConnection);
        existing!.ColumnFor("id")!.IsAutoNumber.ShouldBeTrue();

        (await events().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Identity is not compared, as on Oracle, so a model that stops saying AutoIncrement leaves the
    ///     column alone rather than trying to rebuild it.
    /// </summary>
    [Fact]
    public async Task identity_is_not_compared()
    {
        await CreateSchemaObjectInDatabase(events());

        var plain = new Table("events");
        plain.AddColumn<long>("id").AsPrimaryKey();
        plain.AddColumn<string>("name");

        (await plain.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Firebird refuses an identity column on a table with rows (335544989), and whether it has rows
    ///     cannot be known when the migration is planned.
    /// </summary>
    [Fact]
    public async Task adding_an_identity_column_to_an_existing_table_is_refused_before_anything_runs()
    {
        var table = new Table("events");
        table.AddColumn<string>("name");
        await CreateSchemaObjectInDatabase(table);

        table.AddColumn<long>("id").AutoIncrement();
        var delta = await table.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.InvalidReason!.ShouldContain("identity");
        await Should.ThrowAsync<SchemaMigrationException>(() => ApplyAsync(AutoCreate.CreateOrUpdate, table));
    }

    [Fact]
    public async Task the_generator_behind_an_identity_is_the_servers_own()
    {
        await CreateSchemaObjectInDatabase(events());

        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$GENERATORS WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0"))
            .ShouldBe(0, "the identity's generator is system flag 6, and goes with its column");
    }
}
