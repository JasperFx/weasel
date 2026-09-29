using FirebirdSql.Data.FirebirdClient;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     A guard has to recognise the object its statement creates, not just the name: constraint and
///     index names share one namespace in a Firebird database, and a view shares one with tables. A
///     guard that matched the name alone skipped its statement silently whenever something else held
///     the name, and the model reported the object missing on every migration afterwards.
/// </summary>
public class guarded_statements: IntegrationContext
{
    [Fact]
    public async Task an_index_named_like_a_primary_keys_own_index_is_refused_rather_than_skipped()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("age");
        await CreateSchemaObjectInDatabase(table);

        table.Indexes.Add(new IndexDefinition("pk_people") { Columns = ["age"] });

        await Should.ThrowAsync<FbException>(() => ApplyAsync(table));
    }

    [Fact]
    public async Task a_table_named_like_a_view_is_refused_rather_than_skipped()
    {
        await ExecuteAsync("CREATE VIEW people AS SELECT 1 AS id FROM RDB$DATABASE");

        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();

        await Should.ThrowAsync<FbException>(() => ApplyAsync(table));
    }

    [Fact]
    public async Task a_foreign_key_named_like_another_tables_key_is_refused_rather_than_skipped()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();

        var others = new Table("others");
        others.AddColumn<int>("id").AsPrimaryKey();
        others.AddColumn<int>("state_id").ForeignKeyTo(states, "id", "fk_shared_name");

        var people = new Table("people");
        people.AddColumn<int>("id").AsPrimaryKey();
        people.AddColumn<int>("state_id").ForeignKeyTo(states, "id", "fk_shared_name");

        await ApplyAsync(states, others);

        await Should.ThrowAsync<FbException>(() => ApplyAsync(people));
    }

    [Fact]
    public async Task the_guard_still_skips_the_object_it_creates()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("age").AddIndex();

        var writer = new StringWriter();
        table.WriteCreateStatement(new FirebirdMigrator(), writer);

        await ExecuteAsync(writer.ToString());
        await ExecuteAsync(writer.ToString());

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
