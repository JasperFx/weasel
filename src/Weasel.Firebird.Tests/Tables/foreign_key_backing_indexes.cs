using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     A primary key, a unique constraint and a foreign key each create an index of their own in
///     Firebird, named after the constraint -- or <c>RDB$PRIMARYn</c> / <c>RDB$FOREIGNn</c> when the
///     constraint is unnamed. Those are not the model's indexes, and comparing them as if they were
///     reported every one as an extra to drop (weasel#445).
/// </summary>
public class foreign_key_backing_indexes: IntegrationContext
{
    private static Table states()
    {
        var table = new Table("states");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("code", "VARCHAR(2)");
        return table;
    }

    private static Table people()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("state_id").ForeignKeyTo("states", "id");
        return table;
    }

    [Fact]
    public async Task the_index_behind_a_foreign_key_is_not_an_index_of_the_model()
    {
        await ApplyAsync(states(), people());

        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$INDICES WHERE RDB$RELATION_NAME = 'PEOPLE'"))
            .ShouldBe(2, "the primary key and the foreign key each have an index");

        var existing = await people().FetchExistingAsync(theConnection);
        existing!.Indexes.ShouldBeEmpty();
        (await people().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task unnamed_constraints_indexes_are_not_indexes_of_the_model_either()
    {
        await ExecuteAsync("""
            CREATE TABLE states (id INTEGER NOT NULL PRIMARY KEY, code VARCHAR(2) UNIQUE);
            CREATE TABLE people (id INTEGER NOT NULL PRIMARY KEY, state_id INTEGER REFERENCES states (id));
            """);

        (await people().FetchExistingAsync(theConnection))!.Indexes.ShouldBeEmpty();
        (await states().FetchExistingAsync(theConnection))!.Indexes.ShouldBeEmpty();
    }

    [Fact]
    public async Task a_declared_index_over_the_foreign_key_columns_is_compared_like_any_other()
    {
        var model = people();
        model.Indexes.Add(new IndexDefinition("idx_people_state") { Columns = ["state_id"] });

        await ApplyAsync(states(), model);

        var existing = await model.FetchExistingAsync(theConnection);
        existing!.Indexes.Single().Name.ShouldBe("IDX_PEOPLE_STATE");

        model.Indexes.Single().IsUnique = true;
        (await model.FindDeltaAsync(theConnection)).Indexes.Different.Count.ShouldBe(1);
    }

    [Fact]
    public async Task dropping_a_foreign_key_takes_its_index_with_it()
    {
        await ApplyAsync(states(), people());

        var keyless = people();
        keyless.ForeignKeys.Clear();
        await ApplyAsync(keyless);

        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$INDICES WHERE RDB$RELATION_NAME = 'PEOPLE'")).ShouldBe(1);
        (await keyless.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     A5: constraint and index names share one namespace in a Firebird database. An index the model
    ///     does not declare is an extra, and extras are dropped before keys are added, so one squatting on
    ///     a key's name is out of the way by the time the key needs it.
    /// </summary>
    [Fact]
    public async Task an_undeclared_index_holding_a_foreign_keys_name_is_dropped_before_the_key_is_added()
    {
        await ApplyAsync(states());
        await ExecuteAsync("CREATE TABLE people (id INTEGER NOT NULL, state_id INTEGER, CONSTRAINT pk_people PRIMARY KEY (id))");
        await ExecuteAsync("CREATE INDEX fk_people_state_id ON people (state_id)");

        await ApplyAsync(people());

        (await people().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$RELATION_CONSTRAINTS WHERE RDB$CONSTRAINT_NAME = 'FK_PEOPLE_STATE_ID'"))
            .ShouldBe(1);
    }

    /// <summary>
    ///     When the index is not Weasel's to drop, the name clash is the server's to report: the guard
    ///     looks for a constraint of that name, finds none, and the ADD fails on the index.
    /// </summary>
    [Fact]
    public async Task an_add_only_table_cannot_add_a_key_whose_name_an_index_holds()
    {
        await ApplyAsync(states());
        await ExecuteAsync("CREATE TABLE people (id INTEGER NOT NULL, state_id INTEGER, CONSTRAINT pk_people PRIMARY KEY (id))");
        await ExecuteAsync("CREATE INDEX fk_people_state_id ON people (state_id)");

        var model = people();
        model.AddOnlyMigrations = true;

        await Should.ThrowAsync<FirebirdSql.Data.FirebirdClient.FbException>(() => ApplyAsync(model));
    }
}
