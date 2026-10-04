using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     Defaults and nullability are compared only when the table asks for it with
///     <see cref="ITable.DetectColumnDrift" />, as on PostgreSQL and SQL Server, and <c>DEFAULT NULL</c>
///     in any spelling is no default at all -- which is how Quartz writes "none".
/// </summary>
public class detecting_column_defaults: IntegrationContext
{
    private static Table jobs(Action<Table.ColumnExpression> configure, bool detect = true)
    {
        var table = new Table("jobs");
        table.DetectColumnDrift = detect;
        table.AddColumn<int>("id").AsPrimaryKey();
        configure(table.AddColumn<int>("state"));
        return table;
    }

    [Theory]
    [InlineData("DEFAULT NULL")]
    [InlineData("default null")]
    [InlineData("DEFAULT  NULL")]
    [InlineData("")]
    public async Task no_default_in_any_spelling_reads_back_as_no_default(string clause)
    {
        await ExecuteAsync($"CREATE TABLE jobs (id INTEGER NOT NULL, state INTEGER {clause}, CONSTRAINT pk_jobs PRIMARY KEY (id))");

        foreach (var model in new[] { jobs(_ => { }), jobs(x => x.DefaultValueByExpression("NULL")) })
        {
            (await model.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        }
    }

    [Theory]
    [InlineData("DEFAULT 0", "0")]
    [InlineData("default    0", "0")]
    [InlineData("DEFAULT 0", "  0 ")]
    public async Task a_default_matches_however_it_was_spelled(string clause, string expression)
    {
        await ExecuteAsync($"CREATE TABLE jobs (id INTEGER NOT NULL, state INTEGER {clause}, CONSTRAINT pk_jobs PRIMARY KEY (id))");

        (await jobs(x => x.DefaultValueByExpression(expression)).FindDeltaAsync(theConnection)).Difference
            .ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_default_is_ignored_without_drift_detection()
    {
        await CreateSchemaObjectInDatabase(jobs(x => x.DefaultValue(0)));

        (await jobs(x => x.DefaultValue(1), detect: false).FindDeltaAsync(theConnection)).Difference
            .ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_new_default_is_set()
    {
        await CreateSchemaObjectInDatabase(jobs(_ => { }));

        var model = jobs(x => x.DefaultValue(5));
        (await model.FindDeltaAsync(theConnection)).Columns.Different.Count.ShouldBe(1);

        await ApplyAsync(model);

        (await model.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        await ExecuteAsync("INSERT INTO jobs (id) VALUES (1)");
        (await ScalarAsync<int>("SELECT state FROM jobs")).ShouldBe(5);
    }

    [Fact]
    public async Task a_changed_default_is_altered()
    {
        await CreateSchemaObjectInDatabase(jobs(x => x.DefaultValue(0)));

        var model = jobs(x => x.DefaultValue(7));
        await ApplyAsync(model);

        (await model.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_default_the_model_no_longer_has_is_dropped()
    {
        await CreateSchemaObjectInDatabase(jobs(x => x.DefaultValue(0)));

        var model = jobs(_ => { });
        await ApplyAsync(model);

        (await model.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await ListAsync("SELECT RDB$DEFAULT_SOURCE FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = 'JOBS' AND RDB$FIELD_NAME = 'STATE' AND RDB$DEFAULT_SOURCE IS NOT NULL"))
            .ShouldBeEmpty();
    }

    [Fact]
    public async Task nullability_is_set_and_dropped()
    {
        await CreateSchemaObjectInDatabase(jobs(_ => { }));

        var notNull = jobs(x => x.NotNull());
        (await notNull.FindDeltaAsync(theConnection)).Columns.Different.Count.ShouldBe(1);
        await ApplyAsync(notNull);
        (await notNull.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        var nullable = jobs(x => x.AllowNulls());
        await ApplyAsync(nullable);
        (await nullable.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task nullability_is_ignored_without_drift_detection()
    {
        await CreateSchemaObjectInDatabase(jobs(_ => { }));

        (await jobs(x => x.NotNull(), detect: false).FindDeltaAsync(theConnection)).Difference
            .ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Measured on 3, 4 and 5: a NOT NULL column with a default fills the rows already there.
    /// </summary>
    [Fact]
    public async Task an_added_not_null_column_with_a_default_fills_existing_rows()
    {
        await CreateSchemaObjectInDatabase(jobs(_ => { }));
        await ExecuteAsync("INSERT INTO jobs (id) VALUES (1)");

        var model = jobs(_ => { });
        model.AddColumn<int>("priority").NotNull().DefaultValue(5);
        await ApplyAsync(model);

        (await ScalarAsync<int>("SELECT priority FROM jobs")).ShouldBe(5);
    }

    /// <summary>
    ///     A4, and worth knowing: a nullable column added with a default leaves the rows already there
    ///     NULL. Only rows inserted afterwards get the default.
    /// </summary>
    [Fact]
    public async Task an_added_nullable_column_with_a_default_leaves_existing_rows_null()
    {
        await CreateSchemaObjectInDatabase(jobs(_ => { }));
        await ExecuteAsync("INSERT INTO jobs (id) VALUES (1)");

        var model = jobs(_ => { });
        model.AddColumn<int>("priority").DefaultValue(5);
        await ApplyAsync(model);
        await ExecuteAsync("INSERT INTO jobs (id) VALUES (2)");

        (await ListAsync("SELECT COALESCE(CAST(priority AS VARCHAR(5)), 'null') FROM jobs ORDER BY id"))
            .ShouldBe(["null", "5"]);
    }

    [Fact]
    public async Task a_domain_default_and_not_null_are_read_through_the_column()
    {
        await ExecuteAsync("""
            CREATE DOMAIN d_state AS INTEGER DEFAULT 3 NOT NULL;
            CREATE TABLE jobs (id INTEGER NOT NULL, state d_state, CONSTRAINT pk_jobs PRIMARY KEY (id));
            """);

        var existing = await jobs(_ => { }).FetchExistingAsync(theConnection);
        var state = existing!.ColumnFor("state")!;

        state.AllowNulls.ShouldBeFalse();
        state.DefaultExpression.ShouldBe("3");
    }
}
