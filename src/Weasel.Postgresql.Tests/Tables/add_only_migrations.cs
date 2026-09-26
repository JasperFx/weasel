using System.Data.Common;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     weasel#629. <c>AutoCreate.CreateOrUpdate</c> drops the columns, indexes and foreign keys the
///     model no longer declares. That is right when the model is the whole truth about the schema,
///     and wrong when the model was <em>translated</em> from another one: a column the source model
///     knows about and the translator could not express is not "removed", it is "not understood".
///     <see cref="ITable.AddOnlyMigrations" /> is what tells the delta which of the two it is
///     looking at.
/// </summary>
[Collection("add_only")]
public class add_only_migrations : IntegrationContext
{
    public add_only_migrations() : base("add_only")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private sealed class RecordingMigrationLogger: IMigrationLogger
    {
        public List<string> WithheldDrops { get; } = [];

        public void SchemaChange(string sql)
        {
        }

        public void OnFailure(DbCommand command, Exception ex) => throw ex;
        public void WithheldDrop(string description) => WithheldDrops.Add(description);
    }

    private static Table table(bool addOnly, params string[] extraColumns)
    {
        var table = new Table(new PostgresqlObjectName("add_only", "orders")) { AddOnlyMigrations = addOnly };
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("customer");
        foreach (var column in extraColumns) table.AddColumn<string>(column);

        return table;
    }

    [Fact]
    public async Task an_undeclared_column_is_left_alone()
    {
        await CreateSchemaObjectInDatabase(table(addOnly: false, "total_amount"));

        var expected = table(addOnly: true);
        var delta = await expected.FindDeltaAsync(theConnection);

        // not "an Update that writes nothing": the difference is not a difference at all, so
        // nothing downstream has to cope with an empty change set
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.Columns.Extras.ShouldBeEmpty();
        delta.HasChanges().ShouldBeFalse();
    }

    [Fact]
    public async Task the_withheld_drop_is_named()
    {
        await CreateSchemaObjectInDatabase(table(addOnly: false, "total_amount", "total_currency"));

        var delta = await table(addOnly: true).FindDeltaAsync(theConnection);

        delta.WithheldDrops.ShouldBe(["column total_amount", "column total_currency"], ignoreOrder: true);

        var description = AddOnlyMigration.Describe(delta.Expected.Identifier, delta.WithheldDrops);
        description.ShouldContain("total_amount");
        description.ShouldContain("AddOnlyMigrations");
    }

    [Fact]
    public async Task the_column_survives_an_applied_migration()
    {
        await CreateSchemaObjectInDatabase(table(addOnly: false, "total_amount"));

        var expected = table(addOnly: true);
        await CreateSchemaObjectInDatabase(expected);

        var actual = await expected.FetchExistingAsync(theConnection);
        actual!.HasColumn("total_amount").ShouldBeTrue();
    }

    [Fact]
    public async Task additive_changes_still_apply()
    {
        await CreateSchemaObjectInDatabase(table(addOnly: false));

        // add-only means "never remove", not "never change"
        var expected = table(addOnly: true, "shipped_on");
        var delta = await expected.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Missing.Single().Name.ShouldBe("shipped_on");

        await CreateSchemaObjectInDatabase(expected);

        var actual = await expected.FetchExistingAsync(theConnection);
        actual!.HasColumn("shipped_on").ShouldBeTrue();
    }

    [Fact]
    public async Task a_changed_index_is_still_recreated()
    {
        var before = table(addOnly: false);
        before.Indexes.Add(new IndexDefinition("idx_orders_customer") { Columns = ["customer"] });
        await CreateSchemaObjectInDatabase(before);

        // the SAME index, now unique: a declared object that changed, not an undeclared one
        var expected = table(addOnly: true);
        expected.Indexes.Add(new IndexDefinition("idx_orders_customer") { Columns = ["customer"], IsUnique = true });

        var delta = await expected.FindDeltaAsync(theConnection);
        delta.Indexes.Different.ShouldNotBeEmpty();
        delta.WithheldDrops.ShouldBeEmpty();

        await CreateSchemaObjectInDatabase(expected);

        var actual = await expected.FetchExistingAsync(theConnection);
        actual!.IndexFor("idx_orders_customer")!.IsUnique.ShouldBeTrue();
    }

    [Fact]
    public async Task an_undeclared_index_is_left_alone()
    {
        var before = table(addOnly: false);
        before.Indexes.Add(new IndexDefinition("idx_orders_customer") { Columns = ["customer"] });
        await CreateSchemaObjectInDatabase(before);

        var expected = table(addOnly: true);
        var delta = await expected.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.WithheldDrops.ShouldBe(["index idx_orders_customer"]);

        await CreateSchemaObjectInDatabase(expected);
        (await expected.FetchExistingAsync(theConnection))!
            .IndexFor("idx_orders_customer").ShouldNotBeNull();
    }

    [Fact]
    public async Task without_the_flag_the_drop_is_unchanged()
    {
        // the regression guard: this is the behaviour every table defined in code still gets
        await CreateSchemaObjectInDatabase(table(addOnly: false, "total_amount"));

        var expected = table(addOnly: false);
        var delta = await expected.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Extras.Single().Name.ShouldBe("total_amount");
        delta.WithheldDrops.ShouldBeEmpty();

        await CreateSchemaObjectInDatabase(expected);
        (await expected.FetchExistingAsync(theConnection))!.HasColumn("total_amount").ShouldBeFalse();
    }

    [Fact]
    public async Task the_apply_path_logs_the_withheld_drop()
    {
        await CreateSchemaObjectInDatabase(table(addOnly: false, "total_amount"));

        // a real migration: one column to add, one undeclared column to leave alone
        var expected = table(addOnly: true, "shipped_on");
        var logger = await ApplyAsync(expected);

        logger.WithheldDrops.ShouldHaveSingleItem().ShouldContain("column total_amount");
    }

    /// <summary>
    ///     The other half of that decision. A schema whose only difference is a withheld drop has
    ///     nothing to migrate, and warning about it anyway would put the same line in the log on
    ///     every application start for as long as the column exists. So the warning belongs to a
    ///     migration that runs; the quiet case is read from <c>delta.WithheldDrops</c> or from the
    ///     reporting commands, on demand.
    /// </summary>
    [Fact]
    public async Task a_migration_with_nothing_to_do_says_nothing()
    {
        await CreateSchemaObjectInDatabase(table(addOnly: false, "total_amount"));

        var logger = await ApplyAsync(table(addOnly: true));

        logger.WithheldDrops.ShouldBeEmpty();
    }

    private async Task<RecordingMigrationLogger> ApplyAsync(Table expected)
    {
        var delta = await expected.FindDeltaAsync(theConnection);
        var logger = new RecordingMigrationLogger();

        await new PostgresqlMigrator().ApplyAllAsync(theConnection, new SchemaMigration(delta),
            AutoCreate.CreateOrUpdate, logger: logger);

        return logger;
    }
}
