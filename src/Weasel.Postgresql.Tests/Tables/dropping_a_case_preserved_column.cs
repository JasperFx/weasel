using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     weasel#627. A column the model no longer declares is dropped from the ACTUAL column read
///     back from the catalog, and that column used to be built with the name folded to lowercase.
///     So an <c>Update</c> delta against a table with quoted, mixed-case columns -- what every
///     EF Core-derived model produces, since <c>MapToTable</c> sets
///     <see cref="ITable.PreserveIdentifierCase" /> -- rendered
///     <c>drop column total_amount</c> for a column PostgreSQL calls <c>"Total_Amount"</c>.
/// </summary>
/// <remarks>
///     Two ways that goes wrong, and the first one is the lucky one: normally it fails with
///     42703 and the whole migration dies. But <c>total_amount</c> is a perfectly good identifier,
///     so if the table happens to hold a lowercase twin as well, the drop succeeds -- against the
///     wrong column.
/// </remarks>
[Collection("drop_case")]
public class dropping_a_case_preserved_column : IntegrationContext
{
    public dropping_a_case_preserved_column() : base("drop_case")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private Table caseSensitiveTable(params string[] columns)
    {
        var table = new Table(new PostgresqlObjectName("drop_case", "Orders")) { PreserveIdentifierCase = true };
        table.AddColumn<Guid>("Id").AsPrimaryKey();
        foreach (var column in columns) table.AddColumn<string>(column);

        return table;
    }

    [Fact]
    public async Task renders_the_drop_with_the_identifier_the_catalog_holds()
    {
        await CreateSchemaObjectInDatabase(caseSensitiveTable("Customer", "Total_Amount"));

        // the same table, minus one column: Total_Amount is now an extra
        var expected = caseSensitiveTable("Customer");
        var delta = await expected.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Extras.Single().Name.ShouldBe("Total_Amount");

        var writer = new StringWriter();
        delta.WriteUpdate(new PostgresqlMigrator(), writer);

        writer.ToString().ShouldContain("""drop column "Total_Amount";""");
    }

    [Fact]
    public async Task applies_the_drop_against_a_case_sensitive_table()
    {
        await CreateSchemaObjectInDatabase(caseSensitiveTable("Customer", "Total_Amount"));

        // the drop used to fail here with 42703: column "total_amount" does not exist
        var expected = caseSensitiveTable("Customer");
        await CreateSchemaObjectInDatabase(expected);

        var actual = await expected.FetchExistingAsync(theConnection);
        actual!.Columns.Select(x => x.Name).ShouldBe(["Id", "Customer"]);

        // and the migration is now settled
        var delta = await expected.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The issue suspected that a table holding both spellings would let a folded
    ///     <c>drop column total_amount</c> destroy the wrong column's data. It does not: name
    ///     pairing is deliberately case-insensitive (weasel#224 -- PostgreSQL folds unquoted
    ///     identifiers, so nothing downstream can tell "Id" from id), which means such a table is
    ///     refused outright by <c>ItemDelta</c> rather than migrated wrongly. Unchanged by
    ///     weasel#627, and pinned here so the distinction is not rediscovered.
    /// </summary>
    [Fact]
    public async Task a_table_carrying_both_spellings_is_refused_rather_than_migrated()
    {
        await CreateSchemaObjectInDatabase(caseSensitiveTable("Customer", "Total_Amount", "total_amount"));

        var expected = caseSensitiveTable("Customer", "total_amount");

        await Should.ThrowAsync<ArgumentException>(() => expected.FindDeltaAsync(theConnection));
    }

    [Fact]
    public async Task reads_the_catalog_spelling_back_onto_the_existing_table()
    {
        var table = caseSensitiveTable("Customer", "Total_Amount");
        await CreateSchemaObjectInDatabase(table);

        var actual = await table.FetchExistingAsync(theConnection);

        // the point of the fix: the actual column carries the name PostgreSQL stored, not a
        // folded one, so anything rendered FROM the actual side names a real column
        actual!.Columns.Select(x => x.Name).ShouldBe(["Id", "Customer", "Total_Amount"]);
        actual.ColumnFor("Total_Amount")!.QuotedName.ShouldBe("\"Total_Amount\"");

        // and the case-insensitive pairing that delta detection depends on still holds
        actual.ColumnFor("total_amount").ShouldNotBeNull();
        table.FindDeltaAsync(theConnection).GetAwaiter().GetResult()
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_lowercase_table_reads_back_exactly_as_before()
    {
        // the regression guard for everything Weasel created itself: unquoted DDL means the
        // catalog reports lowercase anyway, so preserving the catalog's spelling is a no-op
        var table = new Table(new PostgresqlObjectName("drop_case", "plain_orders"));
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("Customer");

        await CreateSchemaObjectInDatabase(table);

        var actual = await table.FetchExistingAsync(theConnection);
        actual!.Columns.Select(x => x.Name).ShouldBe(["id", "customer"]);
    }
}
