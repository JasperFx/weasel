using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     A computed column declared with <c>CAST</c> could never reconcile, because SQL Server stores it
///     as <c>CONVERT</c>.
/// </summary>
/// <remarks>
///     <para>
///         Worse than churn: a computed definition cannot be altered in place, so the delta correctly
///         emits <c>DROP COLUMN</c> + <c>ADD</c>. That <c>DROP</c> fails as soon as anything depends on
///         the column, so a CAST-declared computed column carrying an index or a foreign key did not
///         merely re-run DDL forever — the migration failed permanently, naming a dependency rather
///         than the expression that actually differed (weasel#637).
///     </para>
///     <para>
///         Found moving Polecat's document indexes onto Weasel schema objects, where every declaration
///         is a persisted computed column over <c>JSON_VALUE</c>.
///     </para>
/// </remarks>
public class computed_columns_declared_with_cast: IntegrationContext
{
    public computed_columns_declared_with_cast(): base("castcc")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private async Task applyAsync(Table table)
    {
        var migration = new SchemaMigration(await table.FindDeltaAsync(theConnection));
        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, JasperFx.AutoCreate.CreateOrUpdate);
    }

    private static Table documents(string customerIdExpression)
    {
        var table = new Table("castcc.documents");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn("data", "nvarchar(max)").NotNull();
        table.AddColumn("cc_customerid", "uniqueidentifier").ComputedAs(customerIdExpression, persisted: true);
        return table;
    }

    /// <summary>The reported failure, end to end against the server.</summary>
    [Fact]
    public async Task a_cast_declared_computed_column_settles()
    {
        var table = documents("CAST(JSON_VALUE(data, '$.customerId') AS uniqueidentifier)");

        await applyAsync(table);

        var after = await table.FindDeltaAsync(theConnection);
        after.HasChanges().ShouldBeFalse("the CAST column reported a difference against itself");
    }

    /// <summary>
    ///     And it stays settled — the second apply is the one that used to emit the DROP/ADD pair
    ///     forever.
    /// </summary>
    [Fact]
    public async Task a_cast_declared_computed_column_stays_settled_over_repeated_migrations()
    {
        var table = documents("CAST(JSON_VALUE(data, '$.customerId') AS uniqueidentifier)");

        await applyAsync(table);
        await applyAsync(table);
        await applyAsync(table);

        (await table.FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();
    }

    /// <summary>
    ///     What the catalog actually reports, so the rewrite is pinned against the server rather than
    ///     against an assumption about it.
    /// </summary>
    [Fact]
    public async Task the_catalog_reports_the_cast_back_as_a_convert()
    {
        var table = documents("CAST(JSON_VALUE(data, '$.customerId') AS uniqueidentifier)");
        await applyAsync(table);

        var existing = await table.FetchExistingAsync(theConnection);
        var stored = existing!.ColumnFor("cc_customerid")!.ComputedExpression!;

        stored.ShouldContain("CONVERT", Case.Insensitive);
        stored.ShouldNotContain("CAST", Case.Insensitive);
    }

    /// <summary>The spelling that already reconciled has to keep reconciling.</summary>
    [Fact]
    public async Task a_convert_declared_computed_column_still_settles()
    {
        var table = new Table("castcc.converted");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn("data", "nvarchar(max)").NotNull();
        table.AddColumn("cc_placed", "datetimeoffset")
            .ComputedAs("CONVERT(datetimeoffset, JSON_VALUE(data, '$.placed'), 126)", persisted: true);

        await applyAsync(table);

        (await table.FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();
    }

    /// <summary>
    ///     The rewrite must not fold away a real difference: a genuinely changed cast target is still
    ///     drift, and still converges once applied.
    /// </summary>
    [Fact]
    public async Task a_genuinely_changed_cast_is_still_detected()
    {
        var table = new Table("castcc.widths");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn("data", "nvarchar(max)").NotNull();
        table.AddColumn("cc_name", "varchar(250)")
            .ComputedAs("CAST(JSON_VALUE(data, '$.name') AS varchar(250))", persisted: true);

        await applyAsync(table);

        var widened = new Table("castcc.widths");
        widened.AddColumn<Guid>("id").AsPrimaryKey();
        widened.AddColumn("data", "nvarchar(max)").NotNull();
        widened.AddColumn("cc_name", "varchar(500)")
            .ComputedAs("CAST(JSON_VALUE(data, '$.name') AS varchar(500))", persisted: true);

        (await widened.FindDeltaAsync(theConnection)).HasChanges()
            .ShouldBeTrue("a widened cast target should still be drift");

        await applyAsync(widened);
        (await widened.FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();
    }
}
