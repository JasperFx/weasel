using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     weasel#668. SQL Server compiles a whole batch before running any of it, and binds column
///     names against tables that already exist at compile time -- deferred name resolution only
///     covers tables that do not exist yet. <see cref="TableDelta.WriteUpdate" /> writes the missing
///     columns' <c>ALTER TABLE ... ADD</c> and everything that follows into one batch, so a later
///     statement naming a column this delta is adding fails to compile with error 207, "Invalid
///     column name" -- and the <c>ALTER</c> never runs either, because nothing in the batch does.
///     A brand-new table is unaffected: the column is created with the table.
/// </summary>
/// <remarks>
///     The line the server draws is between an <b>expression</b> and a <b>name list</b>, which is
///     why the reported case is a filtered index rather than any index. A predicate, a check
///     constraint and a computed definition are all compiled, so they bind now and fail; the column
///     list of a <c>CREATE INDEX</c>, its <c>INCLUDE</c> list and a foreign key's columns are
///     metadata references that resolve when the statement runs, and those three pass today. They
///     are here so the distinction stays measured rather than assumed, and so a fix that reorders
///     this method cannot quietly break them.
///     <para>
///     SQL Server specific: PostgreSQL analyzes each statement of a multi-statement command as it
///     reaches it, and the same add-column-plus-filtered-index-plus-check pair applies there in one
///     command. Oracle already sends one statement per command, and MySQL and SQLite parse per
///     statement.
///     </para>
/// </remarks>
[Collection("integration")]
public class adding_a_column_and_something_that_references_it: IntegrationContext
{
    public adding_a_column_and_something_that_references_it(): base("addcol")
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await ResetSchema();
    }

    private static Table startingTable(string name)
    {
        var table = new Table($"addcol.{name}");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        return table;
    }

    /// <summary>
    ///     Goes through <see cref="SchemaObjectsExtensions.ApplyChangesAsync" /> rather than running
    ///     the rendered DDL directly, because that is the path Wolverine and Marten take and it is
    ///     the one that already splits on <c>GO</c> (weasel#593).
    /// </summary>
    private async Task assertNoDeltasAfterPatching(Table table)
    {
        await table.ApplyChangesAsync(theConnection);

        var delta = await table.FindDeltaAsync(theConnection);
        if (delta.HasChanges())
        {
            var writer = new StringWriter();
            delta.WriteUpdate(new SqlServerMigrator(), writer);
            throw new Exception("Found these differences:\n\n" + writer);
        }
    }

    /// <summary>The reported failure: Wolverine's dead letter expiry column and its filtered index.</summary>
    [Fact]
    public async Task add_a_column_and_a_filtered_index_on_that_column()
    {
        await CreateSchemaObjectInDatabase(startingTable("filtered"));

        var table = startingTable("filtered");
        table.AddColumn<DateTimeOffset>("expires");

        var index = new IndexDefinition("idx_filtered_expires") { Predicate = "[expires] IS NOT NULL" };
        index.AgainstColumns("expires");
        table.Indexes.Add(index);

        await assertNoDeltasAfterPatching(table);
    }

    /// <summary>A check constraint's expression binds the same way. Not in the report; same cause.</summary>
    [Fact]
    public async Task add_a_column_and_a_check_constraint_on_that_column()
    {
        await CreateSchemaObjectInDatabase(startingTable("checked"));

        var table = startingTable("checked");
        table.AddColumn<int>("rating");
        table.CheckConstraints.Add(new TableCheckConstraint("ck_checked_rating", "[rating] > 0"));

        await assertNoDeltasAfterPatching(table);
    }

    /// <summary>
    ///     Two statements inside the missing-columns pass alone are enough, so a separator between
    ///     that pass and the rest would not cover this one.
    /// </summary>
    [Fact]
    public async Task add_a_column_and_a_computed_column_derived_from_it()
    {
        await CreateSchemaObjectInDatabase(startingTable("computed"));

        var table = startingTable("computed");
        table.AddColumn<int>("quantity");
        table.AddColumn<int>("doubled").ComputedAs("[quantity] * 2");

        await assertNoDeltasAfterPatching(table);
    }

    /// <summary>Passes today -- an index's key column list resolves at execution time.</summary>
    [Fact]
    public async Task add_a_column_and_a_plain_index_on_that_column()
    {
        await CreateSchemaObjectInDatabase(startingTable("docs"));

        var table = startingTable("docs");
        table.AddColumn<DateTimeOffset>("expires");

        var index = new IndexDefinition("idx_docs_expires");
        index.AgainstColumns("expires");
        table.Indexes.Add(index);

        await assertNoDeltasAfterPatching(table);
    }

    /// <summary>Passes today -- so does an INCLUDE list.</summary>
    [Fact]
    public async Task add_a_column_and_an_index_that_includes_it()
    {
        await CreateSchemaObjectInDatabase(startingTable("included"));

        var table = startingTable("included");
        table.AddColumn<DateTimeOffset>("expires");

        var index = new IndexDefinition("idx_included_name");
        index.AgainstColumns("name");
        index.IncludedColumns = ["expires"];
        table.Indexes.Add(index);

        await assertNoDeltasAfterPatching(table);
    }

    /// <summary>Passes today -- so does a foreign key's column list.</summary>
    [Fact]
    public async Task add_a_column_and_a_foreign_key_on_that_column()
    {
        var parent = new Table("addcol.parents");
        parent.AddColumn<int>("id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(parent);

        await CreateSchemaObjectInDatabase(startingTable("children"));

        var table = startingTable("children");
        table.AddColumn<int>("parent_id");
        table.ForeignKeys.Add(new ForeignKey("fk_children_parent")
        {
            ColumnNames = ["parent_id"],
            LinkedNames = ["id"],
            LinkedTable = parent.Identifier
        });

        await assertNoDeltasAfterPatching(table);
    }
}
