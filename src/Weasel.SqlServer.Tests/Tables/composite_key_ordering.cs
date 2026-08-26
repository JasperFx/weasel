using Shouldly;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     Column order is part of a composite key's identity: (a, b) and (b, a) are different indexes
///     with different query plans, and a foreign key's two column lists are positionally paired. The
///     catalog queries either had no ORDER BY at all or ordered by the wrong column, so all three
///     kinds could come back reordered.
/// </summary>
public class composite_key_ordering: IntegrationContext
{
    public composite_key_ordering(): base("ordering")
    {
    }

    [Fact]
    public async Task composite_primary_key_keeps_its_declared_key_order()
    {
        await ResetSchema();

        // Column order is a, b, c but the key is declared (c, a).
        await theConnection.CreateCommand(
                "create table ordering.composite_pk (a int not null, b int not null, c int not null, constraint pk_composite primary key (c, a))")
            .ExecuteNonQueryAsync();

        var existing = await new Table("ordering.composite_pk").FetchExistingAsync(theConnection);

        existing!.PrimaryKeyColumns.ShouldBe(["c", "a"]);
        existing.PrimaryKeyName.ShouldBe("pk_composite");
    }

    [Fact]
    public async Task composite_index_keeps_its_key_order()
    {
        await ResetSchema();

        // Key order is the reverse of the table's column order, which is what index_column_id
        // reported and key_ordinal gets right.
        await theConnection.CreateCommand(
                "create table ordering.reading (id int not null constraint pk_reading primary key, a int not null, b int not null)")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand("create index ix_reading on ordering.reading (b, a)")
            .ExecuteNonQueryAsync();

        var existing = await new Table("ordering.reading").FetchExistingAsync(theConnection);

        existing!.IndexFor("ix_reading")!.Columns.ShouldBe(["b", "a"]);
    }

    [Fact]
    public async Task included_columns_are_not_mixed_into_the_key()
    {
        await ResetSchema();

        await theConnection.CreateCommand(
                "create table ordering.covering (id int not null constraint pk_covering primary key, a int not null, b int not null, note varchar(20) null)")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand("create index ix_covering on ordering.covering (b, a) include (note)")
            .ExecuteNonQueryAsync();

        var existing = await new Table("ordering.covering").FetchExistingAsync(theConnection);

        var index = existing!.IndexFor("ix_covering")!;
        index.Columns.ShouldBe(["b", "a"]);
        index.IncludedColumns.ShouldBe(["note"]);
    }

    [Fact]
    public async Task composite_foreign_key_pairs_the_right_columns()
    {
        await ResetSchema();

        await theConnection.CreateCommand(
                "create table ordering.fk_parent (x int not null, y int not null, constraint pk_parent primary key (x, y))")
            .ExecuteNonQueryAsync();

        // Chosen so that sorting each side independently mispairs them: the child columns sort to
        // (a, b) while the parent columns stay (x, y), which would claim a -> x. The truth is
        // b -> x and a -> y.
        await theConnection.CreateCommand(
                "create table ordering.fk_child (b int not null, a int not null, constraint fk_child_parent foreign key (b, a) references ordering.fk_parent (x, y))")
            .ExecuteNonQueryAsync();

        var existing = await new Table("ordering.fk_child").FetchExistingAsync(theConnection);

        var fk = existing!.ForeignKeys.Single();
        fk.ColumnNames.ShouldBe(["b", "a"]);
        fk.LinkedNames.ShouldBe(["x", "y"]);
    }

    [Fact]
    public async Task a_single_column_key_is_unaffected()
    {
        await ResetSchema();

        var table = new Table("ordering.simple");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").AllowNulls();
        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);

        existing!.PrimaryKeyColumns.ShouldBe(["id"]);
    }
}
