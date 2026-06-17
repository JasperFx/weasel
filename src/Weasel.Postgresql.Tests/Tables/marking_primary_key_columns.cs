using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

public class marking_primary_key_columns
{
    // #307: under concurrent DDL the multi-result-set catalog read in Table.readExistingAsync can be
    // torn, so the primary-key query returns a column name the columns query did not (yet) include.
    // markPrimaryKeyColumns must skip the missing column instead of dereferencing null and throwing a
    // NullReferenceException.
    [Fact]
    public void ignores_pk_names_with_no_matching_column()
    {
        var table = new Table("public.mt_thing");
        table.AddColumn("id", "uuid");
        table.AddColumn("tenant_id", "varchar");

        // "ghost" has no matching column — pre-fix this threw a NullReferenceException.
        Should.NotThrow(() =>
            Table.markPrimaryKeyColumns(table, new[] { "id", "tenant_id", "ghost" }));

        table.ColumnFor("id")!.IsPrimaryKey.ShouldBeTrue();
        table.ColumnFor("tenant_id")!.IsPrimaryKey.ShouldBeTrue();
        table.ColumnFor("ghost").ShouldBeNull();
    }

    [Fact]
    public void marks_only_the_present_primary_key_columns()
    {
        var table = new Table("public.mt_thing");
        table.AddColumn("id", "uuid");
        table.AddColumn("name", "varchar");

        Table.markPrimaryKeyColumns(table, new[] { "id" });

        table.ColumnFor("id")!.IsPrimaryKey.ShouldBeTrue();
        table.ColumnFor("name")!.IsPrimaryKey.ShouldBeFalse();
    }
}
