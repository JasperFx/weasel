using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     SQL Server identifiers are case-insensitive under the default collation, so a key that
///     differs from the declaration only by case is the same key (weasel#779). It used to be
///     dropped and re-added on every ensure.
/// </summary>
public class primary_key_comparison_ignores_case
{
    private static Table build(string name, params string[] keys)
    {
        var table = new Table("dbo.pk_case");
        foreach (var key in keys) table.AddColumn<int>(key).AsPrimaryKey();
        table.AddColumn<string>("name");
        return table;
    }

    [Fact]
    public void keys_differing_only_by_case_are_no_difference()
    {
        var delta = new TableDelta(build("expected", "Id", "Tenant"), build("actual", "id", "tenant"));

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void a_genuinely_different_key_is_still_an_update()
    {
        var delta = new TableDelta(build("expected", "Id", "Tenant"), build("actual", "id", "other"));

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);
    }
}
