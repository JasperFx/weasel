using Shouldly;
using Weasel.Core.Tests.Migrations;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     <see cref="Migrator.SplitIntoBatches" /> is the seam <see cref="SchemaMigration.RollbackAllAsync" />
///     runs rendered rollback text through, so SQL Server can split on its <c>GO</c> lines (weasel#593).
///     Every other provider inherits the default, and the default has to leave the text exactly as it
///     was: a dialect where <c>GO</c> is ordinary identifier text must not have it treated as a
///     separator.
/// </summary>
public class migrator_batch_seam
{
    [Fact]
    public void the_default_returns_the_whole_text_as_one_batch()
    {
        var sql = "a;\nGO\nb;";

        new apply_with_retries.FakeMigrator(false).SplitIntoBatches(sql).ShouldBe([sql]);
    }
}
