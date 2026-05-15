using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     Regression test for the original Marten Bug 550
///     (<c>Bug_550_schema_diff_with_precision</c>) — ported to Weasel as part of
///     #279 because the schema-diff / patch logic now lives in Weasel.
///     <para>
///     The original bug: when a column was configured with an explicit
///     precision-qualified type (e.g. <c>character varying (100)</c>),
///     round-tripping it through PostgreSQL's catalog and diffing against the
///     same configuration produced a spurious
///     <see cref="SchemaPatchDifference" />, because the configured spelling
///     ("character varying (100)" with a space) and the catalog's spelling
///     ("character varying(100)" without one, or <c>varchar</c>) weren't being
///     normalised consistently. The fix made the diff compare normalised type
///     names so identical schemas round-trip cleanly with
///     <see cref="SchemaPatchDifference.None" />.
///     </para>
/// </summary>
[Collection("bug550")]
public class Bug550_SchemaDiffWithPrecision: IntegrationContext
{
    public Bug550_SchemaDiffWithPrecision(): base("bug550")
    {
    }

    [Fact]
    public async Task no_delta_for_explicitly_precision_qualified_column()
    {
        await ResetSchema();

        // The Marten failure looked like: a `character varying (100)` column on
        // the doc storage table came back from the catalog as "character varying"
        // (no precision) or "character varying(100)" (no space), causing a false
        // Update diff. Build the same shape directly against Weasel's Table API.
        var table = new Table(new PostgresqlObjectName(SchemaName, "doc_with_precision"));
        table.AddColumn("id", "uuid").AsPrimaryKey();
        table.AddColumn("data", "jsonb").NotNull();
        table.AddColumn("name", "character varying (100)");

        await CreateSchemaObjectInDatabase(table);

        // Re-fetch the actual table from the catalog and diff it against the
        // same configured Table. If the precision normalisation works, the diff
        // is None; otherwise the `name` column would show up as Different.
        var delta = await table.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeFalse();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
