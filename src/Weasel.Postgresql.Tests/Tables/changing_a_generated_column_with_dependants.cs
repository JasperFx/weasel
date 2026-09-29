using JasperFx.Core;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     The PostgreSQL half of weasel#638: a changed generation expression takes its index with it.
/// </summary>
/// <remarks>
///     <para>
///         Same shape as the SQL Server defect, different symptom. A generation expression cannot be
///         altered in place, so the delta drops and re-adds the column. PostgreSQL does not refuse that
///         drop the way SQL Server does — it <em>succeeds</em> and silently drops every index and
///         constraint that depended on the column. Verified against the server: after
///         <c>ALTER TABLE ... DROP COLUMN gc</c> only the primary key index is left.
///     </para>
///     <para>
///         An index that matches the model is in neither <c>Indexes.Extras</c> nor
///         <c>Indexes.Different</c>, so nothing recreated it. One <c>ApplyAll</c> therefore left the
///         schema not matching the model — the migration was not convergent in a single pass, and the
///         index was simply gone until something ran again.
///     </para>
/// </remarks>
[Collection("generated_dependants")]
public class changing_a_generated_column_with_dependants: IntegrationContext
{
    public changing_a_generated_column_with_dependants(): base("gcdep")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private async Task applyAsync(Table table)
    {
        var migration = new SchemaMigration(await table.FindDeltaAsync(theConnection));
        await new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, JasperFx.AutoCreate.CreateOrUpdate);
    }

    private static Table documents(string path)
    {
        var table = new Table("gcdep.documents");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("data", "jsonb").NotNull();
        table.AddColumn<string>("gc_name").GeneratedAs($"data->>'{path}'");

        table.Indexes.Add(new IndexDefinition("idx_gcdep_name") { Columns = ["gc_name"] });
        return table;
    }

    [Fact]
    public async Task the_index_survives_a_changed_generation_expression()
    {
        await applyAsync(documents("name"));

        var retargeted = documents("full_name");
        (await retargeted.FindDeltaAsync(theConnection)).HasChanges().ShouldBeTrue("the change should be drift");

        await applyAsync(retargeted);

        var existing = await retargeted.FetchExistingAsync(theConnection);
        existing!.Indexes.Any(x => x.Name.EqualsIgnoreCase("idx_gcdep_name"))
            .ShouldBeTrue("PostgreSQL dropped the index with the column and nothing put it back");
    }

    /// <summary>
    ///     The consequence that matters: one apply has to leave the schema matching the model.
    /// </summary>
    [Fact]
    public async Task one_apply_converges()
    {
        await applyAsync(documents("name"));

        var retargeted = documents("full_name");
        await applyAsync(retargeted);

        (await retargeted.FindDeltaAsync(theConnection)).HasChanges()
            .ShouldBeFalse("a single apply did not converge");
    }

    /// <summary>Control: an index that does not touch the generated column is left alone.</summary>
    [Fact]
    public async Task an_unrelated_index_is_not_disturbed()
    {
        Table table(string path)
        {
            var t = new Table("gcdep.unrelated");
            t.AddColumn<int>("id").AsPrimaryKey();
            t.AddColumn("data", "jsonb").NotNull();
            t.AddColumn<string>("label");
            t.AddColumn<string>("gc_name").GeneratedAs($"data->>'{path}'");

            t.Indexes.Add(new IndexDefinition("idx_gcdep_label") { Columns = ["label"] });
            return t;
        }

        await applyAsync(table("name"));

        var retargeted = table("full_name");
        var delta = await retargeted.FindDeltaAsync(theConnection);
        var sql = new StringWriter();
        delta.WriteUpdate(new PostgresqlMigrator(), sql);

        sql.ToString().ShouldNotContain("idx_gcdep_label", Case.Insensitive);

        await applyAsync(retargeted);
        (await retargeted.FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();
    }
}
