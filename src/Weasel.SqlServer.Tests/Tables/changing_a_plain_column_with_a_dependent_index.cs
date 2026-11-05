using JasperFx.Core;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     The non-computed analogue of weasel#638: ALTER COLUMN on a column a matching index sits on was
///     refused with Msg 5074 because nothing dropped the index first (weasel#777).
/// </summary>
public class changing_a_plain_column_with_a_dependent_index: IntegrationContext
{
    public changing_a_plain_column_with_a_dependent_index(): base("pcdep")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private async Task applyAsync(Table table)
    {
        var migration = new SchemaMigration(await table.FindDeltaAsync(theConnection));
        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, JasperFx.AutoCreate.CreateOrUpdate);
    }

    private static Table terms(string type)
    {
        var table = new Table("pcdep.terms");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn("term", type).NotNull();
        table.Indexes.Add(new IndexDefinition("ix_pcdep_term") { Columns = ["term"] });
        return table;
    }

    [Fact]
    public async Task a_column_carrying_an_index_can_have_its_type_changed()
    {
        await applyAsync(terms("varchar(255)"));

        var widened = terms("nvarchar(255)");
        (await widened.FindDeltaAsync(theConnection)).HasChanges().ShouldBeTrue();

        await applyAsync(widened);

        (await widened.FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();
        var existing = await widened.FetchExistingAsync(theConnection);
        existing!.Indexes.Any(x => x.Name.EqualsIgnoreCase("ix_pcdep_term"))
            .ShouldBeTrue("the index was dropped and never recreated");
    }

    [Fact]
    public async Task an_unrelated_index_is_not_disturbed()
    {
        Table table(string type)
        {
            var t = new Table("pcdep.unrelated");
            t.AddColumn<Guid>("id").AsPrimaryKey();
            t.AddColumn("term", type).NotNull();
            t.AddColumn<string>("label");
            t.Indexes.Add(new IndexDefinition("ix_pcdep_label") { Columns = ["label"] });
            return t;
        }

        await applyAsync(table("varchar(255)"));
        var delta = await table("nvarchar(255)").FindDeltaAsync(theConnection);
        var sql = new StringWriter();
        delta.WriteUpdate(new SqlServerMigrator(), sql);
        sql.ToString().ShouldNotContain("ix_pcdep_label", Case.Insensitive);
    }
}
