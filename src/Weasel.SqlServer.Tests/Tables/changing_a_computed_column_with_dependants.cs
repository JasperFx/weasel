using JasperFx.Core;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     A changed computed-column definition was dropped without dropping the index or foreign key that
///     depended on it, so SQL Server refused the drop and the migration could not be applied at all.
/// </summary>
/// <remarks>
///     <para>
///         <c>WriteUpdate</c> handles a changed computed definition by dropping and re-adding the
///         column, which is right — the data is derived, so it is lossless. Before the column change it
///         dropped <c>Indexes.Extras</c> and <c>Indexes.Different</c>. An index that <em>matches</em>
///         the model is in neither set, and no foreign key was dropped at any point, so:
///     </para>
///     <code>
///     The index 'ix_cc' is dependent on column 'cc'.
///     Msg 4922 ... ALTER TABLE DROP COLUMN cc failed because one or more objects access this column.
///     </code>
///     <para>
///         Reachable whenever a computed expression legitimately changes while an index or foreign key
///         sits on the column — widening a declared type, a serializer naming-policy change that
///         retargets the JSON path, or switching to the native <c>json</c> column type. Each is an
///         ordinary configuration change (weasel#638).
///     </para>
/// </remarks>
public class changing_a_computed_column_with_dependants: IntegrationContext
{
    public changing_a_computed_column_with_dependants(): base("ccdep")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private async Task applyAsync(Table table)
    {
        var migration = new SchemaMigration(await table.FindDeltaAsync(theConnection));
        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, JasperFx.AutoCreate.CreateOrUpdate);
    }

    /// <summary>A table whose computed column carries an index that matches the model either side.</summary>
    private static Table indexed(string width)
    {
        var table = new Table("ccdep.documents");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn("data", "nvarchar(max)").NotNull();
        table.AddColumn("cc_name", $"varchar({width})")
            .ComputedAs($"CONVERT(varchar({width}), JSON_VALUE(data, '$.name'))", persisted: true);

        table.Indexes.Add(new IndexDefinition("ix_ccdep_name") { Columns = ["cc_name"] });
        return table;
    }

    /// <summary>The reported failure: the migration could not be applied at all.</summary>
    [Fact]
    public async Task a_computed_column_carrying_an_index_can_be_changed()
    {
        await applyAsync(indexed("250"));

        var widened = indexed("500");
        (await widened.FindDeltaAsync(theConnection)).HasChanges().ShouldBeTrue("the widening should be drift");

        await applyAsync(widened);

        (await widened.FindDeltaAsync(theConnection)).HasChanges()
            .ShouldBeFalse("the table did not converge after the change");
    }

    /// <summary>And the index is really back afterwards, not silently lost with the column.</summary>
    [Fact]
    public async Task the_index_is_recreated_after_the_column_is()
    {
        await applyAsync(indexed("250"));
        await applyAsync(indexed("500"));

        var existing = await indexed("500").FetchExistingAsync(theConnection);
        existing!.Indexes.Any(x => x.Name.EqualsIgnoreCase("ix_ccdep_name"))
            .ShouldBeTrue("the index was dropped and never recreated");
    }

    /// <summary>The foreign key variant, which produced the same refusal.</summary>
    [Fact]
    public async Task a_computed_column_carrying_a_foreign_key_can_be_changed()
    {
        var customers = new Table("ccdep.customers");
        customers.AddColumn<Guid>("id").AsPrimaryKey();
        await applyAsync(customers);

        Table documents(string path)
        {
            var table = new Table("ccdep.orders");
            table.AddColumn<Guid>("id").AsPrimaryKey();
            table.AddColumn("data", "nvarchar(max)").NotNull();
            table.AddColumn("cc_customerid", "uniqueidentifier")
                .ComputedAs($"CONVERT(uniqueidentifier, JSON_VALUE(data, '{path}'))", persisted: true);

            table.ForeignKeys.Add(new ForeignKey("fk_ccdep_orders_customer")
            {
                ColumnNames = ["cc_customerid"],
                LinkedNames = ["id"],
                LinkedTable = customers.Identifier
            });
            return table;
        }

        await applyAsync(documents("$.customerId"));

        var retargeted = documents("$.customer_id");
        (await retargeted.FindDeltaAsync(theConnection)).HasChanges().ShouldBeTrue();

        await applyAsync(retargeted);

        (await retargeted.FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();

        var existing = await retargeted.FetchExistingAsync(theConnection);
        existing!.ForeignKeys.Any(x => x.Name.EqualsIgnoreCase("fk_ccdep_orders_customer"))
            .ShouldBeTrue("the foreign key was dropped and never recreated");
    }

    /// <summary>
    ///     An index whose key is only <em>included</em> on the computed column depends on it just the
    ///     same.
    /// </summary>
    [Fact]
    public async Task an_index_including_the_computed_column_is_handled_too()
    {
        Table table(string width)
        {
            var t = new Table("ccdep.included");
            t.AddColumn<Guid>("id").AsPrimaryKey();
            t.AddColumn("data", "nvarchar(max)").NotNull();
            t.AddColumn("cc_name", $"varchar({width})")
                .ComputedAs($"CONVERT(varchar({width}), JSON_VALUE(data, '$.name'))", persisted: true);

            t.Indexes.Add(new IndexDefinition("ix_ccdep_included")
            {
                Columns = ["id"], IncludedColumns = ["cc_name"]
            });
            return t;
        }

        await applyAsync(table("250"));
        await applyAsync(table("500"));

        (await table("500").FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();
    }

    /// <summary>
    ///     A control on the other side: an index that does not touch the computed column is left
    ///     alone, not dropped and recreated for nothing.
    /// </summary>
    [Fact]
    public async Task an_unrelated_index_is_not_disturbed()
    {
        Table table(string width)
        {
            var t = new Table("ccdep.unrelated");
            t.AddColumn<Guid>("id").AsPrimaryKey();
            t.AddColumn("data", "nvarchar(max)").NotNull();
            t.AddColumn<string>("label");
            t.AddColumn("cc_name", $"varchar({width})")
                .ComputedAs($"CONVERT(varchar({width}), JSON_VALUE(data, '$.name'))", persisted: true);

            t.Indexes.Add(new IndexDefinition("ix_ccdep_label") { Columns = ["label"] });
            return t;
        }

        await applyAsync(table("250"));

        var widened = table("500");
        var delta = await widened.FindDeltaAsync(theConnection);
        var sql = new StringWriter();
        delta.WriteUpdate(new SqlServerMigrator(), sql);

        sql.ToString().ShouldNotContain("ix_ccdep_label", Case.Insensitive,
            "an index that does not touch the computed column should not be dropped");

        await applyAsync(widened);
        (await widened.FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();
    }

    /// <summary>
    ///     And an ordinary update with no computed change emits nothing extra — the new pass is inert
    ///     unless a computed definition actually changed.
    /// </summary>
    [Fact]
    public async Task an_update_with_no_computed_change_is_unaffected()
    {
        await applyAsync(indexed("250"));

        var extended = indexed("250");
        extended.AddColumn<string>("added_later");

        var delta = await extended.FindDeltaAsync(theConnection);
        var sql = new StringWriter();
        delta.WriteUpdate(new SqlServerMigrator(), sql);

        sql.ToString().ShouldNotContain("drop index", Case.Insensitive);

        await applyAsync(extended);
        (await extended.FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();
    }
}
