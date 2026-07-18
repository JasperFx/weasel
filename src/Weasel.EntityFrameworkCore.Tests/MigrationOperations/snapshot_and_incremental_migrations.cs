using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Shouldly;
using Weasel.Core;
using Weasel.EntityFrameworkCore.Tests.MigrationOperations.SampleGenerated;
using Xunit;
using PgTable = Weasel.Postgresql.Tables.Table;
using PgIndex = Weasel.Postgresql.Tables.IndexDefinition;

namespace Weasel.EntityFrameworkCore.Tests.MigrationOperations;

/// <summary>
///     The serialized schema snapshot + incremental diff (#367): snapshot
///     round-trips, and model changes diff into incremental EF operations
///     without a live database.
/// </summary>
public class snapshot_and_incremental_migrations
{
    private static MigrationOperationTranslationOptions options() => SampleWeaselSchema.TranslationOptions();

    private static EfSchemaSnapshot snapshotOfSample()
        => EfSchemaSnapshot.FromSchemaObjects(SampleWeaselSchema.Objects(), options());

    [Fact]
    public void snapshot_round_trips_through_json_with_zero_diff()
    {
        var snapshot = snapshotOfSample();

        var json = snapshot.ToJson();
        var rehydrated = EfSchemaSnapshot.FromJson(json);

        var diff = EfSnapshotDiffer.Diff(rehydrated, snapshotOfSample(), options());

        diff.HasChanges.ShouldBeFalse();
        diff.UpOperations.ShouldBeEmpty();
        diff.DownOperations.ShouldBeEmpty();
    }

    [Fact]
    public void added_column_diffs_to_add_column_and_drops_on_down()
    {
        var baseline = snapshotOfSample();

        var changed = SampleWeaselSchema.Objects();
        var orders = changed.OfType<PgTable>().Single(x => x.Identifier.Name == "orders");
        orders.AddColumn<string>("tenant_id").NotNull();
        orders.ColumnFor("tenant_id")!.DefaultExpression = "'*DEFAULT*'";

        var diff = EfSnapshotDiffer.Diff(baseline,
            EfSchemaSnapshot.FromSchemaObjects(changed, options()), options());

        var add = diff.UpOperations.OfType<AddColumnOperation>().Single();
        add.Name.ShouldBe("tenant_id");
        add.Table.ShouldBe("orders");
        add.Schema.ShouldBe(SampleWeaselSchema.SchemaName);
        add.IsNullable.ShouldBeFalse();
        add.DefaultValueSql.ShouldBe("'*DEFAULT*'");

        diff.DownOperations.OfType<DropColumnOperation>().Single().Name.ShouldBe("tenant_id");
    }

    [Fact]
    public void changed_index_recreates_it()
    {
        var baseline = snapshotOfSample();

        var changed = SampleWeaselSchema.Objects();
        var orders = changed.OfType<PgTable>().Single(x => x.Identifier.Name == "orders");
        var index = (PgIndex)orders.Indexes.Single(x => x.Name == "idx_orders_status");
        index.Predicate = "status <> 'closed'";

        var diff = EfSnapshotDiffer.Diff(baseline,
            EfSchemaSnapshot.FromSchemaObjects(changed, options()), options());

        diff.UpOperations.OfType<DropIndexOperation>().Single().Name.ShouldBe("idx_orders_status");
        diff.UpOperations.OfType<CreateIndexOperation>().Single().Filter.ShouldBe("status <> 'closed'");

        // rollback restores the original definition
        diff.DownOperations.OfType<CreateIndexOperation>().Single().Filter.ShouldBe("status <> 'archived'");
    }

    [Fact]
    public void added_foreign_key_and_table_diff_together()
    {
        var baseline = snapshotOfSample();

        var changed = SampleWeaselSchema.Objects().ToList();
        var regions = new PgTable($"{SampleWeaselSchema.SchemaName}.regions");
        regions.AddColumn<int>("id").AsPrimaryKey();
        changed.Add(regions);

        var customers = changed.OfType<PgTable>().Single(x => x.Identifier.Name == "customers");
        customers.AddColumn<int>("region_id");
        ((ITable)customers).AddForeignKey("fk_customers_region",
            new DbObjectName(SampleWeaselSchema.SchemaName, "regions"), new[] { "region_id" }, new[] { "id" });

        var diff = EfSnapshotDiffer.Diff(baseline,
            EfSchemaSnapshot.FromSchemaObjects(changed, options()), options());

        diff.UpOperations.OfType<CreateTableOperation>().Single().Name.ShouldBe("regions");
        diff.UpOperations.OfType<AddColumnOperation>().Single().Name.ShouldBe("region_id");
        var fk = diff.UpOperations.OfType<AddForeignKeyOperation>().Single();
        fk.Name.ShouldBe("fk_customers_region");
        fk.PrincipalTable.ShouldBe("regions");

        diff.DownOperations.OfType<DropTableOperation>().Single().Name.ShouldBe("regions");
        diff.DownOperations.OfType<DropForeignKeyOperation>().Single().Name.ShouldBe("fk_customers_region");
    }

    [Fact]
    public void altered_column_produces_alter_column_with_old_definition()
    {
        var baseline = snapshotOfSample();

        var changed = SampleWeaselSchema.Objects();
        var customers = changed.OfType<PgTable>().Single(x => x.Identifier.Name == "customers");
        customers.ColumnFor("name")!.AllowNulls = true;
        customers.ColumnFor("name")!.DefaultExpression = null;

        var diff = EfSnapshotDiffer.Diff(baseline,
            EfSchemaSnapshot.FromSchemaObjects(changed, options()), options());

        var alter = diff.UpOperations.OfType<AlterColumnOperation>().Single();
        alter.Name.ShouldBe("name");
        alter.IsNullable.ShouldBeTrue();
        alter.DefaultValueSql.ShouldBeNull();
        alter.OldColumn.IsNullable.ShouldBeFalse();
        alter.OldColumn.DefaultValueSql.ShouldBe("'unknown'");

        // down restores the baseline definition
        var revert = diff.DownOperations.OfType<AlterColumnOperation>().Single();
        revert.IsNullable.ShouldBeFalse();
        revert.DefaultValueSql.ShouldBe("'unknown'");
    }

    [Fact]
    public void changed_raw_object_is_refused_with_guidance()
    {
        var partitioned = new PgTable("efgen.partitioned");
        partitioned.AddColumn<int>("id").AsPrimaryKey();
        partitioned.AddColumn<string>("tenant_id").AsPrimaryKey();
        partitioned.PartitionByList("tenant_id");

        var opts = options();
        opts.ForceRawSql = o => o is PgTable { Partitioning: not null };

        var baseline = EfSchemaSnapshot.FromSchemaObjects(new ISchemaObject[] { partitioned }, opts);

        var changedTable = new PgTable("efgen.partitioned");
        changedTable.AddColumn<int>("id").AsPrimaryKey();
        changedTable.AddColumn<string>("tenant_id").AsPrimaryKey();
        changedTable.AddColumn<string>("extra");
        changedTable.PartitionByList("tenant_id");

        var target = EfSchemaSnapshot.FromSchemaObjects(new ISchemaObject[] { changedTable }, opts);

        Should.Throw<NotSupportedException>(() => EfSnapshotDiffer.Diff(baseline, target, opts))
            .Message.ShouldContain("live-database baseline");
    }

    [Fact]
    public void incremental_operations_render_through_the_emitter()
    {
        var baseline = snapshotOfSample();

        var changed = SampleWeaselSchema.Objects();
        var orders = changed.OfType<PgTable>().Single(x => x.Identifier.Name == "orders");
        orders.AddColumn<string>("tenant_id").NotNull();
        orders.ColumnFor("tenant_id")!.DefaultExpression = "'*DEFAULT*'";

        var diff = EfSnapshotDiffer.Diff(baseline,
            EfSchemaSnapshot.FromSchemaObjects(changed, options()), options());

        var emission = SampleWeaselSchema.EmissionOptions();
        emission.LastMigrationId = "20260718120000_WeaselSampleSchema";
        var migration = EfMigrationFileEmitter.EmitMigration(
            "AddTenantId", diff.UpOperations, diff.DownOperations, emission);

        migration.MigrationId.ShouldBe("20260718120001_AddTenantId");
        migration.Code.ShouldContain("migrationBuilder.AddColumn<string>(");
        migration.Code.ShouldContain("defaultValueSql: \"'*DEFAULT*'\"");
        migration.Code.ShouldContain("migrationBuilder.DropColumn(");
    }
}
