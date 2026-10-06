using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     Table level storage parameters: CREATE ... WITH (...), reading pg_class.reloptions back, and
///     comparing only the parameters the model declares.
/// </summary>
[Collection("table_storage_parameters")]
public class table_storage_parameters: IntegrationContext
{
    public table_storage_parameters(): base("table_storage_parameters")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private static Table newTable(Action<Table>? configure = null)
    {
        var table = new Table("table_storage_parameters.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        configure?.Invoke(table);
        return table;
    }

    private static string[] ddlLinesFor(Table table)
    {
        var writer = new StringWriter();
        table.WriteCreateStatement(new PostgresqlMigrator(), writer);
        return writer.ToString().Split(["\r\n", "\n"], StringSplitOptions.None);
    }

    private static string[] partitionLines(Table table)
        => ddlLinesFor(table)
            .Where(x => x.Contains("PARTITION OF", StringComparison.OrdinalIgnoreCase)).ToArray();

    private async Task<TableDelta> deltaFor(Table expected)
        => (TableDelta)await expected.FindDeltaAsync(theConnection);

    private static string updateScript(TableDelta delta)
    {
        var writer = new StringWriter();
        delta.WriteUpdate(new PostgresqlMigrator(), writer);
        return writer.ToString();
    }

    private static string rollbackScript(TableDelta delta)
    {
        var writer = new StringWriter();
        delta.WriteRollback(new PostgresqlMigrator(), writer);
        return writer.ToString();
    }

    [Fact]
    public void no_storage_parameters_writes_no_with_clause()
    {
        ddlLinesFor(newTable()).ShouldNotContain(x => x.Contains("WITH", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void create_statement_writes_the_with_clause_after_the_column_list()
    {
        var table = newTable(t =>
        {
            t.FillFactor = 70;
            t.StorageParameters["autovacuum_vacuum_scale_factor"] = "0.05";
        });

        ddlLinesFor(table).ShouldContain(") WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.05);");
    }

    [Fact]
    public void fill_factor_is_a_view_over_the_storage_parameters()
    {
        var table = newTable();
        table.FillFactor.ShouldBeNull();

        table.FillFactor = 80;
        table.StorageParameters["fillfactor"].ShouldBe(80);

        table.FillFactor = null;
        table.StorageParameters.Count.ShouldBe(0);
    }

    [Fact]
    public void toast_parameters_are_rejected()
    {
        var table = newTable(t => t.StorageParameters["toast.autovacuum_enabled"] = "false");

        var exception = Should.Throw<InvalidOperationException>(() => ddlLinesFor(table));
        exception.Message.ShouldContain("toast.autovacuum_enabled");
    }

    [Fact]
    public void partitioned_parent_does_not_get_the_with_clause_but_every_partition_does()
    {
        var table = newTable(t =>
        {
            t.FillFactor = 70;
            t.AddColumn<string>("role")
                .PartitionByListValues()
                .AddPartition("admin", "admin")
                .AddPartition("super", "super");
        });

        ddlLinesFor(table).ShouldNotContain(x => x.StartsWith(") WITH"));

        var partitions = partitionLines(table);
        partitions.Length.ShouldBe(3); // two declared + default
        partitions.ShouldAllBe(x => x.Contains("WITH (fillfactor = 70);"));
    }

    [Fact]
    public void range_and_hash_partitions_carry_the_parameters_too()
    {
        var range = newTable(t =>
        {
            t.FillFactor = 60;
            t.PartitionByRange("first_name").AddRange("m", "'m'", "'mz'");
        });
        var rangeLines = partitionLines(range);
        rangeLines.Length.ShouldBe(2); // one declared + default
        rangeLines.ShouldAllBe(x => x.Contains("WITH (fillfactor = 60);"));

        var hash = newTable(t =>
        {
            t.FillFactor = 60;
            t.PartitionByHash(new HashPartitioning { Columns = ["first_name"], Suffixes = ["one", "two"] });
        });
        var hashLines = partitionLines(hash);
        hashLines.Length.ShouldBe(2);
        hashLines.ShouldAllBe(x => x.Contains("WITH (fillfactor = 60);"));
    }

    [Fact]
    public async Task create_and_fetch_round_trip_reads_the_parameters_back()
    {
        var table = newTable(t =>
        {
            t.FillFactor = 70;
            t.StorageParameters["autovacuum_vacuum_scale_factor"] = "0.05";
            t.StorageParameters["autovacuum_enabled"] = "false";
        });
        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);

        existing!.StorageParameters["fillfactor"].ShouldBe("70");
        existing.StorageParameters["autovacuum_vacuum_scale_factor"].ShouldBe("0.05");
        existing.StorageParameters["autovacuum_enabled"].ShouldBe("false");
        existing.FillFactor.ShouldBe(70);
    }

    [Fact]
    public async Task fetching_a_table_without_parameters_gives_an_empty_collection()
    {
        var table = newTable();
        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);
        existing!.StorageParameters.Count.ShouldBe(0);
    }

    [Fact]
    public async Task matching_parameters_yield_no_delta()
    {
        var table = newTable(t =>
        {
            t.FillFactor = 70;
            t.StorageParameters["autovacuum_vacuum_scale_factor"] = "0.05";
        });
        await CreateSchemaObjectInDatabase(table);

        var delta = await deltaFor(table);

        delta.HasChanges().ShouldBeFalse();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task table_without_declared_parameters_has_no_delta_even_when_the_database_has_some()
    {
        await CreateSchemaObjectInDatabase(newTable(t => t.FillFactor = 70));

        var delta = await deltaFor(newTable());

        delta.HasChanges().ShouldBeFalse();
    }

    [Fact]
    public async Task numeric_values_compare_numerically_and_case_insensitively()
    {
        await CreateSchemaObjectInDatabase(newTable(t =>
        {
            t.StorageParameters["autovacuum_vacuum_scale_factor"] = "0.050";
            t.StorageParameters["autovacuum_enabled"] = "false";
        }));

        var delta = await deltaFor(newTable(t =>
        {
            t.StorageParameters["autovacuum_vacuum_scale_factor"] = "0.05";
            t.StorageParameters["autovacuum_enabled"] = "FALSE";
        }));

        delta.HasChanges().ShouldBeFalse();
    }

    [Fact]
    public async Task undeclared_actual_parameters_are_ignored()
    {
        await CreateSchemaObjectInDatabase(newTable(t =>
        {
            t.FillFactor = 70;
            t.StorageParameters["autovacuum_vacuum_threshold"] = 1000;
        }));

        var delta = await deltaFor(newTable(t => t.FillFactor = 70));

        delta.HasChanges().ShouldBeFalse();
    }

    [Fact]
    public async Task changed_parameter_is_an_update_with_an_alter_and_a_restoring_rollback()
    {
        await CreateSchemaObjectInDatabase(newTable(t => t.FillFactor = 70));

        var expected = newTable(t =>
        {
            t.FillFactor = 60;
            t.StorageParameters["autovacuum_vacuum_scale_factor"] = "0.01";
        });
        var delta = await deltaFor(expected);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        updateScript(delta).ShouldContain(
            "ALTER TABLE table_storage_parameters.people SET (fillfactor = 60, autovacuum_vacuum_scale_factor = 0.01);");

        var rollback = rollbackScript(delta);
        rollback.ShouldContain("ALTER TABLE table_storage_parameters.people SET (fillfactor = 70);");
        rollback.ShouldContain("ALTER TABLE table_storage_parameters.people RESET (autovacuum_vacuum_scale_factor);");

        await expected.ApplyChangesAsync(theConnection);

        var existing = await expected.FetchExistingAsync(theConnection);
        existing!.StorageParameters["fillfactor"].ShouldBe("60");
        existing.StorageParameters["autovacuum_vacuum_scale_factor"].ShouldBe("0.01");

        (await deltaFor(expected)).HasChanges().ShouldBeFalse();
    }

    [Fact]
    public async Task declared_parameter_on_a_table_that_has_none_is_an_update()
    {
        await CreateSchemaObjectInDatabase(newTable());

        var expected = newTable(t => t.FillFactor = 85);
        var delta = await deltaFor(expected);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        await expected.ApplyChangesAsync(theConnection);

        (await expected.FetchExistingAsync(theConnection))!.FillFactor.ShouldBe(85);
    }

    private static Table partitionedTable(int fillFactor)
        => newTable(t =>
        {
            t.FillFactor = fillFactor;
            t.AddColumn<string>("role")
                .PartitionByListValues()
                .AddPartition("admin", "admin")
                .AddPartition("super", "super");
        });

    [Fact]
    public async Task partitioned_table_parameters_land_on_the_partitions_and_round_trip_without_delta()
    {
        var table = partitionedTable(70);
        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);

        // PostgreSQL rejects parameters on the parent, so none are there
        existing!.StorageParameters.Count.ShouldBe(0);
        existing.PartitionStorageParameters.Count.ShouldBe(3);
        existing.PartitionStorageParameters.Values.ShouldAllBe(x => (string)x["fillfactor"]! == "70");

        (await deltaFor(table)).HasChanges().ShouldBeFalse();
    }

    [Fact]
    public async Task partitioned_table_change_alters_each_existing_partition()
    {
        await CreateSchemaObjectInDatabase(partitionedTable(70));

        var expected = partitionedTable(55);
        var delta = await deltaFor(expected);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        var script = updateScript(delta);
        script.ShouldContain("ALTER TABLE table_storage_parameters.people_admin SET (fillfactor = 55);");
        script.ShouldContain("ALTER TABLE table_storage_parameters.people_super SET (fillfactor = 55);");
        script.ShouldContain("ALTER TABLE table_storage_parameters.people_default SET (fillfactor = 55);");
        script.ShouldNotContain("ALTER TABLE table_storage_parameters.people SET");

        await expected.ApplyChangesAsync(theConnection);

        (await deltaFor(expected)).HasChanges().ShouldBeFalse();
        var existing = await expected.FetchExistingAsync(theConnection);
        existing!.PartitionStorageParameters.Values.ShouldAllBe(x => (string)x["fillfactor"]! == "55");
    }
}
