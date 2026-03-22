using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables.partitioning;

[Collection("integration")]
public class range_partitioning : IntegrationContext
{
    public range_partitioning() : base("partitioning")
    {
    }

    [Fact]
    public void generates_partition_function_and_scheme_ddl()
    {
        var table = new Table("partitioning.events");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<bool>("is_archived").NotNull();
        table.AddColumn<string>("data");

        var partitioning = table.PartitionByRange("is_archived", "bit");
        partitioning.AddBoundary(1);

        var writer = new StringWriter();
        table.WriteCreateStatement(new SqlServerMigrator(), writer);
        var ddl = writer.ToString();

        // Should have partition function
        ddl.ShouldContain("CREATE PARTITION FUNCTION [pf_events_is_archived] (bit)");
        ddl.ShouldContain("RANGE RIGHT");
        ddl.ShouldContain("FOR VALUES (1)");

        // Should have partition scheme
        ddl.ShouldContain("CREATE PARTITION SCHEME [ps_events_is_archived]");
        ddl.ShouldContain("AS PARTITION [pf_events_is_archived]");
        ddl.ShouldContain("ALL TO ([PRIMARY])");

        // Table should be ON the scheme
        ddl.ShouldContain("ON [ps_events_is_archived]([is_archived])");

        // Should NOT have PostgreSQL syntax
        ddl.ShouldNotContain("PARTITION BY RANGE");
    }

    [Fact]
    public void generates_date_range_partitioning()
    {
        var table = new Table("partitioning.orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<DateTime>("order_date").NotNull();
        table.AddColumn<decimal>("total");

        var partitioning = table.PartitionByRange("order_date", "datetime");
        partitioning.AddBoundary<DateTime>(new DateTime(2024, 1, 1));
        partitioning.AddBoundary<DateTime>(new DateTime(2025, 1, 1));
        partitioning.AddBoundary<DateTime>(new DateTime(2026, 1, 1));

        var writer = new StringWriter();
        table.WriteCreateStatement(new SqlServerMigrator(), writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("CREATE PARTITION FUNCTION [pf_orders_order_date] (datetime)");
        ddl.ShouldContain("FOR VALUES ('2024-01-01 00:00:00', '2025-01-01 00:00:00', '2026-01-01 00:00:00')");
    }

    [Fact]
    public async Task can_create_partitioned_table_in_database()
    {
        await ResetSchema();

        var table = new Table("partitioning.partitioned_events");
        table.AddColumn<int>("id");
        table.AddColumn<bool>("is_archived").NotNull();
        table.AddColumn<string>("data");

        // Partition column must be in the primary key for SQL Server
        table.ModifyColumn("id").AsPrimaryKey();
        table.ModifyColumn("is_archived").AsPrimaryKey();

        var partitioning = table.PartitionByRange("is_archived", "bit");
        partitioning.AddBoundary(1);

        await CreateSchemaObjectInDatabase(table);

        // Verify the partition function exists
        var pfExists = await theConnection.CreateCommand(
                "SELECT COUNT(*) FROM sys.partition_functions WHERE name = 'pf_partitioned_events_is_archived'")
            .ExecuteScalarAsync();
        ((int)pfExists!).ShouldBeGreaterThan(0);

        // Verify the partition scheme exists
        var psExists = await theConnection.CreateCommand(
                "SELECT COUNT(*) FROM sys.partition_schemes WHERE name = 'ps_partitioned_events_is_archived'")
            .ExecuteScalarAsync();
        ((int)psExists!).ShouldBeGreaterThan(0);

        // Verify the table uses the partition scheme
        var partitionCount = await theConnection.CreateCommand($"""
            SELECT COUNT(*)
            FROM sys.partitions p
            JOIN sys.objects o ON p.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = 'partitioning' AND o.name = 'partitioned_events'
            """)
            .ExecuteScalarAsync();
        // With 1 boundary value, we get 2 partitions
        ((int)partitionCount!).ShouldBe(2);
    }

    [Fact]
    public void delta_detects_no_change()
    {
        var partitioning = new RangePartitioning("is_archived", "bit");
        partitioning.AddBoundary(1);

        var actual = new SqlServerPartitionInfo
        {
            Column = "is_archived",
            SqlDataType = "bit",
            IsRangeRight = true,
            BoundaryValues = ["1"]
        };

        partitioning.CreateDelta(actual).ShouldBe(PartitionDelta.None);
    }

    [Fact]
    public void delta_detects_additive_change()
    {
        var partitioning = new RangePartitioning("order_date", "datetime");
        partitioning.AddBoundary("'2024-01-01'");
        partitioning.AddBoundary("'2025-01-01'");
        partitioning.AddBoundary("'2026-01-01'");

        var actual = new SqlServerPartitionInfo
        {
            Column = "order_date",
            SqlDataType = "datetime",
            IsRangeRight = true,
            BoundaryValues = ["'2024-01-01'", "'2025-01-01'"]
        };

        partitioning.CreateDelta(actual).ShouldBe(PartitionDelta.Additive);
    }

    [Fact]
    public void delta_detects_rebuild_on_column_change()
    {
        var partitioning = new RangePartitioning("order_date", "datetime");
        partitioning.AddBoundary("'2024-01-01'");

        var actual = new SqlServerPartitionInfo
        {
            Column = "created_at",
            SqlDataType = "datetime",
            IsRangeRight = true,
            BoundaryValues = ["'2024-01-01'"]
        };

        partitioning.CreateDelta(actual).ShouldBe(PartitionDelta.Rebuild);
    }

    [Fact]
    public void delta_detects_rebuild_when_boundaries_removed()
    {
        var partitioning = new RangePartitioning("order_date", "datetime");
        partitioning.AddBoundary("'2025-01-01'");

        var actual = new SqlServerPartitionInfo
        {
            Column = "order_date",
            SqlDataType = "datetime",
            IsRangeRight = true,
            BoundaryValues = ["'2024-01-01'", "'2025-01-01'"]
        };

        partitioning.CreateDelta(actual).ShouldBe(PartitionDelta.Rebuild);
    }
}
