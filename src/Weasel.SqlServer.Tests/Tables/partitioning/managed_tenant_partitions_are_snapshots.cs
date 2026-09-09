using System.Collections.Concurrent;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables.partitioning;

/// <summary>
/// weasel#583, the SQL Server half. <see cref="ManagedTenantPartitions"/> has the same shape as the
/// PostgreSQL <c>ManagedListPartitions</c>: the tenant registry is published through
/// <c>ReadOnlyDictionary</c> wrappers that do not copy, and it is read straight off the schema-object path
/// that emits partition-function DDL and computes its delta. Clearing and refilling those maps in place
/// while another database's worker is generating DDL either throws "Collection was modified" or, with
/// nothing raised at all, writes a partition function missing boundaries for real tenants.
/// </summary>
[Collection("integration")]
public class managed_tenant_partitions_are_snapshots: IntegrationContext
{
    private const int TenantCount = 400;

    public managed_tenant_partitions_are_snapshots() : base("mtp_snapshot")
    {
    }

    [Fact]
    public async Task readers_never_see_the_registry_being_reloaded()
    {
        await ResetSchema();

        var database = new DatabaseWithTables("mtp_snapshot_integration", ConnectionSource.ConnectionString);
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp_snapshot", "tenant_registry")) { AllowOrdinalSharing = true };

        var values = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 1; i <= TenantCount; i++)
        {
            values["tenant" + i] = i;
        }

        await manager.ResetValues(database, values, CancellationToken.None);

        var orders = new Table("mtp_snapshot.orders");
        orders.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.PartitionByManagedTenants(manager);

        var migrator = new SqlServerMigrator();
        var failures = new ConcurrentQueue<Exception>();
        var tornReads = 0;

        var writer = Task.Run(async () =>
        {
            for (var i = 0; i < 25; i++)
            {
                manager.ForceReload();
                await manager.InitializeAsync(database, CancellationToken.None);
            }
        });

        var readers = Enumerable.Range(0, 3).Select(_ => Task.Run(() =>
        {
            while (!writer.IsCompleted)
            {
                try
                {
                    // The DDL read side — OrderedBoundaries() enumerates the registry from in here.
                    var text = new StringWriter();
                    orders.WriteCreateStatement(migrator, text);

                    if (BoundaryCount(text.ToString()) != TenantCount + 1)
                    {
                        // +1 for the sentinel boundary 0 that is always present.
                        Interlocked.Increment(ref tornReads);
                    }

                    // ...and the published map, which callers enumerate directly.
                    var counted = 0;
                    foreach (var _ in manager.Ordinals)
                    {
                        counted++;
                    }

                    if (counted != TenantCount)
                    {
                        Interlocked.Increment(ref tornReads);
                    }
                }
                catch (Exception e)
                {
                    failures.Enqueue(e);
                }
            }
        })).ToArray();

        await writer;
        await Task.WhenAll(readers);

        if (failures.TryDequeue(out var failure))
        {
            throw new Exception(
                "A reader threw while the tenant registry was being reloaded underneath it", failure);
        }

        tornReads.ShouldBe(0, "A reader observed an incomplete tenant registry");
    }

    private static int BoundaryCount(string ddl)
    {
        const string marker = "AS RANGE RIGHT FOR VALUES (";
        var start = ddl.IndexOf(marker, StringComparison.Ordinal);
        if (start < 0)
        {
            return 0;
        }

        start += marker.Length;
        var end = ddl.IndexOf(')', start);
        return ddl.Substring(start, end - start).Split(',').Length;
    }
}
