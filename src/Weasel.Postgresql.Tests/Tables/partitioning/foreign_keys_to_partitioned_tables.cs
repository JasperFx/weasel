using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

/// <summary>
/// A foreign key that REFERENCES a partitioned table gets cloned by PostgreSQL: one extra
/// pg_constraint row per partition of the referenced table, carrying a PostgreSQL-chosen name and a
/// non-zero conparentid. Those rows are not part of anyone's configuration and they cannot be
/// dropped on their own -- "42P16: cannot drop inherited constraint". Reading them back as real
/// foreign keys made every subsequent migration emit a DROP for them and fail.
///
/// Reported downstream as https://github.com/JasperFx/marten/issues/5044.
/// </summary>
[Collection("cloned_fks")]
public class foreign_keys_to_partitioned_tables: IntegrationContext
{
    public foreign_keys_to_partitioned_tables(): base("cloned_fks")
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await ResetSchema();
    }

    private Table buildStreams()
    {
        var streams = new Table(new PostgresqlObjectName(SchemaName, "streams"));
        streams.AddColumn<Guid>("id").AsPrimaryKey();
        streams.AddColumn<string>("tenant_id").AsPrimaryKey().NotNull()
            .PartitionByListValues()
            .AddPartition("one", "one");

        return streams;
    }

    private Table buildLookup(Table streams)
    {
        var lookup = new Table(new PostgresqlObjectName(SchemaName, "lookup"));
        lookup.AddColumn<string>("natural_key").AsPrimaryKey().NotNull();
        lookup.AddColumn<Guid>("stream_id").NotNull();
        lookup.AddColumn<string>("tenant_id").AsPrimaryKey().NotNull();

        lookup.ForeignKeys.Add(new ForeignKey("fk_lookup_streams")
        {
            ColumnNames = ["stream_id", "tenant_id"],
            LinkedNames = ["id", "tenant_id"],
            LinkedTable = streams.Identifier,
            OnDelete = CascadeAction.Cascade
        });

        return lookup;
    }

    [Fact]
    public async Task cloned_foreign_key_rows_are_not_read_back_as_configuration()
    {
        if (theConnection.State != System.Data.ConnectionState.Open) await theConnection.OpenAsync();
        await theConnection.EnsureSchemaExists(SchemaName);

        var streams = buildStreams();
        var lookup = buildLookup(streams);

        var migrator = new PostgresqlMigrator();
        var initial = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, streams, lookup);
        await migrator.ApplyAllAsync(theConnection, initial, AutoCreate.CreateOrUpdate);

        // PostgreSQL has now cloned fk_lookup_streams once per partition of streams.
        var existing = await lookup.FetchExistingAsync(theConnection);
        existing.ShouldNotBeNull();
        existing.ForeignKeys.Select(x => x.Name).ShouldBe(["fk_lookup_streams"]);

        // ...so the schema reads as up to date rather than as drift...
        var second = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, streams, lookup);
        second.Difference.ShouldBe(SchemaPatchDifference.None);

        // ...and re-applying does not try to drop an inherited constraint.
        await Should.NotThrowAsync(() =>
            migrator.ApplyAllAsync(theConnection, second, AutoCreate.CreateOrUpdate));
    }

    [Fact]
    public async Task adding_a_partition_to_the_referenced_table_does_not_become_drift()
    {
        if (theConnection.State != System.Data.ConnectionState.Open) await theConnection.OpenAsync();
        await theConnection.EnsureSchemaExists(SchemaName);

        var streams = buildStreams();
        var lookup = buildLookup(streams);

        var migrator = new PostgresqlMigrator();
        var initial = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, streams, lookup);
        await migrator.ApplyAllAsync(theConnection, initial, AutoCreate.CreateOrUpdate);

        // A second partition clones another set of foreign key rows onto lookup.
        var streamsV2 = new Table(new PostgresqlObjectName(SchemaName, "streams"));
        streamsV2.AddColumn<Guid>("id").AsPrimaryKey();
        streamsV2.AddColumn<string>("tenant_id").AsPrimaryKey().NotNull()
            .PartitionByListValues()
            .AddPartition("one", "one")
            .AddPartition("two", "two");

        var lookupV2 = buildLookup(streamsV2);

        var next = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, streamsV2, lookupV2);
        await migrator.ApplyAllAsync(theConnection, next, AutoCreate.CreateOrUpdate);

        var existing = await lookupV2.FetchExistingAsync(theConnection);
        existing.ShouldNotBeNull();
        existing.ForeignKeys.Select(x => x.Name).ShouldBe(["fk_lookup_streams"]);

        var third = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, streamsV2, lookupV2);
        third.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
