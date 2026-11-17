using JasperFx;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables.partitioning;

/// <summary>
///     JasperFx/polecat#1041: turning RANGE partitioning on for a table that already holds rows. The
///     delta used to call that Invalid and stop there, so <c>AutoCreate.CreateOrUpdate</c> refused the
///     migration and <c>AutoCreate.All</c> dropped the table and every row in it. The table is now
///     moved onto the scheme in place by rebuilding its clustered primary key there.
/// </summary>
[Collection("integration")]
public class moving_an_existing_table_onto_a_partition_scheme: IntegrationContext
{
    private const string Schema = "part_move";
    private const int RowCount = 2000;

    // Partition functions and schemes are database-wide and named after the table, so the tables here
    // have names no other test uses
    private const string Events = "pmove_events";
    private const string Tags = "pmove_tags";

    public moving_an_existing_table_onto_a_partition_scheme(): base(Schema)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await ResetSchema();
        await theConnection.CreateCommand($"""
            if exists (select 1 from sys.partition_schemes where name = 'ps_{Events}_is_archived') drop partition scheme ps_{Events}_is_archived;
            if exists (select 1 from sys.partition_functions where name = 'pf_{Events}_is_archived') drop partition function pf_{Events}_is_archived;
            """).ExecuteNonQueryAsync();
    }

    private static Table EventsTable(bool partitioned)
    {
        var table = new Table($"{Schema}.{Events}");
        table.AddColumn("seq_id", "bigint").AsPrimaryKey().AutoNumber();
        table.AddColumn("stream_id", "uniqueidentifier").NotNull();
        table.AddColumn("version", "bigint").NotNull();
        table.AddColumn("data", "varchar(100)").NotNull();
        var archived = table.AddColumn("is_archived", "bit").NotNull().DefaultValue(0);

        if (partitioned)
        {
            archived.AsPrimaryKey();
            table.PartitionByRange("is_archived", "bit").AddBoundary(1);
        }

        table.Indexes.Add(new IndexDefinition($"ix_{Events}_stream_and_version")
        {
            IsUnique = true,
            Columns = partitioned ? ["stream_id", "version", "is_archived"] : ["stream_id", "version"]
        });

        return table;
    }

    /// <summary>
    ///     A table whose foreign key references the events table's primary key. Partitioning changes
    ///     that key, so the reference cannot survive in this shape: the partitioned model drops it.
    /// </summary>
    private static Table TagsTable(bool partitioned)
    {
        var table = new Table($"{Schema}.{Tags}");
        table.AddColumn("value", "varchar(50)").AsPrimaryKey();
        table.AddColumn("seq_id", "bigint").AsPrimaryKey();

        if (!partitioned)
        {
            table.ForeignKeys.Add(new ForeignKey($"fk_{Tags}_seq_id")
            {
                ColumnNames = ["seq_id"],
                LinkedNames = ["seq_id"],
                LinkedTable = new SqlServerObjectName(Schema, Events)
            });
        }

        return table;
    }

    private async Task createPopulatedTablesAsync()
    {
        await new SqlServerMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, EventsTable(false), TagsTable(false)),
            AutoCreate.CreateOrUpdate);

        await theConnection.CreateCommand($"""
            with numbers as (
                select top ({RowCount}) row_number() over (order by (select null)) as n
                from sys.all_objects a cross join sys.all_objects b
            )
            insert into {Schema}.{Events} (stream_id, version, data, is_archived)
            select newid(), 1, concat('row ', n), case when n % 4 = 0 then 1 else 0 end from numbers;

            insert into {Schema}.{Tags} (value, seq_id)
            select concat('tag ', seq_id), seq_id from {Schema}.{Events} where seq_id % 10 = 0;
            """).ExecuteNonQueryAsync();
    }

    private async Task<List<(long SeqId, string Data, bool Archived)>> readRowsAsync()
    {
        var rows = new List<(long, string, bool)>();
        await using var reader = await theConnection
            .CreateCommand($"select seq_id, data, is_archived from {Schema}.{Events} order by seq_id")
            .ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetInt64(0), reader.GetString(1), reader.GetBoolean(2)));
        }

        return rows;
    }

    private async Task<long> rowsInPartitionAsync(int partitionNumber)
    {
        var rows = await theConnection.CreateCommand($"""
            select coalesce(sum(p.rows), 0) from sys.partitions p
            where p.object_id = object_id('{Schema}.{Events}') and p.index_id <= 1 and p.partition_number = {partitionNumber}
            """).ExecuteScalarAsync();
        return Convert.ToInt64(rows);
    }

    private async Task assertMovedAsync(List<(long SeqId, string Data, bool Archived)> before)
    {
        (await readRowsAsync()).ShouldBe(before);

        var archived = before.Count(x => x.Archived);
        (await rowsInPartitionAsync(1)).ShouldBe(before.Count - archived);
        (await rowsInPartitionAsync(2)).ShouldBe(archived);

        // The identity carries on where it was
        var next = (long)(await theConnection
            .CreateCommand($"insert into {Schema}.{Events} (stream_id, version, data) output inserted.seq_id values (newid(), 1, 'after')")
            .ExecuteScalarAsync())!;
        next.ShouldBeGreaterThan(before.Max(x => x.SeqId));

        // ...and the database matches the model, so the next run does nothing
        var again = await SchemaMigration.DetermineAsync(theConnection, EventsTable(true), TagsTable(true));
        again.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    private async Task<SchemaMigration> partitionedMigrationAsync()
        => await SchemaMigration.DetermineAsync(theConnection, EventsTable(true), TagsTable(true));

    [Fact]
    public async Task the_delta_is_a_rebuild_and_not_a_drop()
    {
        await createPopulatedTablesAsync();

        var delta = (TableDelta)(await partitionedMigrationAsync()).Deltas[0];

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.MovesOntoPartitionScheme.ShouldBeTrue();
        delta.CanRebuildInPlace.ShouldBeTrue();
        DestructiveChange.DropsAndRecreates(delta).ShouldBeFalse();
    }

    [Fact]
    public async Task create_or_update_moves_every_row_onto_the_scheme()
    {
        await createPopulatedTablesAsync();
        var before = await readRowsAsync();

        await new SqlServerMigrator().ApplyAllAsync(theConnection, await partitionedMigrationAsync(),
            AutoCreate.CreateOrUpdate);

        await assertMovedAsync(before);
    }

    [Fact]
    public async Task auto_create_all_moves_the_rows_rather_than_dropping_the_table()
    {
        await createPopulatedTablesAsync();
        var before = await readRowsAsync();

        await new SqlServerMigrator().ApplyAllAsync(theConnection, await partitionedMigrationAsync(), AutoCreate.All);

        await assertMovedAsync(before);
    }

    [Fact]
    public async Task create_only_still_refuses_it()
    {
        await createPopulatedTablesAsync();

        var migration = await partitionedMigrationAsync();

        Should.Throw<SchemaMigrationException>(() => migration.AssertPatchingIsValid(AutoCreate.CreateOnly));
    }

    [Fact]
    public async Task the_script_never_drops_the_table_and_can_run_twice()
    {
        await createPopulatedTablesAsync();
        var before = await readRowsAsync();

        var script = scriptFor(await partitionedMigrationAsync());
        script.ShouldNotContain("drop table", Case.Insensitive);

        await runScriptAsync(script);
        await runScriptAsync(script);

        await assertMovedAsync(before);
    }

    [Fact]
    public async Task a_move_cut_off_after_the_key_came_off_finishes_on_the_next_run()
    {
        await createPopulatedTablesAsync();
        var before = await readRowsAsync();

        var lines = scriptFor(await partitionedMigrationAsync()).Split('\n');
        var keyDropped = Array.FindIndex(lines, x =>
            x.Contains($"drop constraint if exists pkey_{Events}_seq_id;", StringComparison.OrdinalIgnoreCase));
        keyDropped.ShouldBeGreaterThan(0);

        // Through the end of that IF ... BEGIN ... END block, which is where a batch can end
        var blockEnd = Array.FindIndex(lines, keyDropped, x => x.Trim() == "END");
        await runScriptAsync(string.Join('\n', lines.Take(blockEnd + 1)));

        // The table is a heap now, with nothing keying it
        var existing = await EventsTable(true).FetchExistingAsync(theConnection);
        existing!.PrimaryKeyColumns.ShouldBeEmpty();

        await new SqlServerMigrator().ApplyAllAsync(theConnection, await partitionedMigrationAsync(),
            AutoCreate.CreateOrUpdate);

        await assertMovedAsync(before);
    }

    [Fact]
    public async Task an_unchanged_foreign_key_from_another_table_is_put_back()
    {
        await createPopulatedTablesAsync();

        // A table whose foreign key the partitioned model still declares, against a unique key that the
        // move does not change
        await theConnection.CreateCommand($"""
            create unique index ix_{Events}_seq_and_archived on {Schema}.{Events} (seq_id, is_archived);
            create table {Schema}.pmove_audit (id int not null primary key, seq_id bigint not null, is_archived bit not null,
                constraint fk_pmove_audit_events foreign key (seq_id, is_archived) references {Schema}.{Events} (seq_id, is_archived));
            """).ExecuteNonQueryAsync();

        var events = EventsTable(true);
        events.Indexes.Add(new IndexDefinition($"ix_{Events}_seq_and_archived")
        {
            IsUnique = true, Columns = ["seq_id", "is_archived"]
        });

        var audit = new Table($"{Schema}.pmove_audit");
        audit.AddColumn<int>("id").AsPrimaryKey();
        audit.AddColumn("seq_id", "bigint").NotNull();
        audit.AddColumn("is_archived", "bit").NotNull();
        audit.ForeignKeys.Add(new ForeignKey("fk_pmove_audit_events")
        {
            ColumnNames = ["seq_id", "is_archived"],
            LinkedNames = ["seq_id", "is_archived"],
            LinkedTable = new SqlServerObjectName(Schema, Events)
        });

        var migration = await SchemaMigration.DetermineAsync(theConnection, events, TagsTable(true), audit);
        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        (await rowsInPartitionAsync(2)).ShouldBe(RowCount / 4);
        var foreignKeys = await theConnection
            .CreateCommand("select count(*) from sys.foreign_keys where name = 'fk_pmove_audit_events'")
            .ExecuteScalarAsync();
        foreignKeys.ShouldBe(1);

        (await SchemaMigration.DetermineAsync(theConnection, events, TagsTable(true), audit))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task rolling_the_move_back_keeps_the_rows()
    {
        await createPopulatedTablesAsync();
        var before = await readRowsAsync();

        var migration = await partitionedMigrationAsync();
        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        var events = (TableDelta)migration.Deltas[0];
        var rollback = new StringWriter();
        events.WriteRollback(new SqlServerMigrator(), rollback);
        await runScriptAsync(rollback.ToString());

        (await readRowsAsync()).ShouldBe(before);
        var partitioned = await theConnection.CreateCommand($"""
            select count(*) from sys.indexes i inner join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
            where i.object_id = object_id('{Schema}.{Events}')
            """).ExecuteScalarAsync();
        partitioned.ShouldBe(0);

        var existing = await EventsTable(false).FetchExistingAsync(theConnection);
        existing!.PrimaryKeyColumns.ShouldBe(["seq_id"]);
    }

    [Fact]
    public async Task turning_partitioning_off_moves_the_rows_back_off_the_scheme()
    {
        await createPopulatedTablesAsync();
        var before = await readRowsAsync();
        await new SqlServerMigrator().ApplyAllAsync(theConnection, await partitionedMigrationAsync(),
            AutoCreate.CreateOrUpdate);

        // Back to the unpartitioned model. The key loses is_archived, which a key on the scheme must
        // carry, so the table has to come off it. The tag table's foreign key is not put back: that is
        // a separate model change this test does not make.
        var migration = await SchemaMigration.DetermineAsync(theConnection, EventsTable(false));
        var delta = (TableDelta)migration.Deltas[0];
        delta.MovesOffPartitionScheme.ShouldBeTrue();
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        (await readRowsAsync()).ShouldBe(before);

        (await SchemaMigration.DetermineAsync(theConnection, EventsTable(false)))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    private static string scriptFor(SchemaMigration migration)
    {
        var writer = new StringWriter();
        migration.WriteAllUpdates(writer, new SqlServerMigrator(), AutoCreate.CreateOrUpdate);
        return writer.ToString();
    }

    private async Task runScriptAsync(string script)
    {
        foreach (var batch in SqlServerBatchSplitter.Split(script))
        {
            if (string.IsNullOrWhiteSpace(batch)) continue;
            await using var cmd = new SqlCommand(batch, theConnection);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
