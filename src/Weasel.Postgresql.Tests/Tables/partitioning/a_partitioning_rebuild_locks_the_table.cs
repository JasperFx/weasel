using JasperFx;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

/// <summary>
///     weasel#882: a change of partitioning copies the table aside, drops it, creates it again and copies
///     the rows back. Since weasel#871 nothing in that script took a lock on the table before the copy
///     aside, so a row another connection had inserted but not committed was missing from the copy, and
///     the drop that waited for it then dropped the row with the table.
/// </summary>
[Collection("partition_rebuild_lock")]
public class a_partitioning_rebuild_locks_the_table: IntegrationContext
{
    private const string Schema = "partition_rebuild_lock";

    public a_partitioning_rebuild_locks_the_table(): base(Schema)
    {
    }

    private static Table unpartitioned()
    {
        var table = new Table($"{Schema}.events");
        table.AddColumn<long>("seq_id").AsPrimaryKey();
        table.AddColumn<bool>("is_archived").DefaultValueByExpression("false");
        table.AddColumn<string>("data");
        return table;
    }

    private static Table partitioned()
    {
        var table = new Table($"{Schema}.events");
        table.AddColumn<long>("seq_id").AsPrimaryKey();
        table.AddColumn<bool>("is_archived").AsPrimaryKey().DefaultValueByExpression("false")
            .PartitionByListValues()
            .AddPartition("archived", true);
        table.AddColumn<string>("data");

        // Declared concurrent. The rebuild builds it in place: the table is empty at that point, and
        // CREATE INDEX CONCURRENTLY cannot run inside the rebuild's transaction.
        table.Indexes.Add(new IndexDefinition("idx_events_data") { Columns = ["data"], IsConcurrent = true });
        return table;
    }

    private async Task insertAsync(NpgsqlConnection conn, long id)
    {
        await using var cmd = conn.CreateCommand(
            $"insert into {Schema}.events (seq_id, is_archived, data) values (:id, false, 'row')");
        cmd.Parameters.AddWithValue("id", id);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<bool> rowExistsAsync(long id)
    {
        await using var cmd = theConnection.CreateCommand($"select count(*) from {Schema}.events where seq_id = :id");
        cmd.Parameters.AddWithValue("id", id);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync()) == 1;
    }

    private async Task<bool> isPartitionedAsync()
    {
        await using var cmd = theConnection.CreateCommand(
            "select count(*) from pg_partitioned_table p join pg_class c on c.oid = p.partrelid " +
            "join pg_namespace n on n.oid = c.relnamespace where n.nspname = :schema and c.relname = 'events'");
        cmd.Parameters.AddWithValue("schema", Schema);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync()) == 1;
    }

    private async Task<bool> tableExistsAsync(string name)
    {
        await using var cmd = theConnection.CreateCommand(
            "select count(*) from information_schema.tables where table_schema = :schema and table_name = :name");
        cmd.Parameters.AddWithValue("schema", Schema);
        cmd.Parameters.AddWithValue("name", name);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync()) == 1;
    }

    private async Task<bool> someoneIsWaitingForTheTableAsync()
    {
        await using var conn = await theDataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand(
            "select count(*) from pg_locks l join pg_class c on c.oid = l.relation " +
            "join pg_namespace n on n.oid = c.relnamespace " +
            "where n.nspname = :schema and c.relname = 'events' and not l.granted");
        cmd.Parameters.AddWithValue("schema", Schema);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync()) > 0;
    }

    private async Task<SchemaMigration> partitioningMigrationAsync(params ISchemaObjectDelta[] others)
    {
        var rebuild = await partitioned().FindDeltaAsync(theConnection);
        rebuild.PartitionDelta.ShouldBe(PartitionDelta.Rebuild);

        return new SchemaMigration(new ISchemaObjectDelta[] { rebuild }.Concat(others).ToArray());
    }

    private async Task startWithOneRowAsync()
    {
        await ResetSchema();
        await CreateSchemaObjectInDatabase(unpartitioned());
        await insertAsync(theConnection, 1);
    }

    [Fact]
    public async Task a_row_another_connection_commits_during_the_rebuild_is_not_lost()
    {
        await startWithOneRowAsync();

        // Another connection is part way through writing a row
        await using var other = await theDataSource.OpenConnectionAsync();
        await using var tx = await other.BeginTransactionAsync();
        await insertAsync(other, 2);

        // The path every apply takes: SchemaMigration.DetermineAsync, then the migrator
        var migration = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, partitioned());
        var applying = new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        // Without the lock the copy aside does not wait: it reads a snapshot without row 2, and the drop
        // after it is what waits for the other transaction, only to drop the table holding the row
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        while (!await someoneIsWaitingForTheTableAsync())
        {
            if (applying.IsCompleted || DateTimeOffset.UtcNow > deadline)
            {
                await applying;
                throw new TimeoutException("The migration never waited for the other connection's transaction");
            }

            await Task.Delay(25);
        }

        await tx.CommitAsync();
        await applying;

        (await isPartitionedAsync()).ShouldBeTrue();
        (await rowExistsAsync(1)).ShouldBeTrue();
        (await rowExistsAsync(2)).ShouldBeTrue();
        (await tableExistsAsync("events_temp")).ShouldBeFalse();
    }

    [Fact]
    public async Task the_rebuild_applies_beside_other_deltas_in_one_migration()
    {
        await startWithOneRowAsync();

        var others = new Table($"{Schema}.others");
        others.AddColumn<int>("id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(others);

        var othersWithAColumn = new Table($"{Schema}.others");
        othersWithAColumn.AddColumn<int>("id").AsPrimaryKey();
        othersWithAColumn.AddColumn<string>("name");

        var created = new Table($"{Schema}.created");
        created.AddColumn<int>("id").AsPrimaryKey();

        var before = new SchemaObjectDelta(created, SchemaPatchDifference.Create);
        var after = await othersWithAColumn.FindDeltaAsync(theConnection);

        var rebuild = await partitioned().FindDeltaAsync(theConnection);
        var migration = new SchemaMigration(new ISchemaObjectDelta[] { before, rebuild, after });

        await new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        (await isPartitionedAsync()).ShouldBeTrue();
        (await rowExistsAsync(1)).ShouldBeTrue();
        (await tableExistsAsync("created")).ShouldBeTrue();
        (await othersWithAColumn.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await partitioned().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_failure_later_in_the_command_rolls_the_rebuild_back_too()
    {
        await startWithOneRowAsync();

        // Fails on create, after the rebuild, in the same command
        var broken = new Table($"{Schema}.broken");
        broken.AddColumn("id", "not_a_type").AsPrimaryKey();

        var migration = await partitioningMigrationAsync(new SchemaObjectDelta(broken, SchemaPatchDifference.Create));

        await Should.ThrowAsync<Exception>(() =>
            new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate));

        // One transaction for the whole command, as before the rebuild had a transaction of its own
        (await isPartitionedAsync()).ShouldBeFalse();
        (await rowExistsAsync(1)).ShouldBeTrue();
        (await tableExistsAsync("events_temp")).ShouldBeFalse();

        // And the failed transaction was closed, so the connection is still usable
        (await theConnection.CreateCommand("select 1").ExecuteScalarAsync()).ShouldBe(1);
    }

    [Fact]
    public async Task the_rebuild_written_on_its_own_opens_its_own_transaction_and_runs()
    {
        await startWithOneRowAsync();

        var delta = await partitioned().FindDeltaAsync(theConnection);
        var writer = new StringWriter();
        delta.WriteUpdate(new PostgresqlMigrator(), writer);
        var ddl = writer.ToString();

        var begin = ddl.IndexOf("begin;", StringComparison.Ordinal);
        var lockAt = ddl.IndexOf($"lock table {Schema}.events in access exclusive mode;", StringComparison.Ordinal);
        var copyAside = ddl.IndexOf($"create table {Schema}.events_temp as select", StringComparison.Ordinal);
        var copyBack = ddl.IndexOf($"from {Schema}.events_temp;", StringComparison.Ordinal);
        var commit = ddl.LastIndexOf("commit;", StringComparison.Ordinal);

        begin.ShouldBe(0);
        lockAt.ShouldBeGreaterThan(begin);
        copyAside.ShouldBeGreaterThan(lockAt);
        copyBack.ShouldBeGreaterThan(copyAside);
        commit.ShouldBeGreaterThan(copyBack);
        ddl.ShouldNotContain(IndexDefinition.IndexCreationBeginComment);
        ddl.ShouldNotContain("concurrently", Case.Insensitive);

        // What a script exported with WriteUpdate is: valid on its own
        await theConnection.CreateCommand(ddl).ExecuteNonQueryAsync();

        (await isPartitionedAsync()).ShouldBeTrue();
        (await rowExistsAsync(1)).ShouldBeTrue();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_migration_script_with_the_rebuild_runs(bool transactional)
    {
        await startWithOneRowAsync();

        var migration = await partitioningMigrationAsync();
        var migrator = new PostgresqlMigrator { IsTransactional = transactional };

        // The script WriteMigrationFileAsync writes: in a DO block when transactional, which is one
        // transaction already and has no BEGIN/COMMIT of its own to offer
        var writer = new StringWriter();
        migrator.WriteScript(writer, (m, w) => migration.WriteAllUpdates(w, m, AutoCreate.All));
        var script = writer.ToString();

        script.ShouldContain($"lock table {Schema}.events in access exclusive mode;");
        if (transactional)
        {
            script.ShouldNotContain("begin;");
            script.ShouldNotContain("commit;");
        }

        await theConnection.CreateCommand(script).ExecuteNonQueryAsync();

        (await isPartitionedAsync()).ShouldBeTrue();
        (await rowExistsAsync(1)).ShouldBeTrue();
        (await tableExistsAsync("events_temp")).ShouldBeFalse();
    }

    [Fact]
    public void every_command_of_a_migration_with_a_rebuild_is_its_own_explicit_transaction_but_a_concurrent_index_build()
    {
        var created = new Table($"{Schema}.created");
        created.AddColumn<int>("id").AsPrimaryKey();

        // An existing table gains a concurrent index: a command of its own, outside any transaction
        var withIndex = new Table($"{Schema}.others");
        withIndex.AddColumn<int>("id").AsPrimaryKey();
        withIndex.AddColumn<string>("name");
        withIndex.Indexes.Add(new IndexDefinition("idx_others_name") { Columns = ["name"], IsConcurrent = true });
        var withoutIndex = new Table($"{Schema}.others");
        withoutIndex.AddColumn<int>("id").AsPrimaryKey();
        withoutIndex.AddColumn<string>("name");

        var migration = new SchemaMigration(new ISchemaObjectDelta[]
        {
            new SchemaObjectDelta(created, SchemaPatchDifference.Create),
            new TableDelta(withIndex, withoutIndex),
            new TableDelta(partitioned(), unpartitioned())
        });

        var commands = new PostgresqlMigrator().BuildMigrationCommands(migration, AutoCreate.CreateOrUpdate);

        var concurrent = commands.Single(x => x.Sql.Contains("CONCURRENTLY", StringComparison.OrdinalIgnoreCase));
        concurrent.ExplicitTransaction.ShouldBeFalse();
        concurrent.Sql.ShouldNotContain("begin;");

        var rebuild = commands.Single(x => x.Sql.Contains("lock table"));
        rebuild.ExplicitTransaction.ShouldBeTrue();
        rebuild.Sql.ShouldStartWith("begin;");
        rebuild.Sql.TrimEnd().ShouldEndWith("commit;");

        // The rebuild writes no transaction of its own into the command's, and the lock, the copy aside and
        // the copy back are all in it
        rebuild.Sql.Split("begin;").Length.ShouldBe(2);
        rebuild.Sql.Split("commit;").Length.ShouldBe(2);
        rebuild.Sql.ShouldContain($"create table {Schema}.events_temp as select");
        rebuild.Sql.ShouldContain($"from {Schema}.events_temp;");

        foreach (var command in commands.Where(x => x.Sql.Trim().Length > 0 && x != concurrent))
        {
            command.ExplicitTransaction.ShouldBeTrue();
            command.Sql.ShouldStartWith("begin;");
        }
    }

    /// <summary>
    ///     What Wolverine's inbox delta did to work around weasel#882 (JasperFx/wolverine#5637): its own
    ///     transaction and lock around the base rebuild
    /// </summary>
    private class SelfLockingTableDelta: TableDelta
    {
        public SelfLockingTableDelta(Table expected, Table? actual): base(expected, actual)
        {
        }

        public override void WriteUpdate(Migrator rules, TextWriter writer)
        {
            writer.WriteLine("begin;");
            writer.WriteLine($"lock table {Expected.Identifier} in access exclusive mode;");
            base.WriteUpdate(rules, writer);
            writer.WriteLine("commit;");
        }
    }

    [Fact]
    public async Task a_delta_that_already_wraps_the_rebuild_in_its_own_transaction_still_applies()
    {
        await startWithOneRowAsync();

        var actual = await unpartitioned().FetchExistingAsync(theConnection);
        var delta = new SelfLockingTableDelta(partitioned(), actual);
        delta.PartitionDelta.ShouldBe(PartitionDelta.Rebuild);

        await new PostgresqlMigrator().ApplyAllAsync(theConnection, new SchemaMigration(delta), AutoCreate.CreateOrUpdate);

        (await isPartitionedAsync()).ShouldBeTrue();
        (await rowExistsAsync(1)).ShouldBeTrue();
        (await theConnection.CreateCommand("select 1").ExecuteScalarAsync()).ShouldBe(1);
    }

    /// <summary>
    ///     weasel#888: a custom delta that needs a transaction block of its own, the way Wolverine's inbox
    ///     delta does, asks <see cref="PostgresqlMigrator.RunsInOneTransaction" /> before writing its own
    ///     begin and commit. It records each answer it got.
    /// </summary>
    private class TransactionAwareTableDelta: TableDelta
    {
        public TransactionAwareTableDelta(Table expected, Table? actual): base(expected, actual)
        {
        }

        public List<bool> Answers { get; } = new();

        public override void WriteUpdate(Migrator rules, TextWriter writer)
        {
            var inOneTransaction = PostgresqlMigrator.RunsInOneTransaction(writer);
            Answers.Add(inOneTransaction);

            if (!inOneTransaction)
            {
                writer.WriteLine("begin;");
            }

            writer.WriteLine($"lock table {Expected.Identifier} in access exclusive mode;");
            base.WriteUpdate(rules, writer);

            if (!inOneTransaction)
            {
                writer.WriteLine("commit;");
            }
        }
    }

    private static Table others(bool withName)
    {
        var table = new Table($"{Schema}.others");
        table.AddColumn<int>("id").AsPrimaryKey();
        if (withName)
        {
            table.AddColumn<string>("name");
        }

        return table;
    }

    [Fact]
    public void runs_in_one_transaction_is_true_for_every_command_of_a_migration_with_a_rebuild()
    {
        var custom = new TransactionAwareTableDelta(others(true), others(false));
        var migration = new SchemaMigration(new ISchemaObjectDelta[]
        {
            new TableDelta(partitioned(), unpartitioned()),
            custom
        });

        var commands = new PostgresqlMigrator().BuildMigrationCommands(migration, AutoCreate.CreateOrUpdate);

        custom.Answers.ShouldBe([true]);

        // The migrator's begin and commit are the only ones in the command
        var command = commands.Single(x => x.Sql.Contains($"lock table {Schema}.others"));
        command.ExplicitTransaction.ShouldBeTrue();
        command.Sql.Split("begin;").Length.ShouldBe(2);
        command.Sql.Split("commit;").Length.ShouldBe(2);
    }

    [Fact]
    public void runs_in_one_transaction_is_false_without_a_rebuild_in_the_migration()
    {
        // Public, so a delta outside Weasel can ask
        typeof(PostgresqlMigrator).GetMethod(nameof(PostgresqlMigrator.RunsInOneTransaction)).ShouldNotBeNull();

        var custom = new TransactionAwareTableDelta(others(true), others(false));

        var commands = new PostgresqlMigrator().BuildMigrationCommands(new SchemaMigration(custom),
            AutoCreate.CreateOrUpdate);

        custom.Answers.ShouldBe([false]);

        var command = commands.Single(x => x.Sql.Contains($"lock table {Schema}.others"));
        command.ExplicitTransaction.ShouldBeFalse();
        command.Sql.ShouldContain("begin;");
        command.Sql.ShouldContain("commit;");

        // Nor for a delta written out on its own
        PostgresqlMigrator.RunsInOneTransaction(new StringWriter()).ShouldBeFalse();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void runs_in_one_transaction_inside_a_migration_script(bool transactional)
    {
        var custom = new TransactionAwareTableDelta(others(true), others(false));
        var migrator = new PostgresqlMigrator { IsTransactional = transactional };

        var writer = new StringWriter();
        migrator.WriteScript(writer, (m, w) => new SchemaMigration(custom).WriteAllUpdates(w, m, AutoCreate.All));

        // The DO block of a transactional script is one transaction, and has no begin or commit to offer
        custom.Answers.ShouldBe([transactional]);
    }

    [Fact]
    public async Task a_custom_delta_that_asks_stays_in_the_migrators_transaction()
    {
        await startWithOneRowAsync();
        await CreateSchemaObjectInDatabase(others(false));

        var custom = new TransactionAwareTableDelta(others(true), await others(false).FetchExistingAsync(theConnection));

        // Fails on create, after the rebuild and the custom delta, in the same command
        var broken = new Table($"{Schema}.broken");
        broken.AddColumn("id", "not_a_type").AsPrimaryKey();

        var migration = await partitioningMigrationAsync(custom, new SchemaObjectDelta(broken, SchemaPatchDifference.Create));

        await Should.ThrowAsync<Exception>(() =>
            new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate));

        // A commit of its own would have ended the migrator's transaction after the custom delta, and
        // left the rebuild and the new column in place
        (await isPartitionedAsync()).ShouldBeFalse();
        (await others(true).FindDeltaAsync(theConnection)).Difference.ShouldNotBe(SchemaPatchDifference.None);
        (await rowExistsAsync(1)).ShouldBeTrue();
        custom.Answers.ShouldBe([true]);
    }

    [Fact]
    public void a_migration_without_a_rebuild_is_unchanged()
    {
        var created = new Table($"{Schema}.created");
        created.AddColumn<int>("id").AsPrimaryKey();

        var migration = new SchemaMigration(new ISchemaObjectDelta[]
        {
            new SchemaObjectDelta(created, SchemaPatchDifference.Create)
        });

        var commands = new PostgresqlMigrator().BuildMigrationCommands(migration, AutoCreate.CreateOrUpdate);

        commands.ShouldAllBe(x => !x.ExplicitTransaction);
        commands.ShouldAllBe(x => !x.Sql.Contains("begin;"));
    }
}
