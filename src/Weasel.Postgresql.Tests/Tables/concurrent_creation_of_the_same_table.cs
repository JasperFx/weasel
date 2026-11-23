using JasperFx;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     weasel#918. <c>CREATE TABLE IF NOT EXISTS</c> is not atomic against a concurrent creator: PostgreSQL
///     checks the catalog for the name, then inserts the table's row type into <c>pg_type</c>. A session that
///     passes the check while another session's create is still uncommitted then waits on that session's
///     unique index entry, and once it commits fails with
///     <c>23505 duplicate key value violates unique constraint "pg_type_typname_nsp_index"</c> (or
///     <c>42P07</c> when the second check sees it). The migration asked for the table to exist, and it does.
///     Two Marten nodes starting together with <c>AutoCreate</c> on hit exactly this.
/// </summary>
[Collection("create_table_race")]
public class concurrent_creation_of_the_same_table: IntegrationContext
{
    private const string Schema = "create_table_race";

    public concurrent_creation_of_the_same_table(): base(Schema)
    {
    }

    private static Table buildTable(string name)
    {
        var table = new Table($"{Schema}.{name}");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<int>("version");
        table.Indexes.Add(new IndexDefinition($"idx_{name}_name") { Columns = ["name"] });
        return table;
    }

    /// <summary>
    ///     The deterministic version of the race: one session holds an uncommitted create of the table while
    ///     the migrator runs its own, which then waits on that session and loses once it commits.
    /// </summary>
    [Fact]
    public async Task a_migration_that_loses_the_create_race_still_succeeds()
    {
        await ResetSchema();

        var table = buildTable("things");

        await using var migrating = await theDataSource.OpenConnectionAsync();
        var migration = await SchemaMigration.DetermineAsync(migrating, table);
        migration.Difference.ShouldBe(SchemaPatchDifference.Create);

        // The other node, part way through creating the same table
        await using var winner = await theDataSource.OpenConnectionAsync();
        await using var tx = await winner.BeginTransactionAsync();
        var ddl = new StringWriter();
        table.WriteCreateStatement(new PostgresqlMigrator(), ddl);
        await using (var create = new NpgsqlCommand(ddl.ToString(), winner, tx))
        {
            await create.ExecuteNonQueryAsync();
        }

        var applying = new PostgresqlMigrator().ApplyAllAsync(migrating, migration, AutoCreate.CreateOrUpdate);

        await waitUntilBlockedAsync(migrating.ProcessID, applying);

        await tx.CommitAsync();

        // Pre-fix: PostgresException 23505 on pg_type_typname_nsp_index
        await applying;

        var again = await SchemaMigration.DetermineAsync(theConnection, table);
        again.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The race as it happens in the wild: several sessions, each having found the table missing, all
    ///     applying the same migration at once. Every one of them has to succeed.
    /// </summary>
    [Fact]
    public async Task many_sessions_creating_the_same_table_at_once_all_succeed()
    {
        const int sessions = 8;
        const int rounds = 8;

        await ResetSchema();

        for (var round = 0; round < rounds; round++)
        {
            var name = $"round_{round}";

            var connections = new List<NpgsqlConnection>();
            var migrations = new List<SchemaMigration>();

            try
            {
                for (var i = 0; i < sessions; i++)
                {
                    var conn = await theDataSource.OpenConnectionAsync();
                    connections.Add(conn);
                    migrations.Add(await SchemaMigration.DetermineAsync(conn, buildTable(name)));
                }

                var go = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var applying = connections.Select(async (conn, i) =>
                {
                    await go.Task;
                    await new PostgresqlMigrator().ApplyAllAsync(conn, migrations[i], AutoCreate.CreateOrUpdate);
                }).ToArray();

                go.SetResult();

                // Pre-fix: some of these fail with 23505 (pg_type_typname_nsp_index) or 42P07
                await Task.WhenAll(applying);
            }
            finally
            {
                foreach (var conn in connections) await conn.DisposeAsync();
            }

            var again = await SchemaMigration.DetermineAsync(theConnection, buildTable(name));
            again.Difference.ShouldBe(SchemaPatchDifference.None);
        }
    }

    /// <summary>
    ///     The guard on the catalog's schema is what keeps this from being swallowed: a unique violation in a
    ///     user schema is data, and no amount of determining the migration again will change it.
    /// </summary>
    [Fact]
    public async Task a_unique_violation_in_the_data_still_fails_the_migration()
    {
        await ResetSchema();

        var table = new Table($"{Schema}.duplicated");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        await CreateSchemaObjectInDatabase(table);

        await theConnection.CreateCommand(
                $"insert into {Schema}.duplicated (id, name) values (1, 'same'), (2, 'same')")
            .ExecuteNonQueryAsync();

        table.Indexes.Add(new IndexDefinition("idx_duplicated_name") { Columns = ["name"], IsUnique = true });

        var migration = await SchemaMigration.DetermineAsync(theConnection, table);

        var e = await Should.ThrowAsync<PostgresException>(() =>
            new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate));

        e.SqlState.ShouldBe(PostgresErrorCodes.UniqueViolation);
    }

    /// <summary>
    ///     A 42P07 that no other session caused comes back the same however often the migration is determined
    ///     again, and has to be reported rather than looped on.
    /// </summary>
    [Fact]
    public async Task a_name_that_is_genuinely_taken_still_fails_the_migration()
    {
        await ResetSchema();

        var other = new Table($"{Schema}.taken");
        other.AddColumn<int>("id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(other);

        // An index may not share a name with a table in its schema
        var table = new Table($"{Schema}.colliding");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.Indexes.Add(new IndexDefinition("taken") { Columns = ["name"] });

        var migration = await SchemaMigration.DetermineAsync(theConnection, table);

        var e = await Should.ThrowAsync<PostgresException>(() =>
            new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate));

        e.SqlState.ShouldBe(PostgresErrorCodes.DuplicateTable);
    }

    private async Task waitUntilBlockedAsync(int pid, Task applying)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        await using var probe = await theDataSource.OpenConnectionAsync();

        while (DateTime.UtcNow < deadline)
        {
            if (applying.IsCompleted)
            {
                // Surfaces whatever it failed with instead of a misleading timeout
                await applying;
                throw new InvalidOperationException("The migration finished without waiting on the other session");
            }

            await using var cmd = new NpgsqlCommand("select count(*) from pg_locks where pid = @pid and not granted", probe);
            cmd.Parameters.AddWithValue("pid", pid);
            if (Convert.ToInt64(await cmd.ExecuteScalarAsync()) > 0)
            {
                return;
            }

            await Task.Delay(25);
        }

        throw new TimeoutException("The migration never blocked on the uncommitted create");
    }
}
