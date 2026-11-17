using System.Diagnostics;
using JasperFx;
using JasperFx.Core;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Connections;
using Weasel.Postgresql.Migrations;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Migrations;

/// <summary>
///     weasel#875: <c>CREATE DATABASE</c>, <c>DROP DATABASE</c> and extension installs get
///     <c>ProvisioningCommandTimeout</c> (two minutes by default) instead of the connection's command
///     timeout, which is usually tuned for queries. Every connection here has a one second command timeout.
/// </summary>
/// <remarks>
///     Databases are server-scoped and several worker lanes share one server, so every database here has a
///     unique name and is dropped afterwards. The slow statements are made slow by a second session holding
///     something the statement has to wait for, only ever on this test's own database.
/// </remarks>
public class provisioning_command_timeout: IAsyncLifetime
{
    private readonly List<string> _created = [];

    private static string OneSecondTimeout(string connectionString, string? database = null)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { CommandTimeout = 1 };
        if (database != null)
        {
            builder.Database = database;
        }

        return builder.ConnectionString;
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        foreach (var name in _created)
        {
            await conn.KillIdleSessions(name);
            await conn.DropDatabase(name);
        }
    }

    private string NewDatabaseName()
    {
        var name = "weasel_provisioning_" + Guid.NewGuid().ToString("N")[..12];
        _created.Add(name);
        return name;
    }

    private static async Task<bool> ExistsOnServer(string name)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        return await conn.DatabaseExists(name);
    }

    private static async Task CreateOnServer(string name)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await new DatabaseSpecification().BuildDatabase(conn, name);
    }

    [Theory]
    [InlineData(1, 120, 120)] // the short query timeout is lengthened
    [InlineData(300, 120, 300)] // a longer connection timeout is kept
    [InlineData(0, 120, 0)] // so is an unlimited one
    [InlineData(1, 0, 0)] // zero means no limit
    [InlineData(1, 1.5, 2)] // whole seconds, rounded up
    public void provisioning_timeout_only_ever_lengthens_the_command_timeout(
        int connectionTimeoutSeconds, double provisioningSeconds, int expected)
    {
        var connectionString =
            new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString) { CommandTimeout = connectionTimeoutSeconds }
                .ConnectionString;
        using var conn = new NpgsqlConnection(connectionString);
        using var cmd = new NpgsqlCommand("select 1", conn);

        cmd.WithProvisioningTimeout(TimeSpan.FromSeconds(provisioningSeconds)).CommandTimeout.ShouldBe(expected);
    }

    [Fact]
    public void an_infinite_provisioning_timeout_means_no_limit()
    {
        using var conn = new NpgsqlConnection(OneSecondTimeout(ConnectionSource.ConnectionString));
        using var cmd = new NpgsqlCommand("select 1", conn);

        cmd.WithProvisioningTimeout(Timeout.InfiniteTimeSpan).CommandTimeout.ShouldBe(0);
    }

    [Fact]
    public void the_default_is_two_minutes()
    {
        new PostgresqlMigrator().ProvisioningCommandTimeout.ShouldBe(2.Minutes());
        new Databases(ConnectionSource.ConnectionString).ProvisioningCommandTimeout.ShouldBe(2.Minutes());
    }

    [Fact]
    public async Task the_collection_gives_only_its_provisioning_statements_the_longer_timeout()
    {
        var databases = new Databases(OneSecondTimeout(ConnectionSource.ConnectionString));
        await using var conn = new NpgsqlConnection(OneSecondTimeout(ConnectionSource.ConnectionString));

        await using (var create = databases.CreateDatabaseCommand(conn, "anything"))
        {
            create.CommandTimeout.ShouldBe(120);
        }

        await using (var drop = CommandExtensions.DropDatabaseCommand(conn, "anything", databases.ProvisioningCommandTimeout))
        {
            drop.CommandTimeout.ShouldBe(120);
        }

        databases.ProvisioningCommandTimeout = 30.Seconds();
        await using (var create = databases.CreateDatabaseCommand(conn, "anything"))
        {
            create.CommandTimeout.ShouldBe(30);
        }

        // Ordinary commands keep the connection's timeout
        await using var query = conn.CreateCommand("select 1");
        query.CommandTimeout.ShouldBe(1);
    }

    [Fact]
    public async Task find_or_create_provisions_a_database_under_a_one_second_command_timeout()
    {
        var databases = new Databases(OneSecondTimeout(ConnectionSource.ConnectionString));
        var name = NewDatabaseName();

        await databases.FindOrCreateDatabase(name);

        (await ExistsOnServer(name)).ShouldBeTrue();
    }

    [Fact]
    public async Task find_or_create_synchronously_provisions_a_database_under_a_one_second_command_timeout()
    {
        var databases = new Databases(OneSecondTimeout(ConnectionSource.ConnectionString));
        var name = NewDatabaseName();

        databases.FindOrCreateDatabaseSynchronously(name);

        (await ExistsOnServer(name)).ShouldBeTrue();
    }

    [Fact]
    public async Task drop_and_recreate_waits_out_a_drop_that_takes_longer_than_the_command_timeout()
    {
        var name = NewDatabaseName();
        await CreateOnServer(name);

        var databases = new Databases(OneSecondTimeout(ConnectionSource.ConnectionString)) { DropAndRecreate = true };

        var elapsed = await whileTheDatabaseRowIsLocked(name, () => databases.FindOrCreateDatabase(name).AsTask());

        elapsed.ShouldBeGreaterThan(1.Seconds());
        (await ExistsOnServer(name)).ShouldBeTrue();
    }

    [Fact]
    public async Task drop_and_recreate_synchronously_waits_out_a_drop_that_takes_longer_than_the_command_timeout()
    {
        var name = NewDatabaseName();
        await CreateOnServer(name);

        var databases = new Databases(OneSecondTimeout(ConnectionSource.ConnectionString)) { DropAndRecreate = true };

        var elapsed = await whileTheDatabaseRowIsLocked(name,
            () => Task.Run(() => databases.FindOrCreateDatabaseSynchronously(name)));

        elapsed.ShouldBeGreaterThan(1.Seconds());
        (await ExistsOnServer(name)).ShouldBeTrue();
    }

    [Fact]
    public async Task the_same_slow_drop_times_out_with_a_one_second_provisioning_timeout()
    {
        // Proves the drop above really is slow, so the tests above are not vacuous
        var name = NewDatabaseName();
        await CreateOnServer(name);

        var databases = new Databases(OneSecondTimeout(ConnectionSource.ConnectionString))
        {
            DropAndRecreate = true, ProvisioningCommandTimeout = 1.Seconds()
        };

        await whileTheDatabaseRowIsLocked(name, async () =>
        {
            var ex = await Should.ThrowAsync<NpgsqlException>(() => databases.FindOrCreateDatabase(name).AsTask());
            ex.InnerException.ShouldBeOfType<TimeoutException>();
        });
    }

    [Fact]
    public async Task an_extension_install_waits_longer_than_the_command_timeout()
    {
        var name = NewDatabaseName();
        await CreateOnServer(name);

        var migrator = new PostgresqlMigrator();

        var elapsed = await whileTheExtensionIsBeingInstalledElsewhere(name,
            () => installExtension(migrator, name));

        elapsed.ShouldBeGreaterThan(1.Seconds());
    }

    [Fact]
    public async Task the_same_extension_install_times_out_with_a_one_second_provisioning_timeout()
    {
        var name = NewDatabaseName();
        await CreateOnServer(name);

        var migrator = new PostgresqlMigrator { ProvisioningCommandTimeout = 1.Seconds() };

        await whileTheExtensionIsBeingInstalledElsewhere(name, async () =>
        {
            var ex = await Should.ThrowAsync<NpgsqlException>(() => installExtension(migrator, name));
            ex.InnerException.ShouldBeOfType<TimeoutException>();
        });
    }

    [Fact]
    public void an_extension_install_is_its_own_provisioning_command_and_the_rest_are_not()
    {
        var things = new Table("provisioning.things");
        things.AddColumn<int>("id").AsPrimaryKey();
        var others = new Table("provisioning.others");
        others.AddColumn<int>("id").AsPrimaryKey();

        var migration = new SchemaMigration(new ISchemaObjectDelta[]
        {
            new SchemaObjectDelta(things, SchemaPatchDifference.Create),
            new SchemaObjectDelta(new Extension("ltree"), SchemaPatchDifference.Create),
            new SchemaObjectDelta(others, SchemaPatchDifference.Create)
        });

        var commands = new PostgresqlMigrator().BuildMigrationCommands(migration, AutoCreate.CreateOrUpdate);

        var provisioning = commands.Where(x => x.Provisioning).ToArray();
        provisioning.Length.ShouldBe(1);
        provisioning[0].Sql.Trim().ShouldBe("CREATE EXTENSION IF NOT EXISTS ltree;");

        commands.Where(x => !x.Provisioning).ShouldAllBe(x => !x.Sql.Contains("EXTENSION"));

        // Still in the order the deltas came in
        var all = string.Join("", commands.Select(x => x.Sql));
        all.IndexOf("provisioning.things").ShouldBeLessThan(all.IndexOf("EXTENSION"));
        all.IndexOf("EXTENSION").ShouldBeLessThan(all.IndexOf("provisioning.others"));
    }

    [Fact]
    public async Task ensure_database_exists_creates_a_database_under_a_one_second_command_timeout()
    {
        var name = NewDatabaseName();

        await using var conn = new NpgsqlConnection(OneSecondTimeout(ConnectionSource.ConnectionString, name));
        await new PostgresqlMigrator().EnsureDatabaseExistsAsync(conn);

        (await ExistsOnServer(name)).ShouldBeTrue();
    }

    private static async Task installExtension(PostgresqlMigrator migrator, string databaseName)
    {
        await using var conn = new NpgsqlConnection(OneSecondTimeout(ConnectionSource.ConnectionString, databaseName));
        await conn.OpenAsync();

        var migration = await SchemaMigration.DetermineAsync(conn, CancellationToken.None, new Extension("ltree"));
        migration.Difference.ShouldBe(SchemaPatchDifference.Create);

        await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate);
    }

    // DROP DATABASE has to delete the database's pg_database row, so it waits for a transaction that has
    // updated that row and not finished. The blocking session is not connected to the database itself, so
    // DropAndRecreate's kill of that database's sessions leaves it alone.
    private static Task<TimeSpan> whileTheDatabaseRowIsLocked(string databaseName, Func<Task> action) =>
        whileBlocked(ConnectionSource.ConnectionString, $"ALTER DATABASE {databaseName} CONNECTION LIMIT 50", action);

    // A second CREATE EXTENSION waits on pg_extension's unique name index for the first one's transaction.
    private static Task<TimeSpan> whileTheExtensionIsBeingInstalledElsewhere(string databaseName, Func<Task> action)
    {
        var connectionString =
            new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString) { Database = databaseName }.ConnectionString;
        return whileBlocked(connectionString, "CREATE EXTENSION ltree", action);
    }

    private static async Task<TimeSpan> whileBlocked(string connectionString, string blockingStatement, Func<Task> action)
    {
        await using var blocker = new NpgsqlConnection(connectionString);
        await blocker.OpenAsync();
        await using var tx = await blocker.BeginTransactionAsync();
        await using (var cmd = new NpgsqlCommand(blockingStatement, blocker, tx))
        {
            await cmd.ExecuteNonQueryAsync();
        }

        var stopwatch = Stopwatch.StartNew();
        var running = action();

        // Long enough to sink a one second command timeout, and well short of two minutes
        var released = Task.Delay(3.Seconds()).ContinueWith(_ => tx.RollbackAsync()).Unwrap();

        try
        {
            await running;
        }
        finally
        {
            await released;
        }

        return stopwatch.Elapsed;
    }

    public class Databases: SingleServerDatabaseCollection<TestDatabaseWithTables>
    {
        public Databases(string masterConnectionString)
            : base(new DefaultNpgsqlDataSourceFactory(), masterConnectionString)
        {
        }

        protected override TestDatabaseWithTables buildDatabase(string databaseName, NpgsqlDataSource dataSource)
        {
            return new TestDatabaseWithTables(databaseName, dataSource);
        }
    }
}
