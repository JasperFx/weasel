using Microsoft.Data.Sqlite;
using Shouldly;
using Xunit;

namespace Weasel.Sqlite.Tests;

/// <summary>
///     Tests for the split between connection-scoped and database-file-scoped PRAGMAs,
///     the precomputed batch SQL caching, and SqliteDataSource applying the file-scoped
///     batch only once per data source. See JasperFx/weasel#562.
/// </summary>
public class SqlitePragmaScopeTests
{
    [Fact]
    public void connection_batch_contains_only_connection_scoped_pragmas()
    {
        var sql = SqlitePragmaSettings.Default.ConnectionPragmaSql;

        sql.ShouldContain("PRAGMA busy_timeout = 5000");
        sql.ShouldContain("PRAGMA synchronous = NORMAL");
        sql.ShouldContain("PRAGMA cache_size = -64000");
        sql.ShouldContain("PRAGMA temp_store = 2");
        sql.ShouldContain("PRAGMA mmap_size = 268435456");
        sql.ShouldContain("PRAGMA foreign_keys = ON");
        sql.ShouldContain("PRAGMA secure_delete = OFF");
        sql.ShouldContain("PRAGMA case_sensitive_like = OFF");

        // database-file scoped PRAGMAs do not belong in the per-connection batch
        sql.ShouldNotContain("page_size");
        sql.ShouldNotContain("auto_vacuum");
        sql.ShouldNotContain("journal_mode");
    }

    [Fact]
    public void database_batch_contains_only_file_scoped_pragmas()
    {
        var sql = SqlitePragmaSettings.Default.DatabasePragmaSql;

        sql.ShouldContain("PRAGMA page_size = 4096");
        sql.ShouldContain("PRAGMA auto_vacuum = 2");
        sql.ShouldContain("PRAGMA journal_mode = WAL");

        sql.ShouldNotContain("busy_timeout");
        sql.ShouldNotContain("foreign_keys");
        sql.ShouldNotContain("synchronous");
    }

    [Fact]
    public void busy_timeout_leads_the_connection_batch()
    {
        // so that any later statement that has to take a file lock benefits from the timeout
        SqlitePragmaSettings.Default.ConnectionPragmaSql
            .ShouldStartWith("PRAGMA busy_timeout = ");
    }

    [Fact]
    public void page_size_precedes_wal_in_the_database_batch()
    {
        // page_size cannot change once a new database has entered WAL mode
        var sql = SqlitePragmaSettings.Default.DatabasePragmaSql;
        sql.IndexOf("page_size", StringComparison.Ordinal)
            .ShouldBeLessThan(sql.IndexOf("journal_mode", StringComparison.Ordinal));
    }

    [Fact]
    public void non_wal_journal_mode_is_a_connection_scoped_pragma()
    {
        // DELETE/TRUNCATE/PERSIST/MEMORY/OFF are per-connection settings, not persisted in the file
        var settings = new SqlitePragmaSettings { JournalMode = JournalMode.MEMORY };

        settings.ConnectionPragmaSql.ShouldContain("PRAGMA journal_mode = MEMORY");
        settings.DatabasePragmaSql.ShouldNotContain("journal_mode");
    }

    [Fact]
    public void wal_autocheckpoint_is_a_connection_scoped_pragma()
    {
        // wal_autocheckpoint maps to sqlite3_wal_autocheckpoint, a per-connection setting
        var settings = new SqlitePragmaSettings { JournalMode = JournalMode.WAL, WalAutoCheckpoint = 1500 };

        settings.ConnectionPragmaSql.ShouldContain("PRAGMA wal_autocheckpoint = 1500");
        settings.DatabasePragmaSql.ShouldNotContain("wal_autocheckpoint");
    }

    [Fact]
    public void batch_sql_is_computed_once_and_cached()
    {
        var settings = SqlitePragmaSettings.Default;

        ReferenceEquals(settings.ConnectionPragmaSql, settings.ConnectionPragmaSql).ShouldBeTrue();
        ReferenceEquals(settings.DatabasePragmaSql, settings.DatabasePragmaSql).ShouldBeTrue();
    }

    [Fact]
    public void mutating_a_setting_invalidates_the_cached_batches()
    {
        var settings = SqlitePragmaSettings.Default;
        var before = settings.ConnectionPragmaSql;
        before.ShouldContain("PRAGMA busy_timeout = 5000");

        settings.BusyTimeout = 12345;

        settings.ConnectionPragmaSql.ShouldContain("PRAGMA busy_timeout = 12345");
        settings.ConnectionPragmaSql.ShouldNotContain("PRAGMA busy_timeout = 5000");

        var databaseBefore = settings.DatabasePragmaSql;
        settings.PageSize = 8192;
        settings.DatabasePragmaSql.ShouldNotBe(databaseBefore);
        settings.DatabasePragmaSql.ShouldContain("PRAGMA page_size = 8192");
    }

    [Fact]
    public async Task data_source_applies_database_pragmas_only_once_per_data_source()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"pragma_scope_{Guid.NewGuid():N}.db");
        var settings = new CountingPragmaSettings();

        try
        {
            await using (var dataSource = new SqliteDataSource($"Data Source={tempFile}", settings))
            {
                await using (var conn1 = await dataSource.OpenConnectionAsync())
                {
                    var cmd = conn1.CreateCommand();
                    cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY)";
                    await cmd.ExecuteNonQueryAsync();
                }

                await using (await dataSource.OpenConnectionAsync())
                {
                }

                using ((SqliteConnection)dataSource.OpenConnection())
                {
                }

                settings.DatabaseApplications.ShouldBe(1);
                settings.ConnectionApplications.ShouldBe(3);
            }
        }
        finally
        {
            cleanUp(tempFile);
        }
    }

    [Fact]
    public async Task new_connections_see_wal_without_reapplication()
    {
        // journal_mode = WAL is persisted in the database file header, so a second connection
        // must observe WAL even though the data source only issued the PRAGMA once
        var tempFile = Path.Combine(Path.GetTempPath(), $"pragma_scope_{Guid.NewGuid():N}.db");
        var settings = new CountingPragmaSettings();

        try
        {
            await using (var dataSource = new SqliteDataSource($"Data Source={tempFile}", settings))
            {
                await using (var conn1 = await dataSource.OpenConnectionAsync())
                {
                    // force the database file to actually be created
                    var create = conn1.CreateCommand();
                    create.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY)";
                    await create.ExecuteNonQueryAsync();
                }

                await using var conn2 = await dataSource.OpenConnectionAsync();
                var cmd = conn2.CreateCommand();
                cmd.CommandText = "PRAGMA journal_mode";
                var mode = await cmd.ExecuteScalarAsync();
                mode!.ToString()!.ToUpperInvariant().ShouldBe("WAL");

                settings.DatabaseApplications.ShouldBe(1);
            }
        }
        finally
        {
            cleanUp(tempFile);
        }
    }

    [Fact]
    public async Task connection_pragmas_are_still_applied_on_every_open()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"pragma_scope_{Guid.NewGuid():N}.db");
        var settings = new SqlitePragmaSettings { BusyTimeout = 7321, ForeignKeys = true };

        try
        {
            await using (var dataSource = new SqliteDataSource($"Data Source={tempFile}", settings))
            {
                await using (await dataSource.OpenConnectionAsync())
                {
                }

                // busy_timeout and foreign_keys are connection-local, so a later connection only
                // has them because the data source applied the per-connection batch again
                await using var conn2 = await dataSource.OpenConnectionAsync();
                (await pragmaValueAsync((SqliteConnection)conn2, "busy_timeout")).ShouldBe("7321");
                (await pragmaValueAsync((SqliteConnection)conn2, "foreign_keys")).ShouldBe("1");
            }
        }
        finally
        {
            cleanUp(tempFile);
        }
    }

    [Fact]
    public void synchronous_open_applies_pragmas_without_blocking_on_async()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"pragma_scope_{Guid.NewGuid():N}.db");
        var settings = new SqlitePragmaSettings { BusyTimeout = 4444 };

        try
        {
            using var dataSource = new SqliteDataSource($"Data Source={tempFile}", settings);
            using var conn = (SqliteConnection)dataSource.OpenConnection();

            var cmd = conn.CreateCommand();
            cmd.CommandText = "PRAGMA busy_timeout";
            cmd.ExecuteScalar()!.ToString().ShouldBe("4444");

            cmd.CommandText = "PRAGMA journal_mode";
            cmd.ExecuteScalar()!.ToString()!.ToUpperInvariant().ShouldBe("WAL");
        }
        finally
        {
            cleanUp(tempFile);
        }
    }

    [Fact]
    public async Task in_memory_data_source_applies_database_pragmas_per_connection()
    {
        // a private :memory: database is a brand-new database per connection, so the
        // file-scoped batch cannot be skipped after the first open
        var settings = new CountingPragmaSettings();
        await using var dataSource = new SqliteDataSource("Data Source=:memory:", settings);

        await using (await dataSource.OpenConnectionAsync())
        {
        }

        await using (await dataSource.OpenConnectionAsync())
        {
        }

        settings.DatabaseApplications.ShouldBe(2);
        settings.ConnectionApplications.ShouldBe(2);
    }

    [Fact]
    public async Task full_apply_to_connection_still_covers_both_scopes()
    {
        // ApplyToConnectionAsync keeps its historical "apply everything" semantics
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var settings = new SqlitePragmaSettings { BusyTimeout = 9999, ForeignKeys = true };
        await settings.ApplyToConnectionAsync(connection);

        (await pragmaValueAsync(connection, "busy_timeout")).ShouldBe("9999");
        (await pragmaValueAsync(connection, "foreign_keys")).ShouldBe("1");
    }

    private static async Task<string> pragmaValueAsync(SqliteConnection connection, string pragmaName)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"PRAGMA {pragmaName}";
        var result = await cmd.ExecuteScalarAsync();
        return result?.ToString() ?? "";
    }

    private static void cleanUp(string tempFile)
    {
        SqliteConnection.ClearAllPools();
        foreach (var file in new[] { tempFile, tempFile + "-wal", tempFile + "-shm" })
        {
            try
            {
                if (File.Exists(file)) File.Delete(file);
            }
            catch
            {
                // ignore cleanup errors
            }
        }
    }

    /// <summary>
    ///     Spy that counts how often each scope of PRAGMAs is applied.
    /// </summary>
    private sealed class CountingPragmaSettings : SqlitePragmaSettings
    {
        private int _connectionApplications;
        private int _databaseApplications;

        public int ConnectionApplications => _connectionApplications;
        public int DatabaseApplications => _databaseApplications;

        public override Task ApplyConnectionPragmasAsync(SqliteConnection connection, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _connectionApplications);
            return base.ApplyConnectionPragmasAsync(connection, ct);
        }

        public override void ApplyConnectionPragmas(SqliteConnection connection)
        {
            Interlocked.Increment(ref _connectionApplications);
            base.ApplyConnectionPragmas(connection);
        }

        public override Task ApplyDatabasePragmasAsync(SqliteConnection connection, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _databaseApplications);
            return base.ApplyDatabasePragmasAsync(connection, ct);
        }

        public override void ApplyDatabasePragmas(SqliteConnection connection)
        {
            Interlocked.Increment(ref _databaseApplications);
            base.ApplyDatabasePragmas(connection);
        }
    }
}
