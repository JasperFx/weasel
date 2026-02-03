using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Sqlite;
using Xunit;

namespace Weasel.Sqlite.Tests;

public class SqlitePragmaSettingsTests
{
    [Fact]
    public void default_settings_have_sensible_values()
    {
        var settings = SqlitePragmaSettings.Default;

        settings.JournalMode.ShouldBe(JournalMode.WAL);
        settings.Synchronous.ShouldBe(SynchronousMode.NORMAL);
        settings.CacheSize.ShouldBe(-64000); // 64MB
        settings.TempStore.ShouldBe(TempStoreMode.MEMORY);
        settings.PageSize.ShouldBe(4096);
        settings.ForeignKeys.ShouldBeTrue();
        settings.AutoVacuum.ShouldBe(AutoVacuumMode.INCREMENTAL);
        settings.BusyTimeout.ShouldBe(5000);
        settings.SecureDelete.ShouldBeFalse();
    }

    [Fact]
    public void high_performance_settings_prioritize_speed()
    {
        var settings = SqlitePragmaSettings.HighPerformance;

        settings.JournalMode.ShouldBe(JournalMode.WAL);
        settings.Synchronous.ShouldBe(SynchronousMode.OFF); // Fastest
        settings.CacheSize.ShouldBe(-128000); // Larger cache
        settings.AutoVacuum.ShouldBe(AutoVacuumMode.NONE); // No vacuum overhead
    }

    [Fact]
    public void high_safety_settings_prioritize_durability()
    {
        var settings = SqlitePragmaSettings.HighSafety;

        settings.Synchronous.ShouldBe(SynchronousMode.FULL); // Maximum safety
        settings.AutoVacuum.ShouldBe(AutoVacuumMode.FULL);
        settings.SecureDelete.ShouldBeTrue(); // Secure data deletion
        settings.BusyTimeout.ShouldBe(10000); // Longer timeout
    }

    [Fact]
    public void can_create_custom_settings()
    {
        var settings = new SqlitePragmaSettings
        {
            JournalMode = JournalMode.TRUNCATE,
            Synchronous = SynchronousMode.FULL,
            CacheSize = -32000,
            ForeignKeys = false
        };

        settings.JournalMode.ShouldBe(JournalMode.TRUNCATE);
        settings.Synchronous.ShouldBe(SynchronousMode.FULL);
        settings.CacheSize.ShouldBe(-32000);
        settings.ForeignKeys.ShouldBeFalse();
    }

    [Fact]
    public async Task apply_settings_to_connection()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var settings = new SqlitePragmaSettings
        {
            JournalMode = JournalMode.MEMORY,
            Synchronous = SynchronousMode.OFF,
            CacheSize = -16000,
            ForeignKeys = true
        };

        await settings.ApplyToConnectionAsync(connection);

        // Verify settings were applied
        var journalMode = await GetPragmaValueAsync(connection, "journal_mode");
        journalMode.ToLowerInvariant().ShouldBe("memory");

        var synchronous = await GetPragmaValueAsync(connection, "synchronous");
        synchronous.ShouldBe("0"); // OFF = 0

        var cacheSize = await GetPragmaValueAsync(connection, "cache_size");
        cacheSize.ShouldBe("-16000");

        var foreignKeys = await GetPragmaValueAsync(connection, "foreign_keys");
        foreignKeys.ShouldBe("1"); // ON = 1
    }

    [Fact]
    public async Task wal_mode_with_auto_checkpoint()
    {
        var tempFile = Path.GetTempFileName();
        try
        {
            {
                await using var connection = new SqliteConnection($"Data Source={tempFile}");
                await connection.OpenAsync();

                var settings = new SqlitePragmaSettings
                {
                    JournalMode = JournalMode.WAL,
                    WalAutoCheckpoint = 2000
                };

                await settings.ApplyToConnectionAsync(connection);

                var journalMode = await GetPragmaValueAsync(connection, "journal_mode");
                journalMode.ToLowerInvariant().ShouldBe("wal");

                var autoCheckpoint = await GetPragmaValueAsync(connection, "wal_autocheckpoint");
                autoCheckpoint.ShouldBe("2000");
            }

            // Force garbage collection to ensure connection is fully disposed on Windows
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
        finally
        {
            // Clean up temp file and WAL files
            try
            {
                if (File.Exists(tempFile))
                {
                    File.Delete(tempFile);
                }
                if (File.Exists(tempFile + "-wal"))
                {
                    File.Delete(tempFile + "-wal");
                }
                if (File.Exists(tempFile + "-shm"))
                {
                    File.Delete(tempFile + "-shm");
                }
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    [Fact]
    public async Task foreign_keys_enforcement()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var settings = new SqlitePragmaSettings { ForeignKeys = true };
        await settings.ApplyToConnectionAsync(connection);

        // Create parent table
        await using var createParent = connection.CreateCommand();
        createParent.CommandText = "CREATE TABLE parent (id INTEGER PRIMARY KEY)";
        await createParent.ExecuteNonQueryAsync();

        // Create child table with foreign key
        await using var createChild = connection.CreateCommand();
        createChild.CommandText = @"
            CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                FOREIGN KEY (parent_id) REFERENCES parent(id)
            )";
        await createChild.ExecuteNonQueryAsync();

        // Try to insert invalid foreign key - should fail
        await using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO child (id, parent_id) VALUES (1, 999)";

        await Should.ThrowAsync<SqliteException>(async () =>
        {
            await insertCmd.ExecuteNonQueryAsync();
        });
    }

    [Fact]
    public void generate_sql_script()
    {
        var settings = SqlitePragmaSettings.Default;

        var script = settings.ToSqlScript();

        script.ShouldContain("PRAGMA journal_mode = WAL");
        script.ShouldContain("PRAGMA synchronous = NORMAL");
        script.ShouldContain("PRAGMA cache_size = -64000");
        script.ShouldContain("PRAGMA foreign_keys = ON");
        script.ShouldContain("PRAGMA temp_store = 2");
        script.ShouldContain("-- SQLite PRAGMA Settings");
    }

    [Fact]
    public void sql_script_includes_wal_checkpoint_when_set()
    {
        var settings = new SqlitePragmaSettings
        {
            JournalMode = JournalMode.WAL,
            WalAutoCheckpoint = 1500
        };

        var script = settings.ToSqlScript();

        script.ShouldContain("PRAGMA wal_autocheckpoint = 1500");
    }

    [Fact]
    public void sql_script_excludes_wal_checkpoint_for_non_wal_mode()
    {
        var settings = new SqlitePragmaSettings
        {
            JournalMode = JournalMode.DELETE,
            WalAutoCheckpoint = 1500
        };

        var script = settings.ToSqlScript();

        script.ShouldNotContain("wal_autocheckpoint");
    }

    [Fact]
    public async Task mmap_size_setting()
    {
        var tempFile = Path.GetTempFileName();
        try
        {
            {
                await using var connection = new SqliteConnection($"Data Source={tempFile}");
                await connection.OpenAsync();

                var settings = new SqlitePragmaSettings
                {
                    MmapSize = 134217728 // 128MB
                };

                await settings.ApplyToConnectionAsync(connection);

                var mmapSize = await GetPragmaValueAsync(connection, "mmap_size");
                mmapSize.ShouldBe("134217728");
            }

            // Force garbage collection to ensure connection is fully disposed on Windows
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
        finally
        {
            try
            {
                if (File.Exists(tempFile))
                {
                    File.Delete(tempFile);
                }
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    [Fact]
    public async Task busy_timeout_setting()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var settings = new SqlitePragmaSettings
        {
            BusyTimeout = 10000
        };

        await settings.ApplyToConnectionAsync(connection);

        var busyTimeout = await GetPragmaValueAsync(connection, "busy_timeout");
        busyTimeout.ShouldBe("10000");
    }

    [Fact]
    public async Task secure_delete_setting()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var settings = new SqlitePragmaSettings
        {
            SecureDelete = true
        };

        await settings.ApplyToConnectionAsync(connection);

        var secureDelete = await GetPragmaValueAsync(connection, "secure_delete");
        secureDelete.ShouldBe("1");
    }

    private static async Task<string> GetPragmaValueAsync(SqliteConnection connection, string pragmaName)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"PRAGMA {pragmaName}";
        var result = await cmd.ExecuteScalarAsync();
        return result?.ToString() ?? "";
    }
}
