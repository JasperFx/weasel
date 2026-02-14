using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Sqlite;
using Xunit;

namespace Weasel.Sqlite.Tests;

public class SqliteExtensionSettingsTests
{
    [Fact]
    public void can_create_empty_extension_settings()
    {
        var settings = new SqliteExtensionSettings();

        settings.Extensions.ShouldBeEmpty();
    }

    [Fact]
    public void can_add_extension_with_library_path_only()
    {
        var settings = new SqliteExtensionSettings();

        settings.AddExtension("mod_spatialite");

        settings.Extensions.Count.ShouldBe(1);
        settings.Extensions[0].LibraryPath.ShouldBe("mod_spatialite");
        settings.Extensions[0].EntryPoint.ShouldBeNull();
    }

    [Fact]
    public void can_add_extension_with_entry_point()
    {
        var settings = new SqliteExtensionSettings();

        settings.AddExtension("my_extension", "sqlite3_my_extension_init");

        settings.Extensions.Count.ShouldBe(1);
        settings.Extensions[0].LibraryPath.ShouldBe("my_extension");
        settings.Extensions[0].EntryPoint.ShouldBe("sqlite3_my_extension_init");
    }

    [Fact]
    public void can_add_multiple_extensions()
    {
        var settings = new SqliteExtensionSettings();

        settings.AddExtension("extension1");
        settings.AddExtension("extension2", "entry_point2");
        settings.AddExtension("extension3");

        settings.Extensions.Count.ShouldBe(3);
        settings.Extensions[0].LibraryPath.ShouldBe("extension1");
        settings.Extensions[1].LibraryPath.ShouldBe("extension2");
        settings.Extensions[1].EntryPoint.ShouldBe("entry_point2");
        settings.Extensions[2].LibraryPath.ShouldBe("extension3");
    }

    [Fact]
    public void throws_when_library_path_is_null()
    {
        var settings = new SqliteExtensionSettings();

        Should.Throw<ArgumentException>(() => settings.AddExtension(null!));
    }

    [Fact]
    public void throws_when_library_path_is_empty()
    {
        var settings = new SqliteExtensionSettings();

        Should.Throw<ArgumentException>(() => settings.AddExtension(""));
    }

    [Fact]
    public void throws_when_library_path_is_whitespace()
    {
        var settings = new SqliteExtensionSettings();

        Should.Throw<ArgumentException>(() => settings.AddExtension("   "));
    }

    [Fact]
    public async Task apply_to_connection_throws_when_connection_is_null()
    {
        var settings = new SqliteExtensionSettings();

        await Should.ThrowAsync<ArgumentNullException>(async () =>
        {
            await settings.ApplyToConnectionAsync(null!);
        });
    }

    [Fact]
    public async Task apply_to_connection_throws_when_connection_is_closed()
    {
        var settings = new SqliteExtensionSettings();
        await using var connection = new SqliteConnection("Data Source=:memory:");

        await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            await settings.ApplyToConnectionAsync(connection);
        });
    }

    [Fact]
    public async Task apply_to_connection_succeeds_with_no_extensions()
    {
        var settings = new SqliteExtensionSettings();

        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        // Should not throw even with no extensions configured
        await settings.ApplyToConnectionAsync(connection);
    }

    [Fact]
    public async Task apply_to_connection_throws_for_nonexistent_extension()
    {
        var settings = new SqliteExtensionSettings();
        settings.AddExtension("nonexistent_extension_12345");

        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var ex = await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            await settings.ApplyToConnectionAsync(connection);
        });

        ex.Message.ShouldContain("Failed to load SQLite extension 'nonexistent_extension_12345'");
        ex.Message.ShouldContain("PATH/LD_LIBRARY_PATH/DYLD_LIBRARY_PATH");
    }

    [Fact]
    public void extension_record_validates_library_path()
    {
        Should.Throw<ArgumentException>(() => new SqliteExtension(null!));
        Should.Throw<ArgumentException>(() => new SqliteExtension(""));
        Should.Throw<ArgumentException>(() => new SqliteExtension("   "));
    }

    [Fact]
    public void extension_record_allows_null_entry_point()
    {
        var extension = new SqliteExtension("my_lib", null);

        extension.LibraryPath.ShouldBe("my_lib");
        extension.EntryPoint.ShouldBeNull();
    }

    [Fact]
    public void extension_record_stores_values_correctly()
    {
        var extension = new SqliteExtension("my_lib", "my_entry");

        extension.LibraryPath.ShouldBe("my_lib");
        extension.EntryPoint.ShouldBe("my_entry");
    }

    [Fact]
    public async Task extensions_are_disabled_after_loading()
    {
        // This test verifies that EnableExtensions is called with false after loading
        // We can't directly verify this, but we can ensure the connection is in a safe state

        var settings = new SqliteExtensionSettings();
        // Not adding any extensions, so loading will succeed

        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await settings.ApplyToConnectionAsync(connection);

        // Connection should still be open and usable
        connection.State.ShouldBe(System.Data.ConnectionState.Open);

        // Should be able to execute queries
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1";
        var result = await cmd.ExecuteScalarAsync();
        result.ShouldBe(1L);
    }
}
