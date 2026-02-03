using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Sqlite;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests;

[Collection("integration")]
public class TypeMappingIntegrationTests
{
    [Fact]
    public async Task datetime_roundtrip()
    {
        await using var connection = await OpenConnectionAsync();
        await CreateEventsTable(connection);

        var now = DateTime.UtcNow;
        var expected = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Millisecond, DateTimeKind.Utc);

        // Insert
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO events (id, name, created_at) VALUES (@id, @name, @created_at)";
        insertCmd.Parameters.AddWithValue("@id", 1);
        insertCmd.Parameters.AddWithValue("@name", "Test Event");
        insertCmd.Parameters.AddWithValue("@created_at", expected.ToString("o")); // ISO8601
        await insertCmd.ExecuteNonQueryAsync();

        // Read back
        var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT created_at FROM events WHERE id = 1";
        var result = await selectCmd.ExecuteScalarAsync();

        result.ShouldNotBeNull();
        var retrieved = DateTime.Parse(result.ToString()!, null, System.Globalization.DateTimeStyles.RoundtripKind);

        // SQLite stores as ISO8601 string, verify it roundtrips correctly with UTC preserved
        retrieved.Kind.ShouldBe(DateTimeKind.Utc);
        retrieved.Year.ShouldBe(expected.Year);
        retrieved.Month.ShouldBe(expected.Month);
        retrieved.Day.ShouldBe(expected.Day);
        retrieved.Hour.ShouldBe(expected.Hour);
        retrieved.Minute.ShouldBe(expected.Minute);
        retrieved.Second.ShouldBe(expected.Second);
    }

    [Fact]
    public async Task datetime_nullable_roundtrip()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("nullable_dates");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<DateTime?>("created_at");
        await CreateTable(connection, table);

        // Insert null
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO nullable_dates (id, created_at) VALUES (1, NULL)";
        await insertCmd.ExecuteNonQueryAsync();

        // Read back null
        var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT created_at FROM nullable_dates WHERE id = 1";
        var result = await selectCmd.ExecuteScalarAsync();

        result.ShouldBe(DBNull.Value);

        // Insert with value
        var now = DateTime.UtcNow;
        insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO nullable_dates (id, created_at) VALUES (2, @created_at)";
        insertCmd.Parameters.AddWithValue("@created_at", now.ToString("o"));
        await insertCmd.ExecuteNonQueryAsync();

        // Read back value
        selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT created_at FROM nullable_dates WHERE id = 2";
        result = await selectCmd.ExecuteScalarAsync();

        result.ShouldNotBe(DBNull.Value);
    }

    [Fact]
    public async Task datetimeoffset_roundtrip()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("scheduled_events");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<DateTimeOffset>("scheduled_for");
        await CreateTable(connection, table);

        var offset = DateTimeOffset.UtcNow;
        var expected = new DateTimeOffset(offset.Year, offset.Month, offset.Day, offset.Hour, offset.Minute, offset.Second, offset.Millisecond, offset.Offset);

        // Insert
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO scheduled_events (id, scheduled_for) VALUES (@id, @scheduled_for)";
        insertCmd.Parameters.AddWithValue("@id", 1);
        insertCmd.Parameters.AddWithValue("@scheduled_for", expected.ToString("o")); // ISO8601 with timezone
        await insertCmd.ExecuteNonQueryAsync();

        // Read back
        var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT scheduled_for FROM scheduled_events WHERE id = 1";
        var result = await selectCmd.ExecuteScalarAsync();

        result.ShouldNotBeNull();
        var retrieved = DateTimeOffset.Parse(result.ToString()!);

        // Verify roundtrip
        retrieved.Year.ShouldBe(expected.Year);
        retrieved.Month.ShouldBe(expected.Month);
        retrieved.Day.ShouldBe(expected.Day);
        retrieved.Hour.ShouldBe(expected.Hour);
        retrieved.Minute.ShouldBe(expected.Minute);
        retrieved.Offset.ShouldBe(expected.Offset);
    }

    [Fact]
    public async Task guid_as_primary_key()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("items");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        await CreateTable(connection, table);

        var id = Guid.NewGuid();

        // Insert
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO items (id, name) VALUES (@id, @name)";
        insertCmd.Parameters.AddWithValue("@id", id.ToString());
        insertCmd.Parameters.AddWithValue("@name", "Test Item");
        await insertCmd.ExecuteNonQueryAsync();

        // Read back
        var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT id, name FROM items WHERE id = @id";
        selectCmd.Parameters.AddWithValue("@id", id.ToString());

        await using var reader = await selectCmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        var retrievedId = Guid.Parse(await reader.GetFieldValueAsync<string>(0));
        var retrievedName = await reader.GetFieldValueAsync<string>(1);

        retrievedId.ShouldBe(id);
        retrievedName.ShouldBe("Test Item");
    }

    [Fact]
    public async Task guid_nullable_roundtrip()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("optional_guids");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<Guid?>("correlation_id");
        await CreateTable(connection, table);

        // Insert null
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO optional_guids (id, correlation_id) VALUES (1, NULL)";
        await insertCmd.ExecuteNonQueryAsync();

        // Read back null
        var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT correlation_id FROM optional_guids WHERE id = 1";
        var result = await selectCmd.ExecuteScalarAsync();

        result.ShouldBe(DBNull.Value);

        // Insert with value
        var guid = Guid.NewGuid();
        insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO optional_guids (id, correlation_id) VALUES (2, @guid)";
        insertCmd.Parameters.AddWithValue("@guid", guid.ToString());
        await insertCmd.ExecuteNonQueryAsync();

        // Read back value
        selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT correlation_id FROM optional_guids WHERE id = 2";
        result = await selectCmd.ExecuteScalarAsync();

        result.ShouldNotBe(DBNull.Value);
        var retrieved = Guid.Parse(result.ToString()!);
        retrieved.ShouldBe(guid);
    }

    [Fact]
    public async Task timespan_roundtrip()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("durations");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.AddColumn<TimeSpan>("duration");
        await CreateTable(connection, table);

        var duration = TimeSpan.FromHours(2.5);

        // Insert
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO durations (id, name, duration) VALUES (@id, @name, @duration)";
        insertCmd.Parameters.AddWithValue("@id", 1);
        insertCmd.Parameters.AddWithValue("@name", "Task Duration");
        insertCmd.Parameters.AddWithValue("@duration", duration.ToString());
        await insertCmd.ExecuteNonQueryAsync();

        // Read back
        var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT duration FROM durations WHERE id = 1";
        var result = await selectCmd.ExecuteScalarAsync();

        result.ShouldNotBeNull();
        var retrieved = TimeSpan.Parse(result.ToString()!);

        retrieved.ShouldBe(duration);
    }

    [Fact]
    public async Task timespan_nullable_roundtrip()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("optional_durations");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<TimeSpan?>("duration");
        await CreateTable(connection, table);

        // Insert null
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO optional_durations (id, duration) VALUES (1, NULL)";
        await insertCmd.ExecuteNonQueryAsync();

        // Read back null
        var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT duration FROM optional_durations WHERE id = 1";
        var result = await selectCmd.ExecuteScalarAsync();

        result.ShouldBe(DBNull.Value);

        // Insert with value
        var duration = TimeSpan.FromMinutes(45);
        insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO optional_durations (id, duration) VALUES (2, @duration)";
        insertCmd.Parameters.AddWithValue("@duration", duration.ToString());
        await insertCmd.ExecuteNonQueryAsync();

        // Read back value
        selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT duration FROM optional_durations WHERE id = 2";
        result = await selectCmd.ExecuteScalarAsync();

        result.ShouldNotBe(DBNull.Value);
        var retrieved = TimeSpan.Parse(result.ToString()!);
        retrieved.ShouldBe(duration);
    }

    [Fact]
    public async Task decimal_precision_roundtrip()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("financial_data");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<decimal>("amount");
        table.AddColumn<decimal>("rate");
        await CreateTable(connection, table);

        var amount = 12345.67m;
        var rate = 0.0525m;

        // Insert
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO financial_data (id, amount, rate) VALUES (@id, @amount, @rate)";
        insertCmd.Parameters.AddWithValue("@id", 1);
        insertCmd.Parameters.AddWithValue("@amount", amount);
        insertCmd.Parameters.AddWithValue("@rate", rate);
        await insertCmd.ExecuteNonQueryAsync();

        // Read back
        var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT amount, rate FROM financial_data WHERE id = 1";

        await using var reader = await selectCmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        // SQLite stores decimal as REAL (double), so there may be minor precision differences
        var retrievedAmount = await reader.GetFieldValueAsync<double>(0);
        var retrievedRate = await reader.GetFieldValueAsync<double>(1);

        // Verify values are very close (within acceptable floating point precision)
        Math.Abs((double)amount - retrievedAmount).ShouldBeLessThan(0.01);
        Math.Abs((double)rate - retrievedRate).ShouldBeLessThan(0.0001);
    }

    [Fact]
    public async Task decimal_nullable_roundtrip()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("optional_amounts");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<decimal?>("amount");
        await CreateTable(connection, table);

        // Insert null
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO optional_amounts (id, amount) VALUES (1, NULL)";
        await insertCmd.ExecuteNonQueryAsync();

        // Read back null
        var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT amount FROM optional_amounts WHERE id = 1";
        var result = await selectCmd.ExecuteScalarAsync();

        result.ShouldBe(DBNull.Value);

        // Insert with value
        var amount = 999.99m;
        insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO optional_amounts (id, amount) VALUES (2, @amount)";
        insertCmd.Parameters.AddWithValue("@amount", amount);
        await insertCmd.ExecuteNonQueryAsync();

        // Read back value
        selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT amount FROM optional_amounts WHERE id = 2";
        result = await selectCmd.ExecuteScalarAsync();

        result.ShouldNotBe(DBNull.Value);
        var retrieved = Convert.ToDouble(result);
        Math.Abs((double)amount - retrieved).ShouldBeLessThan(0.01);
    }

    [Fact]
    public async Task all_types_together()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("all_types");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.AddColumn<int>("count");
        table.AddColumn<long>("big_number");
        table.AddColumn<double>("rate");
        table.AddColumn<decimal>("amount");
        table.AddColumn<bool>("is_active");
        table.AddColumn<DateTime>("created_at");
        table.AddColumn<DateTimeOffset>("scheduled_for");
        table.AddColumn<TimeSpan>("duration");
        table.AddColumn<byte[]>("data");
        await CreateTable(connection, table);

        // Test data
        var id = Guid.NewGuid();
        var name = "Test Record";
        var count = 42;
        var bigNumber = 9876543210L;
        var rate = 0.125;
        var amount = 99.99m;
        var isActive = true;
        var createdAt = DateTime.UtcNow;
        var scheduledFor = DateTimeOffset.UtcNow.AddDays(7);
        var duration = TimeSpan.FromHours(3);
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Insert
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = @"
            INSERT INTO all_types
            (id, name, count, big_number, rate, amount, is_active, created_at, scheduled_for, duration, data)
            VALUES
            (@id, @name, @count, @big_number, @rate, @amount, @is_active, @created_at, @scheduled_for, @duration, @data)";

        insertCmd.Parameters.AddWithValue("@id", id.ToString());
        insertCmd.Parameters.AddWithValue("@name", name);
        insertCmd.Parameters.AddWithValue("@count", count);
        insertCmd.Parameters.AddWithValue("@big_number", bigNumber);
        insertCmd.Parameters.AddWithValue("@rate", rate);
        insertCmd.Parameters.AddWithValue("@amount", amount);
        insertCmd.Parameters.AddWithValue("@is_active", isActive);
        insertCmd.Parameters.AddWithValue("@created_at", createdAt.ToString("o"));
        insertCmd.Parameters.AddWithValue("@scheduled_for", scheduledFor.ToString("o"));
        insertCmd.Parameters.AddWithValue("@duration", duration.ToString());
        insertCmd.Parameters.AddWithValue("@data", data);

        await insertCmd.ExecuteNonQueryAsync();

        // Read back
        var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT * FROM all_types WHERE id = @id";
        selectCmd.Parameters.AddWithValue("@id", id.ToString());

        await using var reader = await selectCmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        // Verify all fields
        Guid.Parse(await reader.GetFieldValueAsync<string>(0)).ShouldBe(id);
        (await reader.GetFieldValueAsync<string>(1)).ShouldBe(name);
        (await reader.GetFieldValueAsync<int>(2)).ShouldBe(count);
        (await reader.GetFieldValueAsync<long>(3)).ShouldBe(bigNumber);
        (await reader.GetFieldValueAsync<double>(4)).ShouldBe(rate);
        Math.Abs((double)amount - await reader.GetFieldValueAsync<double>(5)).ShouldBeLessThan(0.01);
        (await reader.GetFieldValueAsync<bool>(6)).ShouldBe(isActive);

        var retrievedDate = DateTime.Parse(await reader.GetFieldValueAsync<string>(7));
        retrievedDate.Year.ShouldBe(createdAt.Year);
        retrievedDate.Month.ShouldBe(createdAt.Month);
        retrievedDate.Day.ShouldBe(createdAt.Day);

        var retrievedOffset = DateTimeOffset.Parse(await reader.GetFieldValueAsync<string>(8));
        retrievedOffset.Day.ShouldBe(scheduledFor.Day);

        var retrievedDuration = TimeSpan.Parse(await reader.GetFieldValueAsync<string>(9));
        retrievedDuration.ShouldBe(duration);

        var retrievedData = (byte[])await reader.GetFieldValueAsync<byte[]>(10);
        retrievedData.ShouldBe(data);
    }

    private static async Task<SqliteConnection> OpenConnectionAsync()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        return connection;
    }

    private static Task CreateEventsTable(SqliteConnection connection)
    {
        var table = new Table("events");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.AddColumn<DateTime>("created_at");

        return CreateTable(connection, table);
    }

    private static Task CreateTable(SqliteConnection connection, Table table)
    {
        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);

        return connection.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();
    }
}
