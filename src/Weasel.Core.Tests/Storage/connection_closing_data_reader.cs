using System.Data;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests.StorageHelpers;

public class connection_closing_data_reader
{
    private static async Task<(SqliteConnection, ConnectionClosingDataReader)> open()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        var command = connection.CreateCommand();
        command.CommandText = "select 1 as one, 'two' as two";
        var reader = await command.ExecuteReaderAsync();
        return (connection, new ConnectionClosingDataReader(reader, connection));
    }

    [Fact]
    public async Task reads_through_to_the_inner_reader()
    {
        var (_, reader) = await open();
        await using (reader)
        {
            (await reader.ReadAsync()).ShouldBeTrue();
            reader.FieldCount.ShouldBe(2);
            reader.GetInt64(0).ShouldBe(1);
            reader.GetFieldValue<string>(reader.GetOrdinal("two")).ShouldBe("two");
            (await reader.ReadAsync()).ShouldBeFalse();
        }
    }

    [Fact]
    public async Task disposing_async_closes_the_connection()
    {
        var (connection, reader) = await open();

        await reader.DisposeAsync();

        connection.State.ShouldBe(ConnectionState.Closed);
    }

    [Fact]
    public async Task disposing_closes_the_connection()
    {
        var (connection, reader) = await open();

        reader.Dispose();

        connection.State.ShouldBe(ConnectionState.Closed);
    }

    [Fact]
    public async Task closing_closes_the_connection()
    {
        var (connection, reader) = await open();

        await reader.CloseAsync();

        reader.IsClosed.ShouldBeTrue();
        connection.State.ShouldBe(ConnectionState.Closed);
    }

    [Fact]
    public async Task unwrap_reaches_the_driver_s_reader_through_any_number_of_wrappers()
    {
        var (connection, reader) = await open();
        await using var twice = new ConnectionClosingDataReader(reader, connection);

        ConnectionClosingDataReader.Unwrap(twice).ShouldBeOfType<SqliteDataReader>();
        ConnectionClosingDataReader.Unwrap(reader.Inner).ShouldBeSameAs(reader.Inner);
    }
}
