using System.Buffers;
using System.Text;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     Covers <see cref="StorageSerializerAdapter" />, lifted out of Polecat's and Fisher's
///     byte-identical per-store copies alongside the serializer itself (weasel#555). The adapter
///     derives the <see cref="IStorageSerializer" /> seam-only members from the shared
///     <see cref="ISerializer" /> members every serializer already carries.
/// </summary>
public class storage_serializer_adapter_tests
{
    public class TestDoc
    {
        public string? Name { get; set; }
        public int Number { get; set; }
    }

    /// <summary>
    ///     The store-shaped subclass: this is the whole of what Polecat and Fisher keep locally
    ///     after adopting the lifted serializer — a namespace of their own plus the seam
    ///     declaration, satisfied entirely by inherited members.
    /// </summary>
    private sealed class StoreStyleSerializer : SystemTextJsonSerializer, IStorageSerializer;

    private readonly SystemTextJsonSerializer theInner = new();

    private IStorageSerializer theAdapter => StorageSerializerAdapter.For(theInner);

    [Fact]
    public void a_serializer_already_implementing_the_seam_is_returned_unwrapped()
    {
        var native = new StoreStyleSerializer();

        StorageSerializerAdapter.For(native).ShouldBeSameAs(native);
    }

    [Fact]
    public void a_core_only_serializer_is_wrapped()
    {
        theAdapter.ShouldBeOfType<StorageSerializerAdapter>();
    }

    [Fact]
    public void to_json_delegates_and_maps_null_to_the_json_null_literal()
    {
        theAdapter.ToJson(new TestDoc { Name = "adapted" })
            .ShouldBe(theInner.ToJson(new TestDoc { Name = "adapted" }));

        theAdapter.ToJson(null).ShouldBe("null");
    }

    [Fact]
    public void clean_json_is_plain_json()
    {
        var doc = new TestDoc { Name = "clean", Number = 2 };

        theAdapter.ToCleanJson(doc).ShouldBe(theAdapter.ToJson(doc));
    }

    [Fact]
    public void write_to_produces_the_utf8_bytes_of_the_json()
    {
        var doc = new TestDoc { Name = "buffered", Number = 4 };
        var writer = new ArrayBufferWriter<byte>();

        theAdapter.WriteTo(writer, doc);

        writer.WrittenSpan.ToArray().ShouldBe(Encoding.UTF8.GetBytes(theInner.ToJson(doc)));
    }

    [Fact]
    public void write_to_parameter_binds_json_as_a_string_and_null_as_dbnull()
    {
        var parameter = new SqliteParameter();

        theAdapter.WriteToParameter(parameter, new TestDoc { Name = "bound" });
        parameter.Value.ShouldBeOfType<string>().ShouldContain("\"name\":\"bound\"");

        theAdapter.WriteToParameter(parameter, null);
        parameter.Value.ShouldBe(DBNull.Value);
    }

    [Fact]
    public async Task the_stream_reads_delegate_to_the_inner_serializer()
    {
        var bytes = Encoding.UTF8.GetBytes(theInner.ToJson(new TestDoc { Name = "streamed", Number = 6 }));

        theAdapter.FromJson<TestDoc>(new MemoryStream(bytes)).Name.ShouldBe("streamed");
        theAdapter.FromJson(typeof(TestDoc), new MemoryStream(bytes))
            .ShouldBeOfType<TestDoc>().Number.ShouldBe(6);

        (await theAdapter.FromJsonAsync<TestDoc>(new MemoryStream(bytes))).Name.ShouldBe("streamed");
        (await theAdapter.FromJsonAsync(typeof(TestDoc), new MemoryStream(bytes)))
            .ShouldBeOfType<TestDoc>().Number.ShouldBe(6);
    }

    [Fact]
    public async Task reads_json_from_a_data_reader_column_sync_and_async()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = "select '{\"name\":\"row\",\"number\":13}'";

        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        theAdapter.FromJson<TestDoc>(reader, 0).Name.ShouldBe("row");
        theAdapter.FromJson(typeof(TestDoc), reader, 0)
            .ShouldBeOfType<TestDoc>().Number.ShouldBe(13);

        // The async reader reads route through the reader's stream rather than the
        // RUC-annotated string overloads.
        (await theAdapter.FromJsonAsync<TestDoc>(reader, 0)).Name.ShouldBe("row");
        (await theAdapter.FromJsonAsync(typeof(TestDoc), reader, 0))
            .ShouldBeOfType<TestDoc>().Number.ShouldBe(13);
    }
}
