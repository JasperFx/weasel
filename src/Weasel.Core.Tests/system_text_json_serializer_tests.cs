using System.Buffers;
using System.Data.Common;
using System.Text;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     Covers the shared System.Text.Json serializer lifted out of Polecat's and Fisher's
///     byte-identical per-store copies (weasel#555). Pure relocation, so these tests pin the
///     behavior the stores already rely on: defaults, casing/enum-storage/non-public-member
///     options, and the DbDataReader/DbParameter members of the storage seam.
/// </summary>
public class system_text_json_serializer_tests
{
    public class TestDoc
    {
        public string? Name { get; set; }
        public int Number { get; set; }
        public string? MaybeNull { get; set; }
        public TestColor Color { get; set; }
    }

    public enum TestColor
    {
        DarkBlue,
        LightGreen
    }

    public class SnakeDoc
    {
        public string? FirstName { get; set; }
    }

    public class GuardedDoc
    {
        public GuardedDoc()
        {
        }

        public GuardedDoc(string name)
        {
            Name = name;
        }

        public string? Name { get; private set; }
    }

    private readonly SystemTextJsonSerializer theSerializer = new();

    [Fact]
    public void a_null_options_argument_is_refused()
    {
        Should.Throw<ArgumentNullException>(() => new SystemTextJsonSerializer(null!));
    }

    [Fact]
    public void default_configuration_is_camel_case_integer_enums_null_ignoring()
    {
        theSerializer.Casing.ShouldBe(Casing.CamelCase);
        theSerializer.EnumStorage.ShouldBe(EnumStorage.AsInteger);
        theSerializer.CollectionStorage.ShouldBe(CollectionStorage.Default);
        theSerializer.NonPublicMembersStorage.ShouldBe(NonPublicMembersStorage.Default);

        var json = theSerializer.ToJson(new TestDoc { Name = "weasel", Number = 3, Color = TestColor.LightGreen });

        json.ShouldBe("{\"name\":\"weasel\",\"number\":3,\"color\":1}");
    }

    [Fact]
    public void round_trips_through_the_string_overloads()
    {
        var doc = new TestDoc { Name = "weasel", Number = 42, Color = TestColor.DarkBlue };

        var typed = theSerializer.FromJson<TestDoc>(theSerializer.ToJson(doc));
        typed.Name.ShouldBe("weasel");
        typed.Number.ShouldBe(42);
        typed.Color.ShouldBe(TestColor.DarkBlue);

        var untyped = theSerializer.FromJson(typeof(TestDoc), theSerializer.ToJson(doc))
            .ShouldBeOfType<TestDoc>();
        untyped.Name.ShouldBe("weasel");
        untyped.Number.ShouldBe(42);
    }

    [Fact]
    public async Task round_trips_through_the_stream_overloads()
    {
        var doc = new TestDoc { Name = "streamed", Number = 7 };
        var bytes = Encoding.UTF8.GetBytes(theSerializer.ToJson(doc));

        theSerializer.FromJson<TestDoc>(new MemoryStream(bytes)).Name.ShouldBe("streamed");
        theSerializer.FromJson(typeof(TestDoc), new MemoryStream(bytes))
            .ShouldBeOfType<TestDoc>().Name.ShouldBe("streamed");

        (await theSerializer.FromJsonAsync<TestDoc>(new MemoryStream(bytes))).Name.ShouldBe("streamed");
        (await theSerializer.FromJsonAsync(typeof(TestDoc), new MemoryStream(bytes)))
            .ShouldBeOfType<TestDoc>().Name.ShouldBe("streamed");
    }

    [Fact]
    public void serializing_null_yields_the_json_null_literal()
    {
        theSerializer.ToJson(null).ShouldBe("null");
    }

    [Fact]
    public void serializes_by_runtime_type_rather_than_declared_type()
    {
        object doc = new TestDoc { Name = "runtime" };

        theSerializer.ToJson(doc).ShouldContain("\"name\":\"runtime\"");
    }

    [Fact]
    public void to_clean_json_is_identical_to_to_json()
    {
        var doc = new TestDoc { Name = "clean", Number = 9 };

        theSerializer.ToCleanJson(doc).ShouldBe(theSerializer.ToJson(doc));
    }

    [Fact]
    public void write_to_produces_the_same_bytes_as_to_json()
    {
        var doc = new TestDoc { Name = "buffered", Number = 5, Color = TestColor.LightGreen };
        var writer = new ArrayBufferWriter<byte>();

        theSerializer.WriteTo(writer, doc);

        writer.WrittenSpan.ToArray().ShouldBe(Encoding.UTF8.GetBytes(theSerializer.ToJson(doc)));
    }

    [Fact]
    public void write_to_parameter_binds_json_as_a_string_and_null_as_dbnull()
    {
        var parameter = new SqliteParameter();

        theSerializer.WriteToParameter(parameter, new TestDoc { Name = "bound" });
        parameter.Value.ShouldBeOfType<string>().ShouldContain("\"name\":\"bound\"");

        theSerializer.WriteToParameter(parameter, null);
        parameter.Value.ShouldBe(DBNull.Value);
    }

    [Fact]
    public void snake_casing_is_applied_when_selected()
    {
        theSerializer.Casing = Casing.SnakeCase;

        theSerializer.ToJson(new SnakeDoc { FirstName = "wendell" })
            .ShouldBe("{\"first_name\":\"wendell\"}");

        theSerializer.FromJson<SnakeDoc>("{\"first_name\":\"wendell\"}")
            .FirstName.ShouldBe("wendell");
    }

    [Fact]
    public void default_casing_keeps_the_member_names_as_declared()
    {
        theSerializer.Casing = Casing.Default;

        theSerializer.ToJson(new SnakeDoc { FirstName = "exact" })
            .ShouldBe("{\"FirstName\":\"exact\"}");
    }

    [Fact]
    public void enums_as_strings_respect_the_naming_policy_and_round_trip()
    {
        theSerializer.EnumStorage = EnumStorage.AsString;

        var json = theSerializer.ToJson(new TestDoc { Name = "e", Color = TestColor.DarkBlue });
        json.ShouldContain("\"color\":\"darkBlue\"");

        theSerializer.FromJson<TestDoc>(json).Color.ShouldBe(TestColor.DarkBlue);
    }

    [Fact]
    public void switching_enum_storage_back_to_integers_removes_the_converter()
    {
        theSerializer.EnumStorage = EnumStorage.AsString;
        theSerializer.EnumStorage = EnumStorage.AsInteger;

        theSerializer.ToJson(new TestDoc { Name = "e", Color = TestColor.LightGreen })
            .ShouldContain("\"color\":1");
    }

    [Fact]
    public void non_public_setters_are_ignored_by_default()
    {
        var json = theSerializer.ToJson(new GuardedDoc("hidden"));
        json.ShouldContain("\"name\":\"hidden\"");

        theSerializer.FromJson<GuardedDoc>(json).Name.ShouldBeNull();
    }

    [Fact]
    public void non_public_setters_are_used_when_opted_into()
    {
        theSerializer.NonPublicMembersStorage = NonPublicMembersStorage.NonPublicSetters;

        var json = theSerializer.ToJson(new GuardedDoc("restored"));

        theSerializer.FromJson<GuardedDoc>(json).Name.ShouldBe("restored");
    }

    [Fact]
    public async Task reads_json_from_a_data_reader_column()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = "select '{\"name\":\"row\",\"number\":11}'";

        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        theSerializer.FromJson<TestDoc>(reader, 0).Name.ShouldBe("row");
        theSerializer.FromJson(typeof(TestDoc), reader, 0)
            .ShouldBeOfType<TestDoc>().Number.ShouldBe(11);

        (await theSerializer.FromJsonAsync<TestDoc>(reader, 0)).Name.ShouldBe("row");
        (await theSerializer.FromJsonAsync(typeof(TestDoc), reader, 0))
            .ShouldBeOfType<TestDoc>().Number.ShouldBe(11);
    }

    [Fact]
    public async Task reads_a_reader_column_the_same_way_whether_or_not_the_provider_streams()
    {
        // Microsoft.Data.Sqlite streams a TEXT column, so this exercises the stream path; the
        // NonStreamingReader case below covers the fallback. Both must produce the same document.
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = "select '{\"name\":\"streamed\",\"number\":42,\"color\":1}'";

        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        var streamed = theSerializer.FromJson<TestDoc>(reader, 0);
        streamed.Name.ShouldBe("streamed");
        streamed.Number.ShouldBe(42);
        streamed.Color.ShouldBe(TestColor.LightGreen);

        await using var fallback = new NonStreamingReader(reader);
        var viaString = theSerializer.FromJson<TestDoc>(fallback, 0);
        viaString.Name.ShouldBe("streamed");
        viaString.Number.ShouldBe(42);
        viaString.Color.ShouldBe(TestColor.LightGreen);
    }

    [Fact]
    public async Task falls_back_to_the_string_path_when_a_provider_refuses_to_stream()
    {
        // SqlDataReader throws InvalidCastException from GetStream on nvarchar(max) and on SQL
        // Server 2025's native json type, so the read path has to survive a provider that refuses
        // (weasel#573). NonStreamingReader reproduces that refusal without needing a container.
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = "select '{\"name\":\"fell back\",\"number\":7}'";

        await using var inner = await command.ExecuteReaderAsync();
        (await inner.ReadAsync()).ShouldBeTrue();

        await using var reader = new NonStreamingReader(inner);

        theSerializer.FromJson<TestDoc>(reader, 0).Name.ShouldBe("fell back");
        theSerializer.FromJson(typeof(TestDoc), reader, 0)
            .ShouldBeOfType<TestDoc>().Number.ShouldBe(7);

        (await theSerializer.FromJsonAsync<TestDoc>(reader, 0)).Name.ShouldBe("fell back");
        (await theSerializer.FromJsonAsync(typeof(TestDoc), reader, 0))
            .ShouldBeOfType<TestDoc>().Number.ShouldBe(7);

        reader.StreamAttempts.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void write_to_reuses_its_json_writer_across_calls()
    {
        // The writer is pooled per thread and reset onto each call's buffer, so two calls must not
        // bleed into one another.
        var first = new ArrayBufferWriter<byte>();
        var second = new ArrayBufferWriter<byte>();

        theSerializer.WriteTo(first, new TestDoc { Name = "one", Number = 1 });
        theSerializer.WriteTo(second, new TestDoc { Name = "two", Number = 2 });

        Encoding.UTF8.GetString(first.WrittenSpan).ShouldBe("{\"name\":\"one\",\"number\":1,\"color\":0}");
        Encoding.UTF8.GetString(second.WrittenSpan).ShouldBe("{\"name\":\"two\",\"number\":2,\"color\":0}");
    }

    [Fact]
    public async Task write_to_is_safe_from_two_threads_at_once()
    {
        // A store's serializer is a singleton, so the pooled writer is thread-static. Two threads
        // writing at the same time must each get intact JSON.
        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(i => Task.Run(() =>
        {
            var buffer = new ArrayBufferWriter<byte>();
            theSerializer.WriteTo(buffer, new TestDoc { Name = "doc" + i, Number = i });
            return Encoding.UTF8.GetString(buffer.WrittenSpan);
        })));

        for (var i = 0; i < results.Length; i++)
        {
            results.ShouldContain("{\"name\":\"doc" + i + "\",\"number\":" + i + ",\"color\":0}");
        }
    }

    /// <summary>
    ///     A reader that refuses to stream, the way Microsoft.Data.SqlClient refuses on an
    ///     <c>nvarchar(max)</c> or <c>json</c> column, while delegating everything else to a real
    ///     reader. Lets the fallback be tested without a SQL Server container.
    /// </summary>
    private sealed class NonStreamingReader(DbDataReader inner): DbDataReader
    {
        public int StreamAttempts { get; private set; }

        public override Stream GetStream(int ordinal)
        {
            StreamAttempts++;
            throw new InvalidCastException(
                $"Invalid attempt to GetStream on column '{GetName(ordinal)}'. The GetStream function can only be used on columns of type Binary, Image, Udt or VarBinary.");
        }

        public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
        {
            if (typeof(T) == typeof(Stream))
            {
                StreamAttempts++;
                throw new InvalidCastException("Invalid attempt to GetStream.");
            }

            return inner.GetFieldValueAsync<T>(ordinal, cancellationToken);
        }

        public override string GetString(int ordinal) => inner.GetString(ordinal);
        public override object GetValue(int ordinal) => inner.GetValue(ordinal);
        public override int GetValues(object[] values) => inner.GetValues(values);
        public override bool IsDBNull(int ordinal) => inner.IsDBNull(ordinal);
        public override int FieldCount => inner.FieldCount;
        public override object this[int ordinal] => inner[ordinal];
        public override object this[string name] => inner[name];
        public override int RecordsAffected => inner.RecordsAffected;
        public override bool HasRows => inner.HasRows;
        public override bool IsClosed => inner.IsClosed;
        public override int Depth => inner.Depth;
        public override bool NextResult() => inner.NextResult();
        public override bool Read() => inner.Read();
        public override System.Collections.IEnumerator GetEnumerator() => inner.GetEnumerator();
        public override bool GetBoolean(int ordinal) => inner.GetBoolean(ordinal);
        public override byte GetByte(int ordinal) => inner.GetByte(ordinal);
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) =>
            inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        public override char GetChar(int ordinal) => inner.GetChar(ordinal);
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) =>
            inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        public override string GetDataTypeName(int ordinal) => inner.GetDataTypeName(ordinal);
        public override DateTime GetDateTime(int ordinal) => inner.GetDateTime(ordinal);
        public override decimal GetDecimal(int ordinal) => inner.GetDecimal(ordinal);
        public override double GetDouble(int ordinal) => inner.GetDouble(ordinal);
        public override Type GetFieldType(int ordinal) => inner.GetFieldType(ordinal);
        public override float GetFloat(int ordinal) => inner.GetFloat(ordinal);
        public override Guid GetGuid(int ordinal) => inner.GetGuid(ordinal);
        public override short GetInt16(int ordinal) => inner.GetInt16(ordinal);
        public override int GetInt32(int ordinal) => inner.GetInt32(ordinal);
        public override long GetInt64(int ordinal) => inner.GetInt64(ordinal);
        public override string GetName(int ordinal) => inner.GetName(ordinal);
        public override int GetOrdinal(string name) => inner.GetOrdinal(name);
    }

    [Fact]
    public void configure_reaches_the_underlying_options()
    {
        theSerializer.Configure(o => o.WriteIndented = true);

        theSerializer.Options.WriteIndented.ShouldBeTrue();
        theSerializer.ToJson(new TestDoc { Name = "pretty" }).ShouldContain("\n");
    }
}
