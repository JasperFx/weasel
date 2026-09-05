using System.Buffers;
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
    public void configure_reaches_the_underlying_options()
    {
        theSerializer.Configure(o => o.WriteIndented = true);

        theSerializer.Options.WriteIndented.ShouldBeTrue();
        theSerializer.ToJson(new TestDoc { Name = "pretty" }).ShouldContain("\n");
    }
}
