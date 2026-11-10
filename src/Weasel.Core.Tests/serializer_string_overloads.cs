using System.Data.Common;
using System.Text.Json;
using Shouldly;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     weasel#742: <c>FromJson(string)</c> moved from the stores' own serializer interfaces onto the
///     shared <see cref="ISerializer" />. It is default-implemented, because a serializer compiled
///     against the interface before the member existed must still load (the weasel#682 shape).
/// </summary>
public class serializer_string_overloads
{
    public class Doc
    {
        public string? Name { get; set; }
        public int Number { get; set; }
    }

    [Fact]
    public void the_shipped_serializer_answers_the_string_overloads_through_the_interface()
    {
        ISerializer serializer = new SystemTextJsonSerializer();
        var json = serializer.ToJson(new Doc { Name = "weasel", Number = 3 });

        serializer.FromJson<Doc>(json).Name.ShouldBe("weasel");
        serializer.FromJson(typeof(Doc), json).ShouldBeOfType<Doc>().Number.ShouldBe(3);
    }

    [Fact]
    public void a_serializer_without_string_overloads_falls_back_to_its_stream_overloads()
    {
        var serializer = new StreamOnlySerializer();
        ISerializer contract = serializer;

        contract.FromJson<Doc>("{\"Name\":\"stream\",\"Number\":7}").Name.ShouldBe("stream");
        contract.FromJson(typeof(Doc), "{\"Name\":\"ünïcode\",\"Number\":8}")
            .ShouldBeOfType<Doc>().Name.ShouldBe("ünïcode");

        serializer.StreamReads.ShouldBe(2);
    }

    [Fact]
    public void the_default_refuses_a_null_string()
    {
        ISerializer contract = new StreamOnlySerializer();

        Should.Throw<ArgumentNullException>(() => contract.FromJson<Doc>((string)null!));
        Should.Throw<ArgumentNullException>(() => contract.FromJson(typeof(Doc), (string)null!));
    }

    /// <summary>
    ///     An implementer written before weasel#742: no string overloads at all. It compiling is
    ///     half the test.
    /// </summary>
    private sealed class StreamOnlySerializer: ISerializer
    {
        public int StreamReads { get; private set; }
        public EnumStorage EnumStorage => EnumStorage.AsInteger;
        public Casing Casing => Casing.Default;
        public string ToJson(object document) => JsonSerializer.Serialize(document);

        public T FromJson<T>(Stream stream)
        {
            StreamReads++;
            return JsonSerializer.Deserialize<T>(stream)!;
        }

        public object FromJson(Type type, Stream stream)
        {
            StreamReads++;
            return JsonSerializer.Deserialize(stream, type)!;
        }

        public T FromJson<T>(DbDataReader reader, int index) => throw new NotSupportedException();
        public object FromJson(Type type, DbDataReader reader, int index) => throw new NotSupportedException();

        public ValueTask<T> FromJsonAsync<T>(Stream stream, CancellationToken cancellationToken = default)
            => new(FromJson<T>(stream));

        public ValueTask<object> FromJsonAsync(Type type, Stream stream, CancellationToken cancellationToken = default)
            => new(FromJson(type, stream));
    }
}
