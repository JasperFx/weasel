using System.Text.Json;

namespace Weasel.Benchmarks.Support;

/// <summary>
///     A document shaped to serialize to roughly 5 KB of JSON — the size the perf survey used as
///     its reference "realistic document". Deterministically generated from a fixed seed so a
///     benchmark run is comparable to the one before it.
/// </summary>
public class BenchmarkDocument
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public DateTimeOffset CreatedAt { get; set; }
    public int Revision { get; set; }
    public bool IsActive { get; set; }
    public decimal Balance { get; set; }
    public string[] Tags { get; set; } = [];
    public Address Shipping { get; set; } = new();
    public Address Billing { get; set; } = new();
    public List<LineItem> Items { get; set; } = [];
    public Dictionary<string, string> Attributes { get; set; } = [];

    /// <summary>
    ///     Builds the reference document. The item count is tuned so <c>ToJson</c> lands within a
    ///     few hundred bytes of 5 KB; <see cref="JsonSize" /> reports what it actually produced so
    ///     the number in Results.md is measured rather than asserted.
    /// </summary>
    public static BenchmarkDocument Create(int seed = 1337)
    {
        var random = new Random(seed);

        var document = new BenchmarkDocument
        {
            Id = new Guid("f1a1f4a0-0000-4000-8000-000000000001"),
            Name = "Benchmark Order " + random.Next(1000, 9999),
            Description = word(random, 24),
            CreatedAt = new DateTimeOffset(2026, 1, 15, 9, 30, 0, TimeSpan.Zero),
            Revision = 7,
            IsActive = true,
            Balance = 12345.67m,
            Tags = Enumerable.Range(0, 8).Select(_ => word(random, 3)).ToArray(),
            Shipping = Address.Create(random),
            Billing = Address.Create(random)
        };

        for (var i = 0; i < 18; i++)
        {
            document.Items.Add(LineItem.Create(random, i));
        }

        for (var i = 0; i < 12; i++)
        {
            document.Attributes["attribute_" + i] = word(random, 4);
        }

        return document;
    }

    /// <summary>
    ///     A deep copy, so a benchmark can mutate one copy without disturbing the shared original.
    /// </summary>
    public BenchmarkDocument Copy()
    {
        return JsonSerializer.Deserialize<BenchmarkDocument>(JsonSerializer.Serialize(this))!;
    }

    public static int JsonSize(BenchmarkDocument document)
    {
        return JsonSerializer.SerializeToUtf8Bytes(document).Length;
    }

    private static string word(Random random, int syllables)
    {
        const string alphabet = "abcdefghijklmnopqrstuvwxyz";
        return string.Create(syllables * 4, random,
            static (span, r) =>
            {
                for (var i = 0; i < span.Length; i++)
                {
                    span[i] = i % 5 == 4 ? ' ' : alphabet[r.Next(alphabet.Length)];
                }
            });
    }

    public class Address
    {
        public string Line1 { get; set; } = string.Empty;
        public string Line2 { get; set; } = string.Empty;
        public string City { get; set; } = string.Empty;
        public string Region { get; set; } = string.Empty;
        public string PostalCode { get; set; } = string.Empty;
        public string Country { get; set; } = string.Empty;

        public static Address Create(Random random)
        {
            return new Address
            {
                Line1 = random.Next(100, 9999) + " " + word(random, 3),
                Line2 = "Suite " + random.Next(1, 400),
                City = word(random, 2),
                Region = "TX",
                PostalCode = random.Next(10000, 99999).ToString(),
                Country = "USA"
            };
        }
    }

    public class LineItem
    {
        public Guid Sku { get; set; }
        public string Title { get; set; } = string.Empty;
        public int Quantity { get; set; }
        public decimal UnitPrice { get; set; }
        public string[] Categories { get; set; } = [];
        public string? Note { get; set; }

        public static LineItem Create(Random random, int index)
        {
            return new LineItem
            {
                Sku = new Guid(index + 1, 0, 0, [1, 2, 3, 4, 5, 6, 7, 8]),
                Title = word(random, 5),
                Quantity = random.Next(1, 20),
                UnitPrice = Math.Round((decimal)random.NextDouble() * 500m, 2),
                Categories = Enumerable.Range(0, 3).Select(_ => word(random, 2)).ToArray(),
                Note = index % 3 == 0 ? word(random, 6) : null
            };
        }
    }
}
