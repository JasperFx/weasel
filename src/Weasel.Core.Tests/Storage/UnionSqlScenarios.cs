using Xunit;
using System.Text.Json;
using Weasel.Storage;

namespace Weasel.Core.Tests.StorageHelpers;

/// <summary>
///     The rows and expectations every engine runs <see cref="IUnionSqlDialect"/> against (weasel#744).
///     Linked into Weasel.Postgresql.Tests and Weasel.SqlServer.Tests, so all three engines judge the
///     same JSON. The rows are what System.Text.Json writes for a union member: the bare active case,
///     no wrapper, no discriminator (jasperfx#1037) — except row 10, whose case type writes its own.
/// </summary>
public static class UnionSqlScenarios
{
    public static readonly (int Id, string Json)[] Rows =
    [
        (1, """{"Payment": 5}"""),
        (2, """{"Payment": "5"}"""),
        (3, """{"Payment": true}"""),
        (4, """{"Payment": null}"""),
        (5, """{"Payment": [1, 2]}"""),
        // AccountEvent(Deposited(Amount), Withdrawn(Amount, Reason)): one shape contains the other
        (6, """{"Payment": {"Amount": 10}}"""),
        (7, """{"Payment": {"Amount": 20, "Reason": "rent"}}"""),
        (8, """{"Payment": {"Amount": 30, "Reason": null}}"""),
        // a disjoint case
        (9, """{"Payment": {"Owner": "o'brien"}}"""),
        // a case type that writes its own discriminator
        (10, """{"Payment": {"$kind": "card", "Last4": "1234"}}"""),
        // the member is missing altogether
        (11, """{}"""),
        (12, """{"Payment": 2.5}"""),
        (13, """{"Payment": false}"""),
        (14, """{"Outer": {"Payment": {"Amount": 1}}}"""),
        (15, """{"we'ird key": {"Amount": 1}}"""),
        // a scalar that happens to equal a property name must not read as that property
        (16, """{"Payment": "Amount"}""")
    ];

    private static readonly string[] Payment = ["Payment"];

    private static readonly UnionCaseTest Deposited = UnionCaseTest.ObjectWith(["Amount"], ["Reason"]);

    public static readonly Dictionary<string, (string[] Path, UnionCaseTest Test, int[] Expected)> TypeTests =
        new()
        {
            ["number"] = (Payment, UnionCaseTest.OfKind(UnionCaseJsonKind.Number), [1, 12]),
            ["string"] = (Payment, UnionCaseTest.OfKind(UnionCaseJsonKind.String), [2, 16]),
            ["boolean"] = (Payment, UnionCaseTest.OfKind(UnionCaseJsonKind.Boolean), [3, 13]),
            ["null"] = (Payment, UnionCaseTest.OfKind(UnionCaseJsonKind.Null), [4]),
            ["array"] = (Payment, UnionCaseTest.OfKind(UnionCaseJsonKind.Array), [5]),
            ["object"] = (Payment, UnionCaseTest.OfKind(UnionCaseJsonKind.Object), [6, 7, 8, 9, 10]),
            ["deposited, by absence"] = (Payment, Deposited, [6]),
            ["withdrawn, null-valued property still present"] =
                (Payment, UnionCaseTest.ObjectWith(["Amount", "Reason"]), [7, 8]),
            ["disjoint case"] = (Payment, UnionCaseTest.ObjectWith(["Owner"]), [9]),
            ["discriminator"] = (Payment, UnionCaseTest.ObjectWithDiscriminator("$kind", "card"), [10]),
            ["nested member"] = (["Outer", "Payment"], Deposited, [14]),
            ["quoted member name"] = (["we'ird key"], Deposited, [15])
        };

    public static TheoryData<string> TypeTestNames()
    {
        var data = new TheoryData<string>();
        foreach (var name in TypeTests.Keys)
        {
            data.Add(name);
        }

        return data;
    }

    /// <summary>Case access as text: row, case member, expected text.</summary>
    public static readonly (int Id, string Member, string? Expected)[] TextAccess =
    [
        (7, "Reason", "rent"),
        (8, "Reason", null),
        (9, "Owner", "o'brien"),
        (6, "Amount", "10"),
        (6, "Reason", null),
        (10, "Last4", "1234")
    ];

    public static string[] PaymentPath => Payment;

    /// <summary>Normalises JSON text so engines' differing whitespace and key order compare equal.</summary>
    public static string NormalizeJson(string json)
    {
        using var document = JsonDocument.Parse(json);
        return JsonSerializer.Serialize(document.RootElement);
    }
}
