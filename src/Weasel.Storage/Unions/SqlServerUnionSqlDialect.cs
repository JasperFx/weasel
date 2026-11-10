#nullable enable
using System;
using System.Collections.Generic;
using System.Text;
using static Weasel.Storage.UnionSqlArguments;

namespace Weasel.Storage;

/// <summary>
///     <see cref="IUnionSqlDialect"/> for SQL Server over a JSON column (<c>nvarchar(max)</c> or the
///     native <c>json</c> type).
/// </summary>
/// <remarks>
///     <para>
///         SQL Server has no single JSON type function, and <c>JSON_VALUE</c> cannot tell the number
///         <c>5</c> from the string <c>"5"</c>. So the kind test reads the <c>[type]</c> column
///         <c>OPENJSON</c> reports for the member within its parent (0 null, 1 string, 2 number,
///         3 boolean, 4 array, 5 object), matching the member's key with OPENJSON's binary collation.
///     </para>
///     <para>
///         Property presence and absence read <c>OPENJSON</c>'s keys too, rather than
///         <c>JSON_PATH_EXISTS</c>: that function only exists from SQL Server 2022, and the 2019 engine
///         (and Azure SQL Edge) refuse it as an unknown built-in. Everything here runs on SQL Server 2016+
///         at database compatibility level 130 or higher. A property holding JSON <c>null</c> is present.
///         <c>OPENJSON</c> yields no rows for a scalar member, so presence is false there. A
///         discriminator and <see cref="CaseAccessAsText"/> use <c>JSON_VALUE</c>, which returns
///         <c>nvarchar(4000)</c> and NULL for an object or array; <see cref="CaseAccessAsJson"/> uses
///         <c>JSON_QUERY</c>, which returns NULL for a scalar.
///     </para>
/// </remarks>
public sealed class SqlServerUnionSqlDialect: IUnionSqlDialect
{
    public static readonly SqlServerUnionSqlDialect Instance = new();

    public string TypeTest(string jsonColumn, IReadOnlyList<string> memberPath, UnionCaseTest test)
    {
        Check(jsonColumn, memberPath);
        test.Validate();

        var parent = Path(memberPath, memberPath.Count - 1);
        var key = memberPath[memberPath.Count - 1];
        var clauses = new List<string>
        {
            $"exists (select 1 from OPENJSON({jsonColumn}, N'{Sql(parent)}') where [key] = N'{Sql(key)}' and [type] = {TypeCode(test.Kind)})"
        };

        var member = Path(memberPath);
        foreach (var property in test.RequiredProperties)
        {
            UnionSqlArguments.CheckName(property, nameof(test));
            clauses.Add($"exists ({HasKey(jsonColumn, member, property)})");
        }

        foreach (var property in test.AbsentProperties)
        {
            UnionSqlArguments.CheckName(property, nameof(test));
            clauses.Add($"not exists ({HasKey(jsonColumn, member, property)})");
        }

        if (test.Discriminator is { } discriminator)
        {
            var path = Path(With(memberPath, discriminator.PropertyName));
            clauses.Add($"JSON_VALUE({jsonColumn}, N'{Sql(path)}') = N'{Sql(discriminator.Value)}'");
        }

        return And(clauses);
    }

    public string CaseAccessAsText(string jsonColumn, IReadOnlyList<string> memberPath,
        IReadOnlyList<string> caseMemberPath)
    {
        Check(jsonColumn, memberPath);
        return $"JSON_VALUE({jsonColumn}, N'{Sql(Path(Concat(memberPath, caseMemberPath)))}')";
    }

    public string CaseAccessAsJson(string jsonColumn, IReadOnlyList<string> memberPath,
        IReadOnlyList<string> caseMemberPath)
    {
        Check(jsonColumn, memberPath);
        return $"JSON_QUERY({jsonColumn}, N'{Sql(Path(Concat(memberPath, caseMemberPath)))}')";
    }

    private static string HasKey(string jsonColumn, string path, string key)
        => $"select 1 from OPENJSON({jsonColumn}, N'{Sql(path)}') where [key] = N'{Sql(key)}'";

    private static string Path(IReadOnlyList<string> path) => Path(path, path.Count);

    private static string Path(IReadOnlyList<string> path, int count)
    {
        var builder = new StringBuilder("$");
        for (var i = 0; i < count; i++)
        {
            builder.Append('.').Append(QuotedKey(path[i]));
        }

        return builder.ToString();
    }

    private static int TypeCode(UnionCaseJsonKind kind) => kind switch
    {
        UnionCaseJsonKind.Null => 0,
        UnionCaseJsonKind.String => 1,
        UnionCaseJsonKind.Number => 2,
        UnionCaseJsonKind.Boolean => 3,
        UnionCaseJsonKind.Array => 4,
        UnionCaseJsonKind.Object => 5,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };
}
