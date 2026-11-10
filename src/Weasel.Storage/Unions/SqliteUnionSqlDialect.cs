#nullable enable
using System;
using System.Collections.Generic;
using System.Text;
using static Weasel.Storage.UnionSqlArguments;

namespace Weasel.Storage;

/// <summary>
///     <see cref="IUnionSqlDialect"/> for SQLite over a TEXT JSON column (JSON1).
/// </summary>
/// <remarks>
///     <para>
///         The kind test is <c>json_type(...)</c>, which splits numbers into <c>'integer'</c> and
///         <c>'real'</c> and booleans into <c>'true'</c> and <c>'false'</c>, so those two kinds render as
///         <c>in (...)</c>. Property presence is <c>json_type(...) is not null</c>, which is true for a
///         property holding JSON <c>null</c> (<c>json_type</c> returns <c>'null'</c>).
///     </para>
///     <para>
///         <see cref="CaseAccessAsText"/> is <c>json_extract</c>, which yields a native SQLite value for
///         a scalar and JSON text for an object or array; <see cref="CaseAccessAsJson"/> is the
///         <c>-&gt;</c> operator (SQLite 3.38+), which always yields JSON text.
///     </para>
///     <para>
///         SQLite's JSON path syntax has no escape inside a quoted key, so a property name containing
///         <c>"</c> is refused.
///     </para>
/// </remarks>
public sealed class SqliteUnionSqlDialect: IUnionSqlDialect
{
    public static readonly SqliteUnionSqlDialect Instance = new();

    public string TypeTest(string jsonColumn, IReadOnlyList<string> memberPath, UnionCaseTest test)
    {
        Check(jsonColumn, memberPath);
        test.Validate();

        var clauses = new List<string> { $"json_type({jsonColumn}, '{Path(memberPath)}') {TypeMatch(test.Kind)}" };

        foreach (var property in test.RequiredProperties)
        {
            clauses.Add($"json_type({jsonColumn}, '{Path(With(memberPath, property))}') is not null");
        }

        foreach (var property in test.AbsentProperties)
        {
            clauses.Add($"json_type({jsonColumn}, '{Path(With(memberPath, property))}') is null");
        }

        if (test.Discriminator is { } discriminator)
        {
            var path = Path(With(memberPath, discriminator.PropertyName));
            clauses.Add($"json_extract({jsonColumn}, '{path}') = '{Sql(discriminator.Value)}'");
        }

        return And(clauses);
    }

    public string CaseAccessAsText(string jsonColumn, IReadOnlyList<string> memberPath,
        IReadOnlyList<string> caseMemberPath)
    {
        Check(jsonColumn, memberPath);
        return $"json_extract({jsonColumn}, '{Path(Concat(memberPath, caseMemberPath))}')";
    }

    public string CaseAccessAsJson(string jsonColumn, IReadOnlyList<string> memberPath,
        IReadOnlyList<string> caseMemberPath)
    {
        Check(jsonColumn, memberPath);
        return $"({jsonColumn} -> '{Path(Concat(memberPath, caseMemberPath))}')";
    }

    /// <summary>The JSON path, already escaped for use inside a single-quoted SQL literal.</summary>
    private static string Path(IReadOnlyList<string> path)
    {
        var builder = new StringBuilder("$");
        foreach (var segment in path)
        {
            if (segment.Contains('"'))
            {
                throw new ArgumentException(
                    $"SQLite's JSON path cannot address the property name '{segment}': it contains a double quote.");
            }

            builder.Append(".\"").Append(Sql(segment)).Append('"');
        }

        return builder.ToString();
    }

    private static string TypeMatch(UnionCaseJsonKind kind) => kind switch
    {
        UnionCaseJsonKind.Null => "= 'null'",
        UnionCaseJsonKind.String => "= 'text'",
        UnionCaseJsonKind.Number => "in ('integer', 'real')",
        UnionCaseJsonKind.Boolean => "in ('true', 'false')",
        UnionCaseJsonKind.Array => "= 'array'",
        UnionCaseJsonKind.Object => "= 'object'",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };
}
