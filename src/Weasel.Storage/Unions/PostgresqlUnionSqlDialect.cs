#nullable enable
using System;
using System.Collections.Generic;
using System.Text;
using static Weasel.Storage.UnionSqlArguments;

namespace Weasel.Storage;

/// <summary>
///     <see cref="IUnionSqlDialect"/> for PostgreSQL over a <c>jsonb</c> column.
/// </summary>
/// <remarks>
///     The kind test is <c>jsonb_typeof(...)</c>; property presence is <c>(... -> 'Prop') is not null</c>,
///     which is true for a property holding JSON <c>null</c> too (that is the <c>jsonb</c> value
///     <c>'null'</c>, not SQL NULL) and false when the member is not an object. The <c>?</c> key-exists
///     operator is deliberately not used: Weasel's command builders treat <c>?</c> as a placeholder.
///     <c>jsonb_typeof</c> does not accept <c>json</c>, so the column must be <c>jsonb</c>.
/// </remarks>
public sealed class PostgresqlUnionSqlDialect: IUnionSqlDialect
{
    public static readonly PostgresqlUnionSqlDialect Instance = new();

    public string TypeTest(string jsonColumn, IReadOnlyList<string> memberPath, UnionCaseTest test)
    {
        Check(jsonColumn, memberPath);
        test.Validate();

        var member = Locate(jsonColumn, memberPath, asText: false);
        var clauses = new List<string> { $"jsonb_typeof({member}) = '{TypeName(test.Kind)}'" };

        foreach (var property in test.RequiredProperties)
        {
            clauses.Add($"({Locate(jsonColumn, With(memberPath, property), asText: false)}) is not null");
        }

        foreach (var property in test.AbsentProperties)
        {
            clauses.Add($"({Locate(jsonColumn, With(memberPath, property), asText: false)}) is null");
        }

        if (test.Discriminator is { } discriminator)
        {
            var located = Locate(jsonColumn, With(memberPath, discriminator.PropertyName), asText: true);
            clauses.Add($"({located}) = '{Sql(discriminator.Value)}'");
        }

        return And(clauses);
    }

    public string CaseAccessAsText(string jsonColumn, IReadOnlyList<string> memberPath,
        IReadOnlyList<string> caseMemberPath)
    {
        Check(jsonColumn, memberPath);
        return Locate(jsonColumn, Concat(memberPath, caseMemberPath), asText: true);
    }

    public string CaseAccessAsJson(string jsonColumn, IReadOnlyList<string> memberPath,
        IReadOnlyList<string> caseMemberPath)
    {
        Check(jsonColumn, memberPath);
        return Locate(jsonColumn, Concat(memberPath, caseMemberPath), asText: false);
    }

    private static string Locate(string jsonColumn, IReadOnlyList<string> path, bool asText)
    {
        var builder = new StringBuilder(jsonColumn);
        for (var i = 0; i < path.Count; i++)
        {
            var op = asText && i == path.Count - 1 ? " ->> " : " -> ";
            builder.Append(op).Append('\'').Append(Sql(path[i])).Append('\'');
        }

        return builder.ToString();
    }

    private static string TypeName(UnionCaseJsonKind kind) => kind switch
    {
        UnionCaseJsonKind.Null => "null",
        UnionCaseJsonKind.String => "string",
        UnionCaseJsonKind.Number => "number",
        UnionCaseJsonKind.Boolean => "boolean",
        UnionCaseJsonKind.Array => "array",
        UnionCaseJsonKind.Object => "object",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };
}
