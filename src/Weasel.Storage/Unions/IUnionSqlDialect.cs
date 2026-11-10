#nullable enable
using System;
using System.Collections.Generic;
using Weasel.Core;
using Weasel.Core.SqlGeneration;

namespace Weasel.Storage;

/// <summary>
///     Renders the SQL a store needs to query a document member typed as a C# union (weasel#744):
///     a <b>type test</b> (<c>x.Payment is Card</c>) and <b>case access</b> (a member of the active
///     case, <c>((Card)x.Payment).Last4</c>).
/// </summary>
/// <remarks>
///     <para>
///         System.Text.Json writes the bare active case of a union: no wrapper object and no
///         discriminator (jasperfx#1037). So a case member lives at the plain path
///         <c>member.caseMember</c>, and a case is recognised only by what its JSON looks like —
///         see <see cref="UnionCaseTest"/>. Event tables do not need any of this: the event type
///         column names the case once Append unwraps the union.
///     </para>
///     <para>
///         Every method takes the JSON column as a SQL expression (<c>d.data</c>) and the location
///         inside it as path segments, each a property name exactly as serialized. Path segments and
///         property names are rendered as escaped SQL literals, never as parameters, because they are
///         type metadata rather than user input. A string discriminator value is rendered the same way.
///     </para>
///     <para>
///         The dialects render no <c>?</c> of their own, which Weasel's command builders treat as a
///         parameter placeholder.
///     </para>
///     <para>
///         Shipped implementations: <see cref="PostgresqlUnionSqlDialect"/> (<c>jsonb</c>),
///         <see cref="SqlServerUnionSqlDialect"/> (<c>OPENJSON</c>, <c>JSON_VALUE</c>,
///         <c>JSON_QUERY</c>) and <see cref="SqliteUnionSqlDialect"/> (<c>json_type</c>,
///         <c>json_extract</c>).
///     </para>
/// </remarks>
public interface IUnionSqlDialect
{
    /// <summary>
    ///     A boolean SQL predicate that is true when the union stored at <paramref name="memberPath"/>
    ///     holds the case <paramref name="test"/> describes. It is false (or NULL) when the member is
    ///     missing.
    /// </summary>
    /// <param name="jsonColumn">The JSON column as a SQL expression, e.g. <c>d.data</c>.</param>
    /// <param name="memberPath">The union member's path from the document root; at least one segment.</param>
    /// <param name="test">How to recognise the case.</param>
    string TypeTest(string jsonColumn, IReadOnlyList<string> memberPath, UnionCaseTest test);

    /// <summary>
    ///     A SQL expression for a scalar member of a case: <paramref name="memberPath"/> followed by
    ///     <paramref name="caseMemberPath"/>. It does not test the case — combine it with
    ///     <see cref="TypeTest"/> when the case matters. What it yields is dialect-specific: text on
    ///     PostgreSQL, <c>nvarchar(4000)</c> on SQL Server, a native SQLite value on SQLite. An object
    ///     or array yields NULL on PostgreSQL's text form and SQL Server; use
    ///     <see cref="CaseAccessAsJson"/> for those.
    /// </summary>
    string CaseAccessAsText(string jsonColumn, IReadOnlyList<string> memberPath,
        IReadOnlyList<string> caseMemberPath);

    /// <summary>
    ///     A SQL expression for a case member as JSON (<c>jsonb</c> on PostgreSQL, JSON text
    ///     elsewhere). On SQL Server it yields NULL for a scalar, because <c>JSON_QUERY</c> only
    ///     returns objects and arrays.
    /// </summary>
    string CaseAccessAsJson(string jsonColumn, IReadOnlyList<string> memberPath,
        IReadOnlyList<string> caseMemberPath);

    /// <summary><see cref="TypeTest"/> as an <see cref="ISqlFragment"/>, for a where clause.</summary>
    ISqlFragment TypeTestFragment(string jsonColumn, IReadOnlyList<string> memberPath, UnionCaseTest test)
        => new UnionSqlFragment(TypeTest(jsonColumn, memberPath, test));
}

/// <summary>A fixed piece of SQL rendered by an <see cref="IUnionSqlDialect"/>; it carries no parameters.</summary>
public sealed class UnionSqlFragment: ISqlFragment
{
    public UnionSqlFragment(string sql)
    {
        Sql = sql;
    }

    public string Sql { get; }

    public void Apply(ICommandBuilder builder) => builder.Append(Sql);

    public override string ToString() => Sql;
}

internal static class UnionSqlArguments
{
    public static void Check(string jsonColumn, IReadOnlyList<string> memberPath)
    {
        if (string.IsNullOrWhiteSpace(jsonColumn))
        {
            throw new ArgumentException("The JSON column expression is required.", nameof(jsonColumn));
        }

        if (memberPath == null || memberPath.Count == 0)
        {
            throw new ArgumentException("A union member path needs at least one segment.", nameof(memberPath));
        }

        foreach (var segment in memberPath)
        {
            CheckName(segment, nameof(memberPath));
        }
    }

    public static void CheckName(string name, string argument)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("A JSON property name cannot be null or empty.", argument);
        }
    }

    public static List<string> Concat(IReadOnlyList<string> first, IReadOnlyList<string> second)
    {
        var list = new List<string>(first.Count + second.Count);
        list.AddRange(first);
        foreach (var segment in second)
        {
            CheckName(segment, "caseMemberPath");
            list.Add(segment);
        }

        return list;
    }

    public static List<string> With(IReadOnlyList<string> path, string last)
    {
        CheckName(last, "property");
        var list = new List<string>(path.Count + 1);
        list.AddRange(path);
        list.Add(last);
        return list;
    }

    /// <summary>Joins clauses with <c>and</c>, parenthesising when there is more than one.</summary>
    public static string And(List<string> clauses)
        => clauses.Count == 1 ? clauses[0] : "(" + string.Join(" and ", clauses) + ")";

    /// <summary>A SQL string literal body: single quotes doubled.</summary>
    public static string Sql(string value) => value.Replace("'", "''");

    /// <summary>A double-quoted JSON path key, escaped per the SQL/JSON path grammar.</summary>
    public static string QuotedKey(string name) => "\"" + name.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
}
