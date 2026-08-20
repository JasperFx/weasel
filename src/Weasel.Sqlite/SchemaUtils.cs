using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Sqlite;

/// <summary>
///     SQLite's identifier rules. Everything that is not dialect-specific lives in
///     <see cref="IdentifierRules" />; what stays here is the double-quote delimiter, SQLite's
///     regular-identifier rule, and its keyword list.
/// </summary>
public sealed class SqliteIdentifierRules: IdentifierRules
{
    public static readonly SqliteIdentifierRules Instance = new();

    protected override char Open => '"';
    protected override char Close => '"';

    /// <summary>
    ///     A regular identifier: a letter or <c>_</c> first, then letters, digits, <c>_</c> or
    ///     <c>$</c>.
    /// </summary>
    /// <remarks>
    ///     This replaces a hand-written list — the old rule quoted only for a reserved keyword, a
    ///     space, a hyphen or a leading digit, so a name containing anything else (a dot, a
    ///     parenthesis, or a double quote itself) went out bare. The double-quote case was the sharp
    ///     one: the escaping was written and correct, but the name never reached it.
    /// </remarks>
    public override bool IsRegularIdentifier(string name)
    {
        if (name.IsEmpty())
        {
            return false;
        }

        if (!char.IsLetter(name[0]) && name[0] != '_')
        {
            return false;
        }

        return name.Skip(1).All(c => char.IsLetterOrDigit(c) || c == '_' || c == '$');
    }

    public override bool IsReservedWord(string name)
        => ReservedKeywords.Contains(name, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// SQLite reserved keywords that should be quoted when used as identifiers.
    /// Based on https://www.sqlite.org/lang_keywords.html
    /// </summary>
    private static readonly string[] ReservedKeywords =
    [
        "ABORT",
        "ACTION",
        "ADD",
        "AFTER",
        "ALL",
        "ALTER",
        "ALWAYS",
        "ANALYZE",
        "AND",
        "AS",
        "ASC",
        "ATTACH",
        "AUTOINCREMENT",
        "BEFORE",
        "BEGIN",
        "BETWEEN",
        "BY",
        "CASCADE",
        "CASE",
        "CAST",
        "CHECK",
        "COLLATE",
        "COLUMN",
        "COMMIT",
        "CONFLICT",
        "CONSTRAINT",
        "CREATE",
        "CROSS",
        "CURRENT",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "DATABASE",
        "DEFAULT",
        "DEFERRABLE",
        "DEFERRED",
        "DELETE",
        "DESC",
        "DETACH",
        "DISTINCT",
        "DO",
        "DROP",
        "EACH",
        "ELSE",
        "END",
        "ESCAPE",
        "EXCEPT",
        "EXCLUDE",
        "EXCLUSIVE",
        "EXISTS",
        "EXPLAIN",
        "FAIL",
        "FILTER",
        "FIRST",
        "FOLLOWING",
        "FOR",
        "FOREIGN",
        "FROM",
        "FULL",
        "GENERATED",
        "GLOB",
        "GROUP",
        "GROUPS",
        "HAVING",
        "IF",
        "IGNORE",
        "IMMEDIATE",
        "IN",
        "INDEX",
        "INDEXED",
        "INITIALLY",
        "INNER",
        "INSERT",
        "INSTEAD",
        "INTERSECT",
        "INTO",
        "IS",
        "ISNULL",
        "JOIN",
        "KEY",
        "LAST",
        "LEFT",
        "LIKE",
        "LIMIT",
        "MATCH",
        "MATERIALIZED",
        "NATURAL",
        "NO",
        "NOT",
        "NOTHING",
        "NOTNULL",
        "NULL",
        "NULLS",
        "OF",
        "OFFSET",
        "ON",
        "OR",
        "ORDER",
        "OTHERS",
        "OUTER",
        "OVER",
        "PARTITION",
        "PLAN",
        "PRAGMA",
        "PRECEDING",
        "PRIMARY",
        "QUERY",
        "RAISE",
        "RANGE",
        "RECURSIVE",
        "REFERENCES",
        "REGEXP",
        "REINDEX",
        "RELEASE",
        "RENAME",
        "REPLACE",
        "RESTRICT",
        "RETURNING",
        "RIGHT",
        "ROLLBACK",
        "ROW",
        "ROWS",
        "SAVEPOINT",
        "SELECT",
        "SET",
        "TABLE",
        "TEMP",
        "TEMPORARY",
        "THEN",
        "TIES",
        "TO",
        "TRANSACTION",
        "TRIGGER",
        "UNBOUNDED",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "USING",
        "VACUUM",
        "VALUES",
        "VIEW",
        "VIRTUAL",
        "WHEN",
        "WHERE",
        "WINDOW",
        "WITH",
        "WITHOUT"
    ];
}

/// <summary>
///     The static facade the SQLite DDL writers call. Delegates to
///     <see cref="SqliteIdentifierRules" />.
/// </summary>
public static class SchemaUtils
{
    /// <inheritdoc cref="IdentifierRules.Quote" />
    public static string QuoteName(string name) => SqliteIdentifierRules.Instance.Quote(name);

    /// <inheritdoc cref="IdentifierRules.Undelimit" />
    public static string Unquote(string name) => SqliteIdentifierRules.Instance.Undelimit(name);

    /// <inheritdoc cref="IdentifierRules.EscapeLiteral" />
    public static string EscapeLiteral(string value) => IdentifierRules.EscapeLiteral(value);

    public static bool IsReservedKeyword(string name) => SqliteIdentifierRules.Instance.IsReservedWord(name);
}
