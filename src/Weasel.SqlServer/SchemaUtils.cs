using JasperFx.Core;
using System.Text.RegularExpressions;

namespace Weasel.SqlServer;

public static class SchemaUtils
{
    /// <summary>
    ///     Wrap a name in delimiters, escaping any embedded <c>]</c>, unless the caller has
    ///     already bracketed it themselves.
    /// </summary>
    /// <remarks>
    ///     For the call sites that have always bracketed. Ordinary names come out exactly as
    ///     they did before, so their generated DDL is byte-identical; see <see cref="QuoteName" />
    ///     for the pass-through rule the two share.
    /// </remarks>
    public static string BracketName(string name)
        => name.IsEmpty() || IsAlreadyBracketed(name) ? name : Bracket(name);

    /// <summary>
    ///     Escape a value being written into a SQL string literal, by doubling its single quotes.
    /// </summary>
    /// <remarks>
    ///     Object names reach string literals in the introspection and drift-correction queries
    ///     (<c>OBJECT_ID('...')</c>, <c>c.name = '...'</c>). Bracketing is the wrong tool there — a
    ///     literal is terminated by <c>'</c>, not by <c>]</c>.
    /// </remarks>
    public static string EscapeLiteral(string value)
        => value.IsEmpty() ? value : value.Replace("'", "''");

    /// <summary>
    ///     Bracket a name only when it is not a regular SQL Server identifier, so DDL for an
    ///     ordinary schema is unchanged.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         A name the caller has already bracketed is passed through untouched. Weasel emitted
    ///         most identifiers bare until 9.25, so bracketing the name yourself was the only way to
    ///         use one that needed delimiting; re-escaping those now would silently rename the
    ///         object — a column declared as <c>[Order Date]</c> would be created under the literal
    ///         name <c>[Order Date]</c>, brackets and all. The cost is that a name genuinely
    ///         containing its own brackets cannot be expressed, which is the rarer case by far.
    ///     </para>
    ///     <para>
    ///         The pass-through is narrow enough not to be a hole: the name has to be bracketed end
    ///         to end with every interior <c>]</c> already doubled, which is exactly what
    ///         <see cref="Bracket" /> produces. A name that merely starts with <c>[</c> and ends
    ///         with <c>]</c> does not qualify — <c>[ix] ON t(id); DROP TABLE victim; --]</c> carries
    ///         a lone <c>]</c>, so it is escaped on its own terms and the DDL it is carrying stays
    ///         inert inside a single delimited identifier.
    ///     </para>
    /// </remarks>
    public static string QuoteName(string name)
        => name.IsEmpty() || (IsRegularIdentifier(name) && !IsReservedWord(name)) || IsAlreadyBracketed(name)
            ? name
            : Bracket(name);

    /// <summary>
    ///     Strip the delimiters off a name the caller bracketed themselves, undoubling any
    ///     interior <c>]]</c>. A name that is not properly bracketed is returned as it is.
    /// </summary>
    /// <remarks>
    ///     The inverse of <see cref="Bracket" />, and the counterpart to the pass-through in
    ///     <see cref="QuoteName" />. Emitting the caller's brackets untouched is only half the
    ///     job: the database reports the bare name back, and a model still holding the bracketed
    ///     spelling never compares equal to it, so the table reported drift on every check. The
    ///     SQL Server model normalizes names through this as they arrive, so what it holds is
    ///     always the name the database will report.
    /// </remarks>
    public static string Unbracket(string name)
        => IsAlreadyBracketed(name) ? name.Substring(1, name.Length - 2).Replace("]]", "]") : name;

    /// <summary>
    ///     Delimit a name unconditionally, with no pass-through. Only for a value that is the
    ///     object's literal name and cannot have been pre-bracketed by a caller — a database name
    ///     read out of a connection string, where <c>[</c> and <c>]</c> are part of the name.
    /// </summary>
    internal static string Bracket(string name)
        => $"[{name.Replace("]", "]]")}]";

    private static bool IsAlreadyBracketed(string name)
    {
        if (name.IsEmpty() || name.Length < 2 || name[0] != '[' || name[^1] != ']')
        {
            return false;
        }

        var inner = name.Substring(1, name.Length - 2);
        return !inner.Replace("]]", "").Contains(']');
    }

    /// <summary>
    ///     A regular identifier: a letter, <c>_</c> or <c>#</c> first, then letters, digits,
    ///     <c>_</c>, <c>@</c>, <c>$</c> or <c>#</c>. "Letter" follows the Unicode standard, so a name
    ///     like <c>Grüße</c> needs no brackets and its DDL is unchanged.
    /// </summary>
    /// <remarks>
    ///     A leading <c>@</c> is excluded even though the general identifier rule allows it: in an
    ///     object name it makes the parser read a variable, so <c>create table t (@param int)</c> is
    ///     a syntax error. SQL Server's own QUOTENAME brackets it too. A leading <c>#</c> is NOT
    ///     excluded — it is accepted unbracketed for a column or an index, and bracketing does not
    ///     change its meaning anywhere (<c>[#t]</c> is still a temp table), so excluding it would
    ///     only churn DDL for no benefit.
    /// </summary>
    public static bool IsRegularIdentifier(string name)
    {
        if (name.IsEmpty())
        {
            return false;
        }

        if (!char.IsLetter(name[0]) && name[0] != '_' && name[0] != '#')
        {
            return false;
        }

        for (var i = 1; i < name.Length; i++)
        {
            var c = name[i];
            if (!char.IsLetterOrDigit(c) && c != '_' && c != '@' && c != '$' && c != '#')
            {
                return false;
            }
        }

        return true;
    }

    public static bool IsReservedWord(string name)
        => ReservedKeywords.Contains(name, StringComparer.InvariantCultureIgnoreCase);

    private static readonly string[] ReservedKeywords =
    [
        "ADD",
        "EXTERNAL",
        "PROCEDURE",
        "ALL",
        "FETCH",
        "PUBLIC",
        "ALTER",
        "FILE",
        "RAISERROR",
        "AND",
        "FILLFACTOR",
        "READ",
        "ANY",
        "FOR",
        "READTEXT",
        "AS",
        "FOREIGN",
        "RECONFIGURE",
        "ASC",
        "FREETEXT",
        "REFERENCES",
        "AUTHORIZATION",
        "FREETEXTTABLE",
        "REPLICATION",
        "BACKUP",
        "FROM",
        "RESTORE",
        "BEGIN",
        "FULL",
        "RESTRICT",
        "BETWEEN",
        "FUNCTION",
        "RETURN",
        "BREAK",
        "GOTO",
        "REVERT",
        "BROWSE",
        "GRANT",
        "REVOKE",
        "BULK",
        "GROUP",
        "RIGHT",
        "BY",
        "HAVING",
        "ROLLBACK",
        "CASCADE",
        "HOLDLOCK",
        "ROWCOUNT",
        "CASE",
        "IDENTITY",
        "ROWGUIDCOL",
        "CHECK",
        "IDENTITY_INSERT",
        "RULE",
        "CHECKPOINT",
        "IDENTITYCOL",
        "SAVE",
        "CLOSE",
        "IF",
        "SCHEMA",
        "CLUSTERED",
        "IN",
        "SECURITYAUDIT",
        "COALESCE",
        "INDEX",
        "SELECT",
        "COLLATE",
        "INNER",
        "SEMANTICKEYPHRASETABLE",
        "COLUMN",
        "INSERT",
        "SEMANTICSIMILARITYDETAILSTABLE",
        "COMMIT",
        "INTERSECT",
        "SEMANTICSIMILARITYTABLE",
        "COMPUTE",
        "INTO",
        "SESSION_USER",
        "CONSTRAINT",
        "IS",
        "SET",
        "CONTAINS",
        "JOIN",
        "SETUSER",
        "CONTAINSTABLE",
        "KEY",
        "SHUTDOWN",
        "CONTINUE",
        "KILL",
        "SOME",
        "CONVERT",
        "LEFT",
        "STATISTICS",
        "CREATE",
        "LIKE",
        "SYSTEM_USER",
        "CROSS",
        "LINENO",
        "TABLE",
        "CURRENT",
        "LOAD",
        "TABLESAMPLE",
        "CURRENT_DATE",
        "MERGE",
        "TEXTSIZE",
        "CURRENT_TIME",
        "NATIONAL",
        "THEN",
        "CURRENT_TIMESTAMP",
        "NOCHECK",
        "TO",
        "CURRENT_USER",
        "NONCLUSTERED",
        "TOP",
        "CURSOR",
        "NOT",
        "TRAN",
        "DATABASE",
        "NULL",
        "TRANSACTION",
        "DBCC",
        "NULLIF",
        "TRIGGER",
        "DEALLOCATE",
        "OF",
        "TRUNCATE",
        "DECLARE",
        "OFF",
        "TRY_CONVERT",
        "DEFAULT",
        "OFFSETS",
        "TSEQUAL",
        "DELETE",
        "ON",
        "UNION",
        "DENY",
        "OPEN",
        "UNIQUE",
        "DESC",
        "OPENDATASOURCE",
        "UNPIVOT",
        "DISK",
        "OPENQUERY",
        "UPDATE",
        "DISTINCT",
        "OPENROWSET",
        "UPDATETEXT",
        "DISTRIBUTED",
        "OPENXML",
        "USE",
        "DOUBLE",
        "OPTION",
        "USER",
        "DROP",
        "OR",
        "VALUES",
        "DUMP",
        "ORDER",
        "VARYING",
        "ELSE",
        "OUTER",
        "VIEW",
        "END",
        "OVER",
        "WAITFOR",
        "ERRLVL",
        "PERCENT",
        "WHEN",
        "ESCAPE",
        "PIVOT",
        "WHERE",
        "EXCEPT",
        "PLAN",
        "WHILE",
        "EXEC",
        "PRECISION",
        "WITH",
        "EXECUTE",
        "PRIMARY",
        "WITHIN GROUP",
        "EXISTS",
        "PRINT",
        "WRITETEXT",
        "EXIT",
        "PROC",
        "RANK"
    ];
}
