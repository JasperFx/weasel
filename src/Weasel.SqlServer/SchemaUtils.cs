using JasperFx.Core;
using System.Text.RegularExpressions;

namespace Weasel.SqlServer;

public static class SchemaUtils
{
    /// <summary>
    ///     Wrap a name in brackets unconditionally, escaping any embedded <c>]</c>.
    /// </summary>
    /// <remarks>
    ///     For the call sites that have always bracketed. Keeping them on this rather than
    ///     <see cref="QuoteName" /> means their generated DDL is byte-identical to before.
    /// </remarks>
    public static string BracketName(string name)
        => name.IsEmpty() ? name : $"[{name.Replace("]", "]]")}]";

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
    ///         There is deliberately no "it already looks bracketed, leave it alone" shortcut. A name
    ///         that merely starts with <c>[</c> and ends with <c>]</c> is not necessarily bracketed —
    ///         it can be an ordinary name that happens to contain both characters, and returning it
    ///         verbatim let arbitrary DDL through:
    ///         <c>[ix] ON t(id); DROP TABLE victim; --]</c> passed through untouched and executed.
    ///         Every name is escaped on its own terms; a name literally called <c>[x]</c> correctly
    ///         becomes <c>[[x]]]</c>.
    ///     </para>
    /// </remarks>
    public static string QuoteName(string name)
        => name.IsEmpty() || (IsRegularIdentifier(name) && !IsReservedWord(name))
            ? name
            : BracketName(name);

    /// <summary>
    ///     Quote one entry of a column list, leaving an entry the caller already bracketed alone.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Column lists were never quoted at all before, so users whose column needed brackets
    ///         had no option but to bracket it themselves inside the string. Running
    ///         <see cref="QuoteName" /> over those would re-escape a name that was already correct
    ///         and emit a column that does not exist. This is the only place that pass-through is
    ///         safe, and only because it is narrow: the entry must be bracketed end to end with every
    ///         interior <c>]</c> already doubled, which is exactly the output of
    ///         <see cref="BracketName" />.
    ///     </para>
    ///     <para>
    ///         That narrowness is what keeps the injection closed. A name merely starting with
    ///         <c>[</c> and ending with <c>]</c> does not qualify —
    ///         <c>[ix] ON t(id); DROP TABLE victim; --]</c> contains a lone <c>]</c>, so it is
    ///         bracketed on its own terms rather than passed through.
    ///     </para>
    /// </remarks>
    public static string QuoteColumnEntry(string name)
        => IsAlreadyBracketed(name) ? name : QuoteName(name);

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
