using JasperFx.Core;
using Weasel.Core;

namespace Weasel.SqlServer;

/// <summary>
///     SQL Server's identifier rules. Everything that is not dialect-specific — delimiting,
///     escaping, the already-delimited pass-through, undelimiting, string literal escaping —
///     lives in <see cref="IdentifierRules" />; what stays here is the delimiter pair, SQL
///     Server's regular-identifier rule, and its keyword list.
/// </summary>
public sealed class SqlServerIdentifierRules: IdentifierRules
{
    public static readonly SqlServerIdentifierRules Instance = new();

    protected override char Open => '[';
    protected override char Close => ']';

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
    /// </remarks>
    public override bool IsRegularIdentifier(string name)
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

    public override bool IsReservedWord(string name)
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

/// <summary>
///     The static facade the SQL Server DDL writers call. Delegates to
///     <see cref="SqlServerIdentifierRules" />; the behaviour and the doc comments for each
///     operation live there and in <see cref="IdentifierRules" />.
/// </summary>
public static class SchemaUtils
{
    /// <inheritdoc cref="IdentifierRules.DelimitIfNeeded" />
    public static string BracketName(string name) => SqlServerIdentifierRules.Instance.DelimitIfNeeded(name);

    /// <inheritdoc cref="IdentifierRules.EscapeLiteral" />
    public static string EscapeLiteral(string value) => IdentifierRules.EscapeLiteral(value);

    /// <inheritdoc cref="IdentifierRules.Quote" />
    public static string QuoteName(string name) => SqlServerIdentifierRules.Instance.Quote(name);

    /// <inheritdoc cref="IdentifierRules.Undelimit" />
    public static string Unbracket(string name) => SqlServerIdentifierRules.Instance.Undelimit(name);

    /// <inheritdoc cref="IdentifierRules.Delimit" />
    internal static string Bracket(string name) => SqlServerIdentifierRules.Instance.Delimit(name);

    /// <inheritdoc cref="IdentifierRules.IsRegularIdentifier" />
    public static bool IsRegularIdentifier(string name) => SqlServerIdentifierRules.Instance.IsRegularIdentifier(name);

    /// <inheritdoc cref="IdentifierRules.IsReservedWord" />
    public static bool IsReservedWord(string name) => SqlServerIdentifierRules.Instance.IsReservedWord(name);
}
