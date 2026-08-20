using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Oracle;

/// <summary>
///     Oracle's identifier rules. Everything that is not dialect-specific lives in
///     <see cref="IdentifierRules" />; what stays here is the double-quote delimiter, Oracle's
///     regular-identifier rule, its keyword list, and its upper-case folding.
/// </summary>
public sealed class OracleIdentifierRules: IdentifierRules
{
    public static readonly OracleIdentifierRules Instance = new();

    protected override char Open => '"';
    protected override char Close => '"';

    /// <summary>
    ///     A regular identifier: a letter first, then letters, digits, <c>_</c>, <c>$</c> or
    ///     <c>#</c>. Case is not part of the question — Oracle folds an undelimited identifier, so
    ///     a mixed-case name is still regular; see <see cref="DelimitedForm" />.
    /// </summary>
    public override bool IsRegularIdentifier(string name)
    {
        if (name.IsEmpty() || !char.IsLetter(name[0]))
        {
            return false;
        }

        return name.Skip(1).All(c => char.IsLetterOrDigit(c) || c == '_' || c == '$' || c == '#');
    }

    /// <summary>
    ///     Oracle folds an undelimited identifier to upper case, so anything that has to be
    ///     delimited is delimited in the folded spelling — landing on the same object it would have
    ///     had bare. This is what the old implementation did for reserved words, and doing it for
    ///     every delimited name is what makes the two cases consistent.
    /// </summary>
    protected override string DelimitedForm(string name) => name.ToUpperInvariant();

    /// <summary>
    ///     Oracle resolves an undelimited identifier by folding it, so two spellings that differ
    ///     only in case are one object.
    /// </summary>
    public override bool SameObject(string a, string b)
        => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);

    public override bool IsReservedWord(string name)
        => ReservedKeywords.Contains(name, StringComparer.OrdinalIgnoreCase);

    private static readonly string[] ReservedKeywords =
    [
        "ACCESS",
        "ADD",
        "ALL",
        "ALTER",
        "AND",
        "ANY",
        "AS",
        "ASC",
        "AUDIT",
        "BETWEEN",
        "BY",
        "CHAR",
        "CHECK",
        "CLUSTER",
        "COLUMN",
        "COMMENT",
        "COMPRESS",
        "CONNECT",
        "CREATE",
        "CURRENT",
        "DATE",
        "DECIMAL",
        "DEFAULT",
        "DELETE",
        "DESC",
        "DISTINCT",
        "DROP",
        "ELSE",
        "EXCLUSIVE",
        "EXISTS",
        "FILE",
        "FLOAT",
        "FOR",
        "FROM",
        "GRANT",
        "GROUP",
        "HAVING",
        "IDENTIFIED",
        "IMMEDIATE",
        "IN",
        "INCREMENT",
        "INDEX",
        "INITIAL",
        "INSERT",
        "INTEGER",
        "INTERSECT",
        "INTO",
        "IS",
        "LEVEL",
        "LIKE",
        "LOCK",
        "LONG",
        "MAXEXTENTS",
        "MINUS",
        "MLSLABEL",
        "MODE",
        "MODIFY",
        "NOAUDIT",
        "NOCOMPRESS",
        "NOT",
        "NOWAIT",
        "NULL",
        "NUMBER",
        "OF",
        "OFFLINE",
        "ON",
        "ONLINE",
        "OPTION",
        "OR",
        "ORDER",
        "PCTFREE",
        "PRIOR",
        "PUBLIC",
        "RAW",
        "RENAME",
        "RESOURCE",
        "REVOKE",
        "ROW",
        "ROWID",
        "ROWNUM",
        "ROWS",
        "SELECT",
        "SESSION",
        "SET",
        "SHARE",
        "SIZE",
        "SMALLINT",
        "START",
        "SUCCESSFUL",
        "SYNONYM",
        "SYSDATE",
        "TABLE",
        "THEN",
        "TO",
        "TRIGGER",
        "UID",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "USER",
        "VALIDATE",
        "VALUES",
        "VARCHAR",
        "VARCHAR2",
        "VIEW",
        "WHENEVER",
        "WHERE",
        "WITH"
    ];
}

/// <summary>
///     The static facade the Oracle DDL writers call. Delegates to
///     <see cref="OracleIdentifierRules" />.
/// </summary>
public static class SchemaUtils
{
    /// <inheritdoc cref="IdentifierRules.Quote" />
    public static string QuoteName(string name) => OracleIdentifierRules.Instance.Quote(name);

    /// <inheritdoc cref="IdentifierRules.Undelimit" />
    public static string Unquote(string name) => OracleIdentifierRules.Instance.Undelimit(name);

    /// <inheritdoc cref="IdentifierRules.EscapeLiteral" />
    public static string EscapeLiteral(string value) => IdentifierRules.EscapeLiteral(value);

    public static bool IsReservedKeyword(string name) => OracleIdentifierRules.Instance.IsReservedWord(name);
}
