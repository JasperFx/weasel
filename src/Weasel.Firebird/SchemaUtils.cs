using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Firebird;

/// <summary>
///     Firebird's identifier rules. Everything that is not dialect-specific lives in
///     <see cref="IdentifierRules" />; what stays here is the double-quote delimiter, Firebird's
///     regular-identifier rule, its keyword list, and its upper-case folding.
/// </summary>
/// <remarks>
///     Firebird resolves identifiers the way Oracle does: an undelimited name is folded to upper
///     case, a delimited one is taken exactly as written. So this is Oracle's rule set with
///     Firebird's own lexical rules and keywords.
/// </remarks>
public sealed class FirebirdIdentifierRules: IdentifierRules
{
    public static readonly FirebirdIdentifierRules Instance = new();

    protected override char Open => '"';
    protected override char Close => '"';

    /// <summary>
    ///     A regular identifier: an ASCII letter first, then ASCII letters, digits, <c>_</c> or
    ///     <c>$</c>. A leading underscore or digit, and any character outside ASCII, has to be
    ///     delimited. Case is not part of the question -- Firebird folds an undelimited identifier,
    ///     so a mixed-case name is still regular; see <see cref="DelimitedForm" />.
    /// </summary>
    public override bool IsRegularIdentifier(string name)
    {
        if (name.IsEmpty() || !char.IsAsciiLetter(name[0]))
        {
            return false;
        }

        return name.Skip(1).All(c => char.IsAsciiLetterOrDigit(c) || c == '_' || c == '$');
    }

    /// <summary>
    ///     Firebird folds an undelimited identifier to upper case, so anything that has to be
    ///     delimited is delimited in the folded spelling -- landing on the same object it would have
    ///     had bare, and on the spelling introspection binds.
    /// </summary>
    protected override string DelimitedForm(string name) => name.ToUpperInvariant();

    /// <summary>
    ///     Firebird resolves an undelimited identifier by folding it, so two spellings that differ
    ///     only in case are one object.
    /// </summary>
    public override bool SameObject(string a, string b)
        => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);

    public override bool IsReservedWord(string name) => ReservedKeywords.Contains(name);

    /// <summary>
    ///     The union of the reserved words of Firebird 3, 4 and 5, taken by probing each server with
    ///     <c>CREATE TABLE &lt;word&gt;</c> and cross-checked against Firebird 5's
    ///     <c>RDB$KEYWORDS</c>. A union, because one DDL script has to run on all three: a word that
    ///     became reserved in 4 still has to be delimited when the script is written for 3.
    /// </summary>
    private static readonly HashSet<string> ReservedKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "ADD", "ADMIN", "ALL", "ALTER", "AND", "ANY", "AS", "AT", "AVG", "BEGIN", "BETWEEN", "BIGINT",
        "BINARY", "BIT_LENGTH", "BLOB", "BOOLEAN", "BOTH", "BY", "CASE", "CAST", "CHAR", "CHARACTER",
        "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CLOSE", "COLLATE", "COLUMN", "COMMENT", "COMMIT",
        "CONNECT", "CONSTRAINT", "CORR", "COUNT", "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CURRENT",
        "CURRENT_CONNECTION", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
        "CURRENT_TRANSACTION", "CURRENT_USER", "CURSOR", "DATE", "DAY", "DEC", "DECFLOAT", "DECIMAL",
        "DECLARE", "DEFAULT", "DELETE", "DELETING", "DETERMINISTIC", "DISCONNECT", "DISTINCT", "DOUBLE",
        "DROP", "ELSE", "END", "ESCAPE", "EXECUTE", "EXISTS", "EXTERNAL", "EXTRACT", "FALSE", "FETCH",
        "FILTER", "FLOAT", "FOR", "FOREIGN", "FROM", "FULL", "FUNCTION", "GDSCODE", "GLOBAL", "GRANT",
        "GROUP", "HAVING", "HOUR", "IN", "INDEX", "INNER", "INSENSITIVE", "INSERT", "INSERTING", "INT",
        "INT128", "INTEGER", "INTO", "IS", "JOIN", "LATERAL", "LEADING", "LEFT", "LIKE", "LOCAL",
        "LOCALTIME", "LOCALTIMESTAMP", "LONG", "LOWER", "MAX", "MERGE", "MIN", "MINUTE", "MONTH",
        "NATIONAL", "NATURAL", "NCHAR", "NO", "NOT", "NULL", "NUMERIC", "OCTET_LENGTH", "OF", "OFFSET",
        "ON", "ONLY", "OPEN", "OR", "ORDER", "OUTER", "OVER", "PARAMETER", "PLAN", "POSITION",
        "POST_EVENT", "PRECISION", "PRIMARY", "PROCEDURE", "PUBLICATION", "RDB$DB_KEY", "RDB$ERROR",
        "RDB$GET_CONTEXT", "RDB$GET_TRANSACTION_CN", "RDB$RECORD_VERSION", "RDB$RESET_CONTEXT",
        "RDB$ROLE_IN_USE", "RDB$SET_CONTEXT", "RDB$SYSTEM_PRIVILEGE", "REAL", "RECORD_VERSION",
        "RECREATE", "RECURSIVE", "REFERENCES", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT",
        "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY", "RELEASE", "RESETTING", "RETURN",
        "RETURNING_VALUES", "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROW", "ROWS", "ROW_COUNT",
        "SAVEPOINT", "SCHEMA", "SCROLL", "SECOND", "SELECT", "SENSITIVE", "SET", "SIMILAR", "SMALLINT",
        "SOME", "SQLCODE", "SQLSTATE", "START", "STDDEV_POP", "STDDEV_SAMP", "SUM", "TABLE", "THEN",
        "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRIGGER", "TRIM",
        "TRUE", "UNBOUNDED", "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPDATING", "UPPER", "USER", "USING",
        "VALUE", "VALUES", "VARBINARY", "VARCHAR", "VARIABLE", "VARYING", "VAR_POP", "VAR_SAMP", "VIEW",
        "WHEN", "WHERE", "WHILE", "WINDOW", "WITH", "WITHOUT", "YEAR"
    };
}

/// <summary>
///     The static facade the Firebird DDL writers call. Delegates to
///     <see cref="FirebirdIdentifierRules" />.
/// </summary>
public static class SchemaUtils
{
    /// <inheritdoc cref="IdentifierRules.Quote" />
    public static string QuoteName(string name) => FirebirdIdentifierRules.Instance.Quote(name);

    /// <summary>
    ///     Quote a name the way a table writes it: as <see cref="QuoteName(string)" /> does, or -- when the table
    ///     preserves identifier case -- delimited exactly as written, so the catalog keeps its case.
    /// </summary>
    public static string QuoteName(string name, bool preserveCase)
        => preserveCase ? FirebirdIdentifierRules.Instance.Delimit(name) : QuoteName(name);

    /// <inheritdoc cref="IdentifierRules.Undelimit" />
    public static string Unquote(string name) => FirebirdIdentifierRules.Instance.Undelimit(name);

    /// <inheritdoc cref="IdentifierRules.EscapeLiteral" />
    public static string EscapeLiteral(string value) => IdentifierRules.EscapeLiteral(value);

    public static bool IsReservedKeyword(string name) => FirebirdIdentifierRules.Instance.IsReservedWord(name);

    /// <summary>
    ///     The spelling the catalog stores for a name Weasel writes into DDL, which is the spelling
    ///     every introspection query and existence guard has to bind. An undelimited name is folded
    ///     by the server and a delimited one is written in the folded spelling (see
    ///     <see cref="FirebirdIdentifierRules" />), so both land upper-cased -- unless the table
    ///     preserves case, in which case the name is delimited exactly as written.
    /// </summary>
    public static string CatalogName(string name, bool preserveCase = false)
        => preserveCase ? name : name.ToUpperInvariant();
}
