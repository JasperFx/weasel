using JasperFx.Core;
using Weasel.Core;

namespace Weasel.MySql;

/// <summary>
///     MySQL's identifier rules. Everything that is not dialect-specific lives in
///     <see cref="IdentifierRules" />; what stays here is the backtick delimiter, MySQL's
///     regular-identifier rule, and its keyword list.
/// </summary>
public sealed class MySqlIdentifierRules: IdentifierRules
{
    public static readonly MySqlIdentifierRules Instance = new();

    protected override char Open => '`';
    protected override char Close => '`';

    /// <summary>
    ///     MySQL delimits every identifier, which is what it has always done here — keeping it
    ///     means the generated DDL for an ordinary schema is byte-identical to before. What
    ///     changes is that an embedded backtick is now doubled instead of closing the quoting.
    /// </summary>
    public override bool RequiresDelimiting(string name) => true;

    /// <summary>
    ///     Unquoted MySQL identifiers permit ASCII letters, digits, <c>_</c> and <c>$</c>, plus
    ///     extended characters, and may not consist solely of digits. Since
    ///     <see cref="RequiresDelimiting" /> is always true this only informs callers; it does not
    ///     gate the quoting.
    /// </summary>
    public override bool IsRegularIdentifier(string name)
    {
        if (name.IsEmpty() || name.All(char.IsDigit))
        {
            return false;
        }

        return name.All(c => char.IsLetterOrDigit(c) || c == '_' || c == '$');
    }

    public override bool IsReservedWord(string name) => ReservedKeywords.Contains(name);

    private static readonly HashSet<string> ReservedKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC",
        "BEFORE", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL",
        "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE",
        "COLUMN", "CONSTRAINT", "CONTINUE", "CREATE", "CROSS", "CURRENT_DATE",
        "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE",
        "DATABASES", "DEFAULT", "DELETE", "DESC", "DESCRIBE", "DISTINCT", "DIV",
        "DOUBLE", "DROP", "DUAL", "EACH", "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED",
        "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FLOAT", "FOR", "FORCE",
        "FOREIGN", "FROM", "FULLTEXT", "GRANT", "GROUP", "HAVING", "IF", "IGNORE",
        "IN", "INDEX", "INFILE", "INNER", "INSERT", "INT", "INTEGER", "INTERVAL",
        "INTO", "IS", "ITERATE", "JOIN", "KEY", "KEYS", "KILL", "LEADING", "LEAVE",
        "LEFT", "LIKE", "LIMIT", "LINES", "LOAD", "LOCK", "LONG", "LOOP", "MATCH",
        "NATURAL", "NOT", "NULL", "NUMERIC", "ON", "OPTIMIZE", "OPTION",
        "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "PRIMARY",
        "PROCEDURE", "RANGE", "READ", "READS", "REAL", "REFERENCES", "REGEXP",
        "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESTRICT", "RETURN",
        "REVOKE", "RIGHT", "SCHEMA", "SCHEMAS", "SELECT", "SET", "SHOW",
        "SMALLINT", "SPATIAL", "SQL", "STARTING", "TABLE", "TERMINATED", "THEN",
        "TINYINT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE",
        "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "VALUES",
        "VARBINARY", "VARCHAR", "WHEN", "WHERE", "WHILE", "WITH", "WRITE", "XOR"
    };
}

/// <summary>
///     The static facade the MySQL DDL writers call. Delegates to
///     <see cref="MySqlIdentifierRules" />.
/// </summary>
public static class SchemaUtils
{
    /// <inheritdoc cref="IdentifierRules.Quote" />
    public static string QuoteName(string name) => MySqlIdentifierRules.Instance.Quote(name);

    public static string QuoteQualifiedName(string schema, string name)
        => string.IsNullOrEmpty(schema)
            ? QuoteName(name)
            : $"{QuoteName(schema)}.{QuoteName(name)}";

    /// <inheritdoc cref="IdentifierRules.Undelimit" />
    public static string Unquote(string name) => MySqlIdentifierRules.Instance.Undelimit(name);

    /// <inheritdoc cref="IdentifierRules.EscapeLiteral" />
    public static string EscapeLiteral(string value) => IdentifierRules.EscapeLiteral(value);

    public static bool IsReservedKeyword(string name) => MySqlIdentifierRules.Instance.IsReservedWord(name);
}
