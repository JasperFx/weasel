namespace Weasel.MySql;

public static class SchemaUtils
{
    public static string QuoteName(string name)
    {
        return $"`{name}`";
    }

    public static string QuoteQualifiedName(string schema, string name)
    {
        return string.IsNullOrEmpty(schema)
            ? QuoteName(name)
            : $"{QuoteName(schema)}.{QuoteName(name)}";
    }

    public static bool IsReservedKeyword(string name)
    {
        return ReservedKeywords.Contains(name, StringComparer.OrdinalIgnoreCase);
    }

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
