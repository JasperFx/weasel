using System.Collections.Frozen;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;

namespace Weasel.Postgresql;

/// <summary>
///     PostgreSQL's identifier rules. Everything that is not dialect-specific lives in
///     <see cref="IdentifierRules" />; what stays here is the double-quote delimiter, PostgreSQL's
///     regular-identifier rule, and its three keyword categories.
/// </summary>
/// <remarks>
///     The usage distinction is modelled as two instances rather than a parameter, because it only
///     ever changes which keyword categories force quoting: a type/function name keyword must be
///     quoted in general use but not as a function name, and a column name keyword the other way
///     round.
/// </remarks>
public sealed class PostgresqlIdentifierRules: IdentifierRules
{
    public static readonly PostgresqlIdentifierRules General = new(SchemaUtils.IdentifierUsage.General);
    public static readonly PostgresqlIdentifierRules Function = new(SchemaUtils.IdentifierUsage.Function);

    private readonly SchemaUtils.IdentifierUsage _usage;

    private PostgresqlIdentifierRules(SchemaUtils.IdentifierUsage usage) => _usage = usage;

    public static PostgresqlIdentifierRules For(SchemaUtils.IdentifierUsage usage)
        => usage == SchemaUtils.IdentifierUsage.Function ? Function : General;

    protected override char Open => '"';
    protected override char Close => '"';

    /// <summary>
    ///     A regular identifier: a letter or <c>_</c> first, then letters, digits, <c>_</c> or
    ///     <c>$</c>. Case is handled separately in <see cref="RequiresDelimiting" />, because an
    ///     uppercase name is syntactically fine bare — PostgreSQL just folds it, which silently
    ///     changes which object the name refers to.
    /// </summary>
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

    /// <summary>
    ///     Quoted when it is a keyword in a category that applies to this usage, when it carries an
    ///     uppercase character that would otherwise be folded away, or — the part that was missing —
    ///     when it simply cannot be written bare. The old rule looked only at keywords and case, so
    ///     a lowercase name with a hyphen, a dot or a leading digit went out unquoted and produced
    ///     DDL that will not parse.
    /// </summary>
    public override bool RequiresDelimiting(string name)
    {
        if (name.IsEmpty())
        {
            return false;
        }

        return !IsRegularIdentifier(name)
               || name.Any(char.IsUpper)
               || IsReservedWord(name)
               || (_usage == SchemaUtils.IdentifierUsage.Function && CategoryCKeywords.Contains(name))
               || (_usage == SchemaUtils.IdentifierUsage.General && CategoryTKeywords.Contains(name));
    }

    public override bool IsReservedWord(string name) => ReservedKeywords.Contains(name);

    /// <summary>
    /// PostgreSQL reserved keywords (catcode 'R').
    /// These keywords are always reserved and must be quoted when used as identifiers.
    /// Source: pg_get_keywords() from PostgreSQL 10-17
    /// </summary>
    private static readonly FrozenSet<string> ReservedKeywords = new[]
    {
        "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC", "BOTH",
        "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "CONSTRAINT", "CREATE", "CURRENT_CATALOG",
        "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
        "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO", "ELSE", "END", "EXCEPT", "FALSE",
        "FETCH", "FOR", "FOREIGN", "FROM", "GRANT", "GROUP", "HAVING", "IN", "INITIALLY",
        "INTERSECT", "INTO", "LATERAL", "LEADING", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP",
        "NOT", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "PLACING", "PRIMARY",
        "REFERENCES", "RETURNING", "SELECT", "SESSION_USER", "SOME", "SYMMETRIC", "SYSTEM_USER",
        "TABLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER", "USING",
        "VARIADIC", "WHEN", "WHERE", "WINDOW", "WITH"
    }.ToFrozenSet(StringComparer.InvariantCultureIgnoreCase);

    /// <summary>
    /// PostgreSQL column name keywords (catcode 'C').
    /// These keywords can be used without quotes except for function or type names.
    /// Source: pg_get_keywords() from PostgreSQL 10-17
    /// </summary>
    private static readonly FrozenSet<string> CategoryCKeywords = new[]
    {
        "BETWEEN", "BIGINT", "BIT", "BOOLEAN", "CHAR", "CHARACTER", "COALESCE", "DEC",
        "DECIMAL", "EXISTS", "EXTRACT", "FLOAT", "GREATEST", "GROUPING", "INOUT", "INT",
        "INTEGER", "INTERVAL", "JSON", "JSON_ARRAY", "JSON_ARRAYAGG", "JSON_EXISTS",
        "JSON_OBJECT", "JSON_OBJECTAGG", "JSON_QUERY", "JSON_SCALAR", "JSON_SERIALIZE",
        "JSON_TABLE", "JSON_VALUE", "LEAST", "MERGE_ACTION", "NATIONAL", "NCHAR", "NONE",
        "NORMALIZE", "NULLIF", "NUMERIC", "OUT", "OVERLAY", "POSITION", "PRECISION",
        "REAL", "ROW", "SETOF", "SMALLINT", "SUBSTRING", "TIME", "TIMESTAMP", "TREAT",
        "TRIM", "VALUES", "VARCHAR", "XMLATTRIBUTES", "XMLCONCAT", "XMLELEMENT",
        "XMLEXISTS", "XMLFOREST", "XMLNAMESPACES", "XMLPARSE", "XMLPI", "XMLROOT",
        "XMLSERIALIZE", "XMLTABLE"
    }.ToFrozenSet(StringComparer.InvariantCultureIgnoreCase);

    /// <summary>
    /// PostgreSQL type/function name keywords (catcode 'T').
    /// These keywords can only be used for function or type names without quotes.
    /// Anywhere else will need to be quoted.
    /// Source: pg_get_keywords() from PostgreSQL 10-17
    /// </summary>
    private static readonly FrozenSet<string> CategoryTKeywords = new[]
    {
        "AUTHORIZATION", "BINARY", "COLLATION", "CONCURRENTLY", "CROSS",
        "CURRENT_SCHEMA", "FREEZE", "FULL", "ILIKE", "INNER", "IS", "ISNULL", "JOIN",
        "LEFT", "LIKE", "NATURAL", "NOTNULL", "OUTER", "OVERLAPS", "RIGHT", "SIMILAR",
        "TABLESAMPLE", "VERBOSE"
    }.ToFrozenSet(StringComparer.InvariantCultureIgnoreCase);
}

public static class SchemaUtils
{
    public enum IdentifierUsage
    {
        General,
        Function
    }

    // TODO: This should probably go to Weasel
    public static async Task DropSchema(string connectionString, string schemaName)
    {
        var reconnectionCount = 0;
        const int maxReconnectionCount = 3;

        var success = false;

        do
        {
            success = await dropSchema(connectionString, schemaName).ConfigureAwait(false);

            if (success || ++reconnectionCount == maxReconnectionCount)
                return;

            await Task.Delay(reconnectionCount * 50, CancellationToken.None).ConfigureAwait(false);
        } while (!success && reconnectionCount < maxReconnectionCount);

        throw new InvalidOperationException($"Unable to drop schema: ${schemaName}");
    }

    private static async Task<bool> dropSchema(string connectionString, string schemaName)
    {
        try
        {
            await using var dbConn = new NpgsqlConnection(connectionString);
            await dbConn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            await dbConn.DropSchemaAsync(schemaName, CancellationToken.None).ConfigureAwait(false);

            return true;
        }
        catch (PostgresException pgException)
        {
            if (pgException.SqlState == PostgresErrorCodes.AdminShutdown)
                return false;

            throw;
        }
    }

    /// <summary>
    /// Quotes a PostgreSQL identifier if it is a reserved or type-function keyword, or contains uppercase characters.
    /// Equivalent to calling <c>QuoteName(name, IdentifierUsage.General)</c>.
    /// </summary>
    [Obsolete("Use QuoteName(string, IdentifierUsage) for context-aware quoting. This overload will be removed in a future version.")]
    public static string QuoteName(string name) => QuoteName(name, IdentifierUsage.General);

    /// <summary>
    /// Quotes a PostgreSQL identifier if it is a keyword, contains uppercase characters, or cannot
    /// be written bare. Keywords are checked according to their PostgreSQL category:
    /// - Reserved (catcode 'R'): always quoted
    /// - Type/function name (catcode 'T'): quoted in General usage, not in Function usage
    /// - Column name (catcode 'C'): quoted only in Function usage
    /// Mixed-case identifiers are always quoted to preserve case.
    /// </summary>
    public static string QuoteName(string name, IdentifierUsage usage)
        => PostgresqlIdentifierRules.For(usage).Quote(name);

    /// <inheritdoc cref="IdentifierRules.Undelimit" />
    public static string Unquote(string name) => PostgresqlIdentifierRules.General.Undelimit(name);

    /// <inheritdoc cref="IdentifierRules.EscapeLiteral" />
    public static string EscapeLiteral(string value) => IdentifierRules.EscapeLiteral(value);

    /// <summary>
    /// Returns true if the given identifier is a PostgreSQL reserved keyword (catcode 'R')
    /// that must always be quoted when used as an identifier.
    /// </summary>
    public static bool IsReservedKeyword(string name) => PostgresqlIdentifierRules.General.IsReservedWord(name);
}
