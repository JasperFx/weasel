using Npgsql;

namespace Weasel.Postgresql;

public static class SchemaUtils
{
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

    public static string QuoteName(string name)
    {
        return ReservedKeywords.Contains(name, StringComparer.InvariantCultureIgnoreCase) || name.Any(char.IsUpper) ? $"\"{name}\"" : name;
    }

    private static readonly string[] ReservedKeywords =
    [
        "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC", "AUTHORIZATION",
        "BINARY", "BOTH", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "CONCURRENTLY", "CONSTRAINT",
        "CREATE", "CROSS", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_SCHEMA",
        "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT",
        "DO", "ELSE", "END", "EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN", "FREEZE", "FROM", "FULL",
        "GRANT", "GROUP", "HAVING", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO", "IS",
        "ISNULL", "JOIN", "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP",
        "NATURAL", "NOT", "NOTNULL", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVERLAPS",
        "PLACING", "PRIMARY", "REFERENCES", "RETURNING", "RIGHT", "SELECT", "SESSION_USER", "SIMILAR",
        "SOME", "SYMMETRIC", "TABLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER",
        "USING", "VARIADIC", "VERBOSE", "WHEN", "WHERE", "WINDOW", "WITH"
    ];
}

