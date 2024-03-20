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
}

