using System.Data;
using FirebirdSql.Data.FirebirdClient;
using JasperFx.Core;

namespace Weasel.Firebird;

public static partial class SchemaObjectsExtensions
{
    /// <summary>
    ///     How many passes <see cref="DropSchemaAsync(FbConnection, CancellationToken)" /> makes before it
    ///     gives up on objects that will not drop.
    /// </summary>
    private const int MaxTeardownPasses = 10;

    /// <summary>
    ///     The kinds of object a teardown drops, in the order one pass tries them, each with the query
    ///     that lists the user-defined ones and the statement that drops one. Every query filters on
    ///     <c>RDB$SYSTEM_FLAG = 0</c>, which leaves out the server's own objects, the two triggers every
    ///     <c>CHECK</c> constraint gets (flag 3), and the generator behind every identity column (flag 6);
    ///     each goes with the object it belongs to.
    /// </summary>
    private static readonly (string Kind, string Query, Func<string, string, string> Drop)[] TeardownSteps =
    [
        ("foreign key", """
                        SELECT TRIM(rc.RDB$RELATION_NAME), TRIM(rc.RDB$CONSTRAINT_NAME)
                        FROM RDB$RELATION_CONSTRAINTS rc
                        JOIN RDB$RELATIONS r ON r.RDB$RELATION_NAME = rc.RDB$RELATION_NAME
                        WHERE rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY' AND COALESCE(r.RDB$SYSTEM_FLAG, 0) = 0
                        """,
            (table, name) => $"ALTER TABLE {delimit(table)} DROP CONSTRAINT {delimit(name)}"),
        ("trigger", "SELECT '', TRIM(RDB$TRIGGER_NAME) FROM RDB$TRIGGERS WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0",
            (_, name) => $"DROP TRIGGER {delimit(name)}"),
        ("package", "SELECT '', TRIM(RDB$PACKAGE_NAME) FROM RDB$PACKAGES WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0",
            (_, name) => $"DROP PACKAGE {delimit(name)}"),
        ("procedure", """
                      SELECT '', TRIM(RDB$PROCEDURE_NAME) FROM RDB$PROCEDURES
                      WHERE RDB$PACKAGE_NAME IS NULL AND COALESCE(RDB$SYSTEM_FLAG, 0) = 0
                      """,
            (_, name) => $"DROP PROCEDURE {delimit(name)}"),
        ("function", """
                     SELECT IIF(COALESCE(RDB$LEGACY_FLAG, 0) = 1, 'EXTERNAL', ''), TRIM(RDB$FUNCTION_NAME) FROM RDB$FUNCTIONS
                     WHERE RDB$PACKAGE_NAME IS NULL AND COALESCE(RDB$SYSTEM_FLAG, 0) = 0
                     """,
            (legacy, name) => legacy == "EXTERNAL"
                ? $"DROP EXTERNAL FUNCTION {delimit(name)}"
                : $"DROP FUNCTION {delimit(name)}"),
        ("view", """
                 SELECT '', TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS
                 WHERE RDB$VIEW_BLR IS NOT NULL AND COALESCE(RDB$SYSTEM_FLAG, 0) = 0
                 """,
            (_, name) => $"DROP VIEW {delimit(name)}"),
        ("table", """
                  SELECT '', TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS
                  WHERE RDB$VIEW_BLR IS NULL AND COALESCE(RDB$SYSTEM_FLAG, 0) = 0
                  """,
            (_, name) => $"DROP TABLE {delimit(name)}"),
        ("sequence", "SELECT '', TRIM(RDB$GENERATOR_NAME) FROM RDB$GENERATORS WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0",
            (_, name) => $"DROP SEQUENCE {delimit(name)}"),
        ("exception", "SELECT '', TRIM(RDB$EXCEPTION_NAME) FROM RDB$EXCEPTIONS WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0",
            (_, name) => $"DROP EXCEPTION {delimit(name)}"),
        ("domain", """
                   SELECT '', TRIM(RDB$FIELD_NAME) FROM RDB$FIELDS
                   WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0 AND RDB$FIELD_NAME NOT STARTING WITH 'RDB$'
                   """,
            (_, name) => $"DROP DOMAIN {delimit(name)}")
    ];

    /// <summary>
    ///     Empty the database of every object a user can create. Firebird 3, 4 and 5 have no schemas, so
    ///     the database is the schema; it is emptied rather than dropped, because the connection doing
    ///     the work is attached to it.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         There is no server-side cascade to lean on, so this enumerates: foreign keys first, then
    ///         triggers, packages, procedures, functions (legacy UDFs included), views, tables (global
    ///         temporary ones included), sequences, exceptions and domains. It repeats until a pass
    ///         drops nothing, which settles a procedure that calls another, or a view over a view,
    ///         without working out a dependency graph. <em>Every new creatable object type has to be
    ///         added here too</em> (weasel#464, weasel#465).
    ///     </para>
    ///     <para>
    ///         The connection pool is cleared first: an idle pooled attachment still holds the tables it
    ///         touched, and <c>DROP TABLE</c> refuses a table in use. Each drop runs in its own
    ///         <c>WAIT</c> transaction, so a drop beside a closing attachment waits for it instead of
    ///         failing at once.
    ///     </para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">Something could not be dropped.</exception>
    public static async Task DropSchemaAsync(this FbConnection conn, CancellationToken ct = default)
    {
        if (conn.State == ConnectionState.Closed)
        {
            await conn.OpenAsync(ct).ConfigureAwait(false);
        }

        FbConnection.ClearPool(conn);

        var migrator = new FirebirdMigrator();
        var failures = new Dictionary<string, Exception>();

        for (var pass = 0; pass < MaxTeardownPasses; pass++)
        {
            failures.Clear();
            var dropped = 0;
            var remaining = 0;

            foreach (var (kind, query, drop) in TeardownSteps)
            {
                foreach (var (owner, name) in await listAsync(conn, query, ct).ConfigureAwait(false))
                {
                    remaining++;

                    var failure = await migrator.TryExecuteInOwnTransactionAsync(conn, drop(owner, name), ct)
                        .ConfigureAwait(false);

                    if (failure == null)
                    {
                        dropped++;
                    }
                    else
                    {
                        failures[$"{kind} {name}"] = failure;
                    }
                }
            }

            if (remaining == 0 || failures.Count == 0)
            {
                return;
            }

            if (dropped == 0)
            {
                break;
            }
        }

        throw new InvalidOperationException(
            $"Could not empty the database: {failures.Select(x => $"{x.Key} ({x.Value.Message.ReplaceLineEndings(" ")})").Join("; ")}",
            failures.Values.First());
    }

    /// <summary>
    ///     Firebird has no schemas, so the only schema there is to drop is the default one, and dropping it
    ///     empties the database. Any other name is refused.
    /// </summary>
    public static Task DropSchemaAsync(this FbConnection conn, string schemaName, CancellationToken ct = default)
    {
        FirebirdObjectName.AssertDefaultSchema(schemaName);
        return conn.DropSchemaAsync(ct);
    }

    /// <summary>
    ///     Empty the database -- there is no schema to create again afterwards.
    /// </summary>
    public static Task ResetSchemaAsync(this FbConnection conn, string schemaName, CancellationToken ct = default)
        => conn.DropSchemaAsync(schemaName, ct);

    private static async Task<List<(string Owner, string Name)>> listAsync(FbConnection conn, string sql,
        CancellationToken ct)
    {
        var names = new List<(string, string)>();

        await using var cmd = conn.CreateCommand(sql);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            names.Add((reader.GetString(0), reader.GetString(1)));
        }

        return names;
    }

    private static string delimit(string name) => FirebirdIdentifierRules.Instance.Delimit(name);
}
