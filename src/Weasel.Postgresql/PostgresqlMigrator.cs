using System.Data.Common;
using System.Text;
using JasperFx;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Migrations;
using Weasel.Postgresql.Tables;

namespace Weasel.Postgresql;

public class PostgresqlMigrator: Migrator
{
    private const string BeginScript = @"DO $$
BEGIN";

    private const string EndScript = @"END
$$;
";

    public PostgresqlMigrator(): base(PostgresqlProvider.Instance.DefaultDatabaseSchemaName)
    {
    }

    /// <summary>
    ///     Used to validate database object name lengths against Postgresql's NAMEDATALEN property to avoid
    ///     Marten getting confused when comparing database schemas against the configuration. See
    ///     https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html
    ///     for more information. This does NOT adjust NAMEDATALEN for you.
    /// </summary>
    public int NameDataLength { get; set; } = 64;

    public override bool MatchesConnection(DbConnection connection)
    {
        return connection is NpgsqlConnection;
    }

    public override IDatabaseProvider Provider => PostgresqlProvider.Instance;

    /// <summary>
    ///     Write out a templated SQL script with all rules
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="writeStep">A continuation to write the inner SQL</param>
    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
    {
        if (IsTransactional)
        {
            writer.WriteLine("DO LANGUAGE plpgsql $tran$");
            writer.WriteLine("BEGIN");
            writer.WriteLine("");
        }

        if (Role.IsNotEmpty())
        {
            writer.WriteLine($"SET ROLE {Role};");
            writer.WriteLine("");
        }

        writeStep(this, writer);

        if (Role.IsNotEmpty())
        {
            writer.WriteLine("RESET ROLE;");
            writer.WriteLine("");
        }

        if (IsTransactional)
        {
            writer.WriteLine("");
            writer.WriteLine("END;");
            writer.WriteLine("$tran$;");
        }
    }

    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        writer.Write(BeginScript);

        foreach (var schemaName in schemaNames) WriteSql(schemaName, writer);

        writer.WriteLine(EndScript);
        writer.WriteLine();
    }

    public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames)
        {
            writer.WriteLine(SchemaObjectsExtensions.DropStatementFor(schemaName));
        }
    }

    private static void WriteSql(string databaseSchemaName, TextWriter writer)
    {
        // Neither the "IF NOT EXISTS(information_schema.schemata) THEN EXECUTE 'CREATE SCHEMA'"
        // pattern nor PostgreSQL's own "CREATE SCHEMA IF NOT EXISTS" is concurrent-safe — both
        // can have two sessions pass the existence check then race on the insert into pg_namespace,
        // surfacing as "23505 duplicate key value violates unique constraint pg_namespace_nspname_index"
        // / "42P06 schema X already exists". Wrap the create in a sub-block that swallows those two
        // race exceptions specifically; any other error still propagates.
        writer.WriteLine(
            $"""
                   BEGIN
                     EXECUTE 'CREATE SCHEMA IF NOT EXISTS {PostgresqlProvider.Instance.ToQualifiedName(databaseSchemaName)}';
                   EXCEPTION
                     WHEN duplicate_schema THEN NULL;
                     WHEN unique_violation THEN NULL;
                   END;

             """);
    }

    protected override async Task executeDelta(
        SchemaMigration migration,
        DbConnection conn,
        AutoCreate autoCreate,
        IMigrationLogger logger,
        CancellationToken ct = default
    )
    {
        var writer = new StringWriter();

        if (migration.Schemas.Any())
        {
            WriteSchemaCreationSql(migration.Schemas, writer);
        }

        migration.WriteAllUpdates(writer, this, autoCreate);

        var sqlCommands = writer
            .ToString()
            .Split(new[] { IndexDefinition.IndexCreationBeginComment, IndexDefinition.IndexCreationEndComment },
                StringSplitOptions.RemoveEmptyEntries);
        foreach (var sql in sqlCommands)
        {
            var cmd = conn.CreateCommand(sql);
            logger.SchemaChange(cmd.CommandText);

            try
            {
                await executeWithConcurrencyRetryAsync(cmd, ct).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                if (logger is DefaultMigrationLogger)
                {
                    throw;
                }

                logger.OnFailure(cmd, e);
            }
        }
    }

    /// <summary>
    ///     Maximum number of attempts (including the first) for a single migration
    ///     statement that fails with a transient catalog-concurrency error. The
    ///     migration DDL Weasel emits is idempotent (<c>IF NOT EXISTS</c> /
    ///     <c>CREATE OR REPLACE</c>) and each statement auto-commits independently,
    ///     so re-running a statement that lost a catalog race is safe.
    /// </summary>
    private const int MaxConcurrencyRetries = 3;

    /// <summary>
    ///     Execute a single migration statement, retrying a bounded number of times
    ///     on the transient PostgreSQL catalog-concurrency errors that surface when
    ///     two sessions lazily ensure storage against the same database at once
    ///     (weasel#293, follow-up to #282). A jittered backoff keeps two racers from
    ///     retrying in lockstep.
    /// </summary>
    private static async Task executeWithConcurrencyRetryAsync(DbCommand cmd, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (PostgresException e)
                when (attempt < MaxConcurrencyRetries && IsTransientCatalogConcurrency(e.SqlState, e.MessageText))
            {
                var delayMs = (50 * attempt) + Random.Shared.Next(0, 50);
                await Task.Delay(delayMs, ct).ConfigureAwait(false);

                // Some transient PostgreSQL errors (40P01 deadlock_detected and
                // XX000 "tuple concurrently updated" in particular) leave the
                // underlying Npgsql connection in a Closed or Broken state —
                // Npgsql moves a connection to Broken when the server-side
                // error has aborted the session. Without this guard, the next
                // ExecuteNonQueryAsync on the same cmd throws
                // InvalidOperationException("Connection is not open"), which
                // falls out of the catch filter (not a PostgresException) and
                // surfaces as a hard migration failure — defeating the retry.
                //
                // Repro before this guard: recurring intermittent failure on
                // EventSourcingTests.end_to_end_event_capture_and_fetching_the_stream.
                // query_before_saving(tenancyStyle: Conjoined) in JasperFx/marten
                // PRs #4576, #4578, #4582, #4584 — all hit this exact path.
                await EnsureConnectionOpenAsync(cmd, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    ///     Re-open a <see cref="DbCommand"/>'s connection if a previous
    ///     transient failure has left it in a non-<see cref="ConnectionState.Open"/>
    ///     state. A <see cref="ConnectionState.Broken"/> connection must be
    ///     closed first — <c>OpenAsync</c> on a Broken connection throws. Pure
    ///     over <see cref="DbCommand"/> + <see cref="ConnectionState"/> so the
    ///     reopen rules can be exercised with a fake command in
    ///     <c>PostgresqlMigratorConcurrencyRetryTests</c>.
    /// </summary>
    internal static async Task EnsureConnectionOpenAsync(DbCommand cmd, CancellationToken ct)
    {
        if (cmd.Connection is null) return;
        if (cmd.Connection.State == System.Data.ConnectionState.Open) return;

        if (cmd.Connection.State == System.Data.ConnectionState.Broken)
        {
            await cmd.Connection.CloseAsync().ConfigureAwait(false);
        }

        await cmd.Connection.OpenAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     True for the PostgreSQL SQLSTATEs that signal a transient, retry-safe
    ///     concurrency conflict while applying idempotent migration DDL:
    ///     <list type="bullet">
    ///         <item><c>40001</c> serialization_failure</item>
    ///         <item><c>40P01</c> deadlock_detected</item>
    ///         <item>
    ///             <c>XX000</c> internal_error <b>only</b> when the message is
    ///             "tuple concurrently updated" — two backends updated the same
    ///             system-catalog row (e.g. <c>pg_proc</c> under concurrent
    ///             <c>CREATE OR REPLACE FUNCTION</c>). <c>XX000</c> is a catch-all,
    ///             so the message guard prevents blanket-retrying unrelated
    ///             internal errors.
    ///         </item>
    ///     </list>
    ///     Pure over the two string inputs so it is unit-testable without
    ///     constructing a <see cref="PostgresException" />.
    /// </summary>
    internal static bool IsTransientCatalogConcurrency(string? sqlState, string? messageText)
    {
        return sqlState switch
        {
            PostgresErrorCodes.SerializationFailure => true,
            PostgresErrorCodes.DeadlockDetected => true,
            PostgresErrorCodes.InternalError =>
                messageText is not null
                && messageText.Contains("tuple concurrently updated", StringComparison.OrdinalIgnoreCase),
            _ => false
        };
    }

    /// <summary>
    ///     Clears the connection-string-keyed pool for the supplied connection.
    ///     <para>
    ///     <see cref="PostgresqlDatabase" /> overrides <c>ReleaseConnectionPoolAsync</c> to clear its
    ///     <see cref="NpgsqlDataSource" /> directly, which is more precise, so this is only reached by
    ///     PostgreSQL databases built on <see cref="DatabaseBase{T}" /> with a connection string instead.
    ///     Without it those databases would be the one provider combination that silently released nothing.
    ///     </para>
    /// </summary>
    public override ValueTask ReleaseConnectionPoolAsync(DbConnection connection, CancellationToken ct = default)
    {
        if (connection is NpgsqlConnection npgsql)
        {
            NpgsqlConnection.ClearPool(npgsql);
        }

        return ValueTask.CompletedTask;
    }

    public override bool IsTransientConnectionFailure(Exception exception)
    {
        // Npgsql does not always surface the server's refusal bare. An NpgsqlMultiHostDataSource (which
        // PostgresqlDatabase.CreateConnection explicitly supports) aggregates its per-host failures, and a
        // failure to connect commonly arrives as an outer exception wrapping the PostgresException that
        // actually carries the SQLSTATE. Matching only the outermost exception would leave this predicate --
        // and therefore the whole retry -- inert for exactly the case it exists to handle.
        foreach (var e in ExceptionChain.Flatten(exception))
        {
            if (e is PostgresException pg && IsTransientConnectionFailure(pg.SqlState)) return true;
        }

        return false;
    }

    /// <summary>
    ///     True for the PostgreSQL SQLSTATEs that signal a transient refusal to accept a new connection,
    ///     where the same work retried after a backoff is likely to get through (weasel#356):
    ///     <list type="bullet">
    ///         <item><c>53300</c> too_many_connections — the server is at <c>max_connections</c></item>
    ///         <item><c>53400</c> configuration_limit_exceeded</item>
    ///         <item><c>57P03</c> cannot_connect_now — the server is still starting up</item>
    ///     </list>
    ///     Deliberately excludes the rest of class 53 (insufficient_resources): <c>53100</c> disk_full and
    ///     <c>53200</c> out_of_memory are real conditions that retrying will not clear.
    ///     Pure over the SQLSTATE so it is unit-testable without constructing a
    ///     <see cref="PostgresException" />.
    /// </summary>
    internal static bool IsTransientConnectionFailure(string? sqlState)
    {
        return sqlState is PostgresErrorCodes.TooManyConnections
            or PostgresErrorCodes.ConfigurationLimitExceeded
            or PostgresErrorCodes.CannotConnectNow;
    }

    public override string ToExecuteScriptLine(string scriptName)
    {
        return $"\\i {scriptName}";
    }

    public static string CreateSchemaStatementFor(string schemaName)
    {
        return $"create schema if not exists {PostgresqlProvider.Instance.ToQualifiedName(schemaName)};";
    }

    /// <summary>
    ///     PostgreSQL's implicit system columns. PostgreSQL rejects
    ///     <c>CREATE TABLE</c> with a column named any of these
    ///     ("42701: column name X conflicts with a system column name"), so they
    ///     must be skipped when generating DDL. <c>oid</c> is intentionally
    ///     excluded — it is no longer a system column on ordinary tables (PG 12+)
    ///     and is a valid user column name.
    /// </summary>
    private static readonly HashSet<string> SystemColumns =
        new(StringComparer.OrdinalIgnoreCase) { "tableoid", "xmin", "cmin", "xmax", "cmax", "ctid" };

    public override bool IsSystemColumn(string columnName) => SystemColumns.Contains(columnName);

    public override void AssertValidIdentifier(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new PostgresqlIdentifierInvalidException(name);
        }

        if (name.IndexOf(' ') >= 0)
        {
            throw new PostgresqlIdentifierInvalidException(name);
        }

        if (name.Length < NameDataLength)
        {
            return;
        }

        throw new PostgresqlIdentifierTooLongException(NameDataLength, name);
    }

    public override async Task EnsureDatabaseExistsAsync(DbConnection connection, CancellationToken ct = default)
    {
        var builder = new NpgsqlConnectionStringBuilder(connection.ConnectionString);
        var databaseName = builder.Database;

        if (string.IsNullOrEmpty(databaseName))
        {
            throw new ArgumentException("The connection string does not specify a database name.");
        }

        builder.Database = "postgres";
        await using var adminConn = new NpgsqlConnection(builder.ConnectionString);
        await adminConn.OpenAsync(ct).ConfigureAwait(false);

        if (!await adminConn.DatabaseExists(databaseName, ct).ConfigureAwait(false))
        {
            await new DatabaseSpecification().BuildDatabase(adminConn, databaseName, ct).ConfigureAwait(false);
        }
    }

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
    }

    public DatabaseWithTables CreateDatabase(NpgsqlDataSource dataSource)
    {
        var builder = new NpgsqlConnectionStringBuilder(dataSource.ConnectionString);
        return new DatabaseWithTables(builder.Database ?? "weasel", dataSource);
    }

    public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null)
    {
        if (connection is not NpgsqlConnection)
        {
            throw new ArgumentException("Expected NpgsqlConnection", nameof(connection));
        }

        var dataSource = new NpgsqlDataSourceBuilder(connection.ConnectionString).Build();
        var builder = new NpgsqlConnectionStringBuilder(connection.ConnectionString);
        return new DatabaseWithTables(identifier ?? builder.Database ?? "weasel", dataSource);
    }

    public override string GenerateDeleteAllSql(IReadOnlyList<DbObjectName> tables, bool resetIdentity = true)
    {
        if (tables.Count == 0)
        {
            return string.Empty;
        }

        var sb = new StringBuilder();
        sb.Append("TRUNCATE TABLE ");
        sb.Append(string.Join(", ", tables.Select(t => t.QualifiedName)));

        if (resetIdentity)
        {
            sb.Append(" RESTART IDENTITY");
        }

        sb.Append(" CASCADE;");

        return sb.ToString();
    }

    public override IDatabaseWithTables CreateDatabase(DbDataSource dataSource, string? identifier = null)
    {
        if (dataSource is not NpgsqlDataSource npgsqlDataSource)
        {
            return base.CreateDatabase(dataSource, identifier);
        }

        var builder = new NpgsqlConnectionStringBuilder(dataSource.ConnectionString);
        return new DatabaseWithTables(identifier ?? builder.Database ?? "weasel", npgsqlDataSource);
    }
}
