using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Text;
using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using JasperFx.Core;
using Weasel.Core;
using Weasel.Core.Migrations;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Firebird;

public class FirebirdMigrator: Migrator
{
    public FirebirdMigrator(): base(FirebirdProvider.Instance.DefaultDatabaseSchemaName)
    {
    }

    public override IDatabaseProvider Provider => FirebirdProvider.Instance;

    /// <inheritdoc />
    public override string DefaultJsonColumnType => FirebirdProvider.JsonColumnType;

    public override bool MatchesConnection(DbConnection connection)
    {
        return connection is FbConnection;
    }

    /// <summary>
    ///     The longest identifier Weasel will write. 31 by default, which is Firebird 3's limit -- in
    ///     bytes, so at 31 or less the UTF-8 length is checked as well as the character count. Set it to
    ///     63 for a database only Firebird 4 and later will open.
    /// </summary>
    /// <remarks>
    ///     Not detected from the server, because names are checked before a connection is opened. And
    ///     never used to truncate: Firebird refuses an over-long name rather than shortening it, and a
    ///     truncation scheme could never change later without renaming constraints.
    /// </remarks>
    public int MaxIdentifierLength { get; set; } = 31;

    /// <summary>
    ///     How long one DDL statement waits for a lock held by another transaction before it fails.
    ///     Each statement runs in its own <c>WAIT</c> transaction: FirebirdClient's default is
    ///     <c>NO WAIT</c>, under which DDL beside any uncommitted DML fails at once and two appliers
    ///     racing the same guarded statement can both lose.
    /// </summary>
    public TimeSpan LockTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    ///     How many times a guarded statement is run before a catalog conflict or a lock timeout is
    ///     reported. The guard makes a re-run a no-op once a concurrent applier has created the object.
    /// </summary>
    public int MaxGuardedStatementAttempts { get; set; } = 5;

    /// <summary>
    ///     The characters that are unsafe in a Firebird identifier beyond the universal ones: the
    ///     <c>"</c> Firebird delimits identifiers with, and the <c>^</c> that ends a PSQL statement in
    ///     an isql script.
    /// </summary>
    private const string UnsafeIdentifierCharacters = "\"^";

    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
    {
        var rendered = new StringWriter();
        writeStep(this, rendered);

        FirebirdScript.WriteIsqlScript(writer, rendered.ToString());
    }

    /// <summary>
    ///     Firebird 3, 4 and 5 have no schemas, so there is nothing to create: the default schema writes
    ///     nothing, and any other is refused before a statement is written.
    /// </summary>
    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames)
        {
            FirebirdObjectName.AssertDefaultSchema(schemaName);
        }
    }

    /// <inheritdoc cref="WriteSchemaCreationSql" />
    public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames)
        {
            FirebirdObjectName.AssertDefaultSchema(schemaName);
        }
    }

    public override string ToExecuteScriptLine(string scriptName)
    {
        return $"INPUT {FirebirdScript.Literal(scriptName)};";
    }

    /// <summary>
    ///     Validates a database object name before it is written into DDL: the safety rules of
    ///     <see cref="IdentifierValidation" />, and the length limit.
    /// </summary>
    /// <exception cref="InvalidOperationException">The name cannot be safely written into DDL.</exception>
    public override void AssertValidIdentifier(string name)
    {
        AssertValidLocalIdentifier(name);
    }

    /// <summary>
    ///     Unlike the other providers, the length is checked here too: a column, primary key or
    ///     constraint name over the limit is refused by Firebird rather than truncated, so there is no
    ///     truncated name for a later comparison to reconcile.
    /// </summary>
    public override void AssertValidLocalIdentifier(string name)
    {
        var problem = IdentifierValidation.FindProblem(name, UnsafeIdentifierCharacters);
        if (problem != null)
        {
            throw new InvalidOperationException($"Firebird identifier '{name}' is not valid because {problem}.");
        }

        AssertFits(name, null);
    }

    /// <summary>
    ///     Refuse a name longer than <see cref="MaxIdentifierLength" />, saying what it names and what to
    ///     do about it.
    /// </summary>
    internal void AssertFits(string name, string? remedy, string? names = null)
        => assertFits(name, MaxIdentifierLength, remedy, names);

    /// <summary>
    ///     Refuse a name the catalog of a Firebird <paramref name="version" /> server cannot hold, before
    ///     it is bound as a parameter against a catalog column: FirebirdClient would fail the query with
    ///     "string truncation" (335544321), which says nothing about the name. A name the catalog holds is
    ///     left to <see cref="MaxIdentifierLength" />, which is checked when DDL is written. Nothing is
    ///     checked when the version is unknown.
    /// </summary>
    internal static void AssertCatalogCanHold(string name, FirebirdServerVersion? version, string names)
    {
        if (version is { } server)
        {
            assertFits(name, server.MaxIdentifierLength,
                $"Firebird {server} holds no longer name in its catalog, so nothing by this name can exist. Use a shorter one.",
                names);
        }
    }

    private static void assertFits(string name, int limit, string? remedy, string? names)
    {
        var inBytes = limit <= 31;
        var length = inBytes ? Encoding.UTF8.GetByteCount(name) : name.Length;

        if (length <= limit)
        {
            return;
        }

        var unit = inBytes ? "bytes" : "characters";
        throw new InvalidOperationException(
            $"Firebird identifier '{name}'{(names == null ? "" : $", {names},")} is {length} {unit}, over the {limit}-{unit[..^1]} limit. "
            + "Firebird refuses a longer name rather than truncating it, so Weasel does too. "
            + (remedy ?? $"Firebird 3 allows 31 bytes; for a database only Firebird 4 or later opens, set {nameof(FirebirdMigrator)}.{nameof(MaxIdentifierLength)} to 63."));
    }

    /// <summary>
    ///     Introspection goes through <see cref="FirebirdDbCommandBuilder" />, which splits a batch into
    ///     one command per statement, because Firebird executes one statement per command.
    /// </summary>
    public override DbCommandBuilder CreateCommandBuilder(DbConnection conn)
        => conn is FbConnection firebird ? new FirebirdDbCommandBuilder(firebird) : base.CreateCommandBuilder(conn);

    /// <summary>
    ///     The statements of rendered DDL, one per command, because Firebird executes one statement per
    ///     command -- the default of one batch sends a whole rollback script as a single command, which
    ///     fails on its second statement.
    /// </summary>
    public override IReadOnlyList<string> SplitIntoBatches(string sql) => FirebirdScript.Split(sql);

    /// <summary>
    ///     A rollback runs the way an apply does: one statement per command, each in its own <c>WAIT</c>
    ///     transaction, rolled back if it fails.
    /// </summary>
    protected override async Task executeRollback(SchemaMigration migration, DbConnection conn, string sql,
        CancellationToken ct = default)
    {
        if (conn is not FbConnection firebird)
        {
            throw new ArgumentException("Expected FbConnection", nameof(conn));
        }

        await ExecuteScriptAsync(firebird, sql, ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     Last applied, first undone. Firebird refuses to drop a table, view or procedure that a view,
    ///     procedure or trigger still uses, where PostgreSQL cascades and MySQL and Oracle leave the
    ///     dependant invalid, so undoing a migration in the order it was applied could not drop a table
    ///     before the view over it.
    /// </summary>
    protected override IEnumerable<ISchemaObjectDelta> OrderRollbacks(IReadOnlyList<ISchemaObjectDelta> deltas)
        => Enumerable.Reverse(deltas);

    /// <summary>
    ///     Firebird before 6 has no schemas, so the fingerprint table is named alone rather than as
    ///     <c>PUBLIC.weasel_schema_fingerprints</c>, which the server would read as a syntax error.
    /// </summary>
    protected override string FingerprintTableName(string tableName) => tableName;

    /// <summary>
    ///     <c>RDB$DB_KEY</c> and <c>RDB$RECORD_VERSION</c>, the pseudo-columns every Firebird table has
    ///     and no <c>CREATE TABLE</c> may declare.
    /// </summary>
    public override bool IsSystemColumn(string columnName)
        => columnName.Equals("RDB$DB_KEY", StringComparison.OrdinalIgnoreCase)
           || columnName.Equals("RDB$RECORD_VERSION", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    ///     The Firebird errors that mean a statement was refused for want of privilege:
    ///     <c>no permission for … access</c> (335544352), which a denied read or write -- and an
    ///     <c>ALTER</c> or <c>DROP</c> of somebody else's object -- carries, and the "no permission for
    ///     CREATE" that a denied <c>CREATE</c> carries instead, numbered 335545094 on Firebird 3 and
    ///     335545264 on 4 and 5.
    /// </summary>
    private static readonly int[] PermissionErrorNumbers = [335544352, 335545094, 335545264];

    /// <summary>
    ///     Is this Firebird error number a permission refusal? A number rather than an exception, so the
    ///     set can be asserted without manufacturing an <see cref="FbException" />, which has no public
    ///     constructor.
    /// </summary>
    internal static bool IsPermissionErrorNumber(int errorNumber)
        => Array.IndexOf(PermissionErrorNumbers, errorNumber) >= 0;

    /// <summary>
    ///     A permission refusal is rarely the exception's own code -- a denied <c>ALTER</c> is
    ///     "unsuccessful metadata update" first -- so every number in <see cref="FbException.Errors" />
    ///     is looked at.
    /// </summary>
    public override bool IsInsufficientPrivilege(Exception exception)
        => exception is FbException firebird && HasErrorNumber(firebird, PermissionErrorNumbers);

    /// <summary>
    ///     The Firebird errors that mean the server could not be reached or dropped the attachment,
    ///     and a retry after a backoff may well succeed: the network is unreachable (335544721), the
    ///     connection was lost (335544727, with 335544726 read errors) -- which is also what a pooled
    ///     connection to a restarted server reports -- or the attachment was shut down (335544856). A
    ///     client-side <see cref="TimeoutException" /> counts too.
    /// </summary>
    /// <remarks>
    ///     The SQLSTATE class is deliberately not used: a missing database file is <c>08001</c>, the
    ///     same class as a server that is down, and retrying it only delays the real error.
    /// </remarks>
    public override bool IsTransientConnectionFailure(Exception exception)
    {
        foreach (var e in ExceptionChain.Flatten(exception))
        {
            if (e is TimeoutException)
            {
                return true;
            }

            if (e is FbException firebird && IsTransientConnectionError(firebird))
            {
                return true;
            }
        }

        return false;
    }

    internal static bool IsTransientConnectionError(FbException exception)
        => HasErrorNumber(exception, TransientConnectionErrorNumbers);

    private static readonly int[] TransientConnectionErrorNumbers = [335544721, 335544727, 335544726, 335544856];

    /// <summary>
    ///     Clear FirebirdClient's pool for this connection string. Firebird needs it more than most: a
    ///     pooled connection is not validated when it is handed out, so after a server restart the pool is
    ///     full of dead attachments, and an idle pooled attachment still holds the tables it touched.
    /// </summary>
    public override ValueTask ReleaseConnectionPoolAsync(DbConnection connection, CancellationToken ct = default)
    {
        if (connection is FbConnection firebird)
        {
            FbConnection.ClearPool(firebird);
        }

        return ValueTask.CompletedTask;
    }

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
    }

    /// <summary>
    ///     A Firebird sequence, for a model built outside Weasel -- EF Core's HiLo and <c>HasSequence</c>.
    /// </summary>
    public override SequenceBase CreateSequence(DbObjectName identifier)
    {
        return new Sequence(identifier);
    }

    public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null)
    {
        if (connection is not FbConnection)
        {
            throw new ArgumentException("Expected FbConnection", nameof(connection));
        }

        var builder = new FbConnectionStringBuilder(connection.ConnectionString);
        var name = identifier ?? Path.GetFileNameWithoutExtension(builder.Database);

        return new DatabaseWithTables(name.IsEmpty() ? "weasel" : name, connection.ConnectionString);
    }

    /// <summary>
    ///     The page size <see cref="EnsureDatabaseExistsAsync" /> creates a database with. 16384, because a
    ///     key over a few long <c>VARCHAR</c> columns in a UTF8 database does not fit a smaller page:
    ///     Quartz's schema fails at 8192.
    /// </summary>
    public int NewDatabasePageSize { get; set; } = 16384;

    /// <summary>
    ///     Create the database file the connection string names, unless it can already be opened.
    /// </summary>
    /// <remarks>
    ///     The database is opened first and created only when that fails because the file is missing
    ///     (weasel#647's lesson, from MySQL): a user that may connect but not create databases -- or a
    ///     server that only allows existing files -- then never reaches the create. A create that loses a
    ///     race with another process, or finds the file there after all, counts as success. The
    ///     connection string's character set becomes the database's default.
    /// </remarks>
    public override async Task EnsureDatabaseExistsAsync(DbConnection connection, CancellationToken ct = default)
    {
        var connectionString = connection.ConnectionString;

        try
        {
            await using var probe = new FbConnection(connectionString);
            await probe.OpenAsync(ct).ConfigureAwait(false);
            return;
        }
        catch (FbException e) when (IsMissingDatabase(e))
        {
            // Fall through to create it.
        }

        try
        {
            await FbConnection.CreateDatabaseAsync(connectionString, NewDatabasePageSize, true, false, ct)
                .ConfigureAwait(false);
        }
        catch (FbException e) when (IsExistingDatabase(e))
        {
            // Somebody else created it between the open and the create.
        }
    }

    /// <summary>
    ///     "I/O error during open" with "Error while trying to open file": the file does not exist. The
    ///     same SQLSTATE class (08) covers a server that is down, so the numbers are what decide.
    /// </summary>
    internal static bool IsMissingDatabase(FbException exception)
        => exception.ErrorCode == 335544344 && HasErrorNumber(exception, 335544734);

    /// <summary>
    ///     The two ways a create finds the database already there: the file exists (335544344 with
    ///     335544733), or a concurrent create won (335544351 with 335544453).
    /// </summary>
    internal static bool IsExistingDatabase(FbException exception)
        => (exception.ErrorCode == 335544344 && HasErrorNumber(exception, 335544733))
           || (exception.ErrorCode == 335544351 && HasErrorNumber(exception, 335544453));

    /// <summary>
    ///     Apply a migration one statement at a time, each in its own transaction.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Everything is rendered before anything runs, deferred foreign keys included, so every
    ///         refusal -- a schema Firebird does not have, an index with mixed directions, a derived
    ///         name over the limit -- fires before the first statement.
    ///     </para>
    ///     <para>
    ///         Each statement then runs in its own <c>WAIT</c> transaction with a lock timeout, and is
    ///         rolled back whenever executing or committing it throws: a failed commit otherwise leaves the
    ///         transaction open and blocks every later statement on the attachment. Under
    ///         FirebirdClient's default <c>NO WAIT</c>, DDL beside any uncommitted DML fails at once, and
    ///         two appliers racing a guarded <c>CREATE INDEX</c> or <c>ADD CONSTRAINT</c> can both lose;
    ///         under <c>WAIT</c> with rollback there was one winner in every one of 540 measured races.
    ///     </para>
    ///     <para>
    ///         A guarded statement that fails with a catalog conflict or a lock timeout is run again, up to
    ///         <see cref="MaxGuardedStatementAttempts" /> times: the guard makes the re-run a no-op once a
    ///         racer has created the object.
    ///     </para>
    /// </remarks>
    protected override async Task executeDelta(
        SchemaMigration migration,
        DbConnection conn,
        AutoCreate autoCreate,
        IMigrationLogger logger,
        CancellationToken ct = default
    )
    {
        var statements = RenderStatements(migration);
        AssertServerSupports(migration, FirebirdServerVersion.Of(conn));

        foreach (var statement in statements)
        {
            await executeStatementAsync(conn, statement, logger, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Refuse, before anything runs, what the model can say but this server cannot do: a partial
    ///     index on Firebird 3 or 4, where the <c>WHERE</c> is a syntax error that would otherwise stop the
    ///     migration halfway through. DDL is written without knowing the server, so this is where the
    ///     version is consulted.
    /// </summary>
    internal static void AssertServerSupports(SchemaMigration migration, FirebirdServerVersion? version)
    {
        if (version is not { SupportsPartialIndexes: false })
        {
            return;
        }

        var partial = migration.Deltas
            .OfType<Tables.TableDelta>()
            .SelectMany(indexesCreatedBy)
            .Where(x => x.Index.Predicate.IsNotEmpty())
            .Select(x => $"{x.Index.Name} on {x.Table}")
            .ToArray();

        if (partial.Any())
        {
            throw new NotSupportedException(
                $"Partial indexes need Firebird 5, and this server is Firebird {version}: {partial.Join(", ")}. "
                + "Drop the Predicate, or apply this migration to a Firebird 5 database.");
        }
    }

    private static IEnumerable<(DbObjectName Table, Tables.IndexDefinition Index)> indexesCreatedBy(Tables.TableDelta delta)
    {
        IEnumerable<Tables.IndexDefinition> indexes = delta.Difference switch
        {
            SchemaPatchDifference.Create or SchemaPatchDifference.Invalid => delta.Expected.Indexes,
            SchemaPatchDifference.Update => delta.Indexes.Missing.Concat(delta.Indexes.Different.Select(x => x.Expected)),
            _ => []
        };

        return indexes.Select(x => (delta.Expected.Identifier, x));
    }

    /// <summary>
    ///     Every statement <paramref name="migration" /> would run, in order, rendered before any of it
    ///     runs.
    /// </summary>
    internal IReadOnlyList<string> RenderStatements(SchemaMigration migration)
    {
        WriteSchemaCreationSql(migration.Schemas, TextWriter.Null);

        var statements = new List<string>();

        foreach (var delta in migration.Deltas)
        {
            var writer = new StringWriter();
            WriteUpdate(writer, delta);
            statements.AddRange(FirebirdScript.Split(writer.ToString()));
        }

        var deferred = new StringWriter();
        migration.WriteDeferredForeignKeys(deferred, this);
        statements.AddRange(FirebirdScript.Split(deferred.ToString()));

        return statements;
    }

    private async Task executeStatementAsync(DbConnection conn, string sql, IMigrationLogger logger,
        CancellationToken ct)
    {
        if (conn is not FbConnection firebird)
        {
            throw new ArgumentException("Expected FbConnection", nameof(conn));
        }

        if (FirebirdScript.IsCommit(sql))
        {
            // Each statement is committed on its own, so a COMMIT in a hand-written script has
            // nothing left to do.
            return;
        }

        // A hand-written script may put a comment before the block, and Split keeps it.
        var guarded = FirebirdScript.IsExecuteBlock(sql);
        logger.SchemaChange(sql);

        for (var attempt = 1;; attempt++)
        {
            var failure = await TryExecuteInOwnTransactionAsync(firebird, sql, ct).ConfigureAwait(false);
            if (failure == null)
            {
                return;
            }

            if (guarded && attempt < MaxGuardedStatementAttempts && IsCatalogConflict(failure)
                && !IsInsufficientPrivilege(failure))
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50 * attempt + Random.Shared.Next(50)), ct)
                    .ConfigureAwait(false);
                continue;
            }

            var translated = TranslateMigrationFailure(conn, sql, failure);

            if (logger is DefaultMigrationLogger)
            {
                // Rethrown with the stack it was caught with, as a bare throw would have kept it.
                ExceptionDispatchInfo.Capture(translated).Throw();
            }

            logger.OnFailure(firebird.CreateCommand(sql), translated);
            return;
        }
    }

    /// <summary>
    ///     Run rendered DDL the way a migration runs it: split into statements, each in a <c>WAIT</c>
    ///     transaction of its own, guarded statements retried on a catalog conflict.
    /// </summary>
    public async Task ExecuteScriptAsync(FbConnection conn, string script, CancellationToken ct = default)
    {
        foreach (var statement in FirebirdScript.Split(script))
        {
            await executeStatementAsync(conn, statement, new DefaultMigrationLogger(TextWriter.Null), ct)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Run one statement in a <c>WAIT</c> transaction of its own and commit it, rolling back if
    ///     either step throws. Returns the failure rather than throwing it, so the caller can decide
    ///     whether to run the statement again.
    /// </summary>
    internal async Task<Exception?> TryExecuteInOwnTransactionAsync(FbConnection conn, string sql,
        CancellationToken ct)
    {
        var transaction = await conn.BeginTransactionAsync(DdlTransactionOptions(), ct).ConfigureAwait(false);

        try
        {
            await using (var command = new FbCommand(sql, conn, transaction))
            {
                await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return null;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // A failed commit leaves the transaction open, and an open transaction on this attachment
            // blocks every retry until it detaches.
            try
            {
                await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Rolled back already, or the attachment is gone; the original failure is the one to report.
            }

            return e;
        }
        finally
        {
            await transaction.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     The transaction every DDL statement runs in: read committed, record versions, and
    ///     <c>WAIT</c> with <see cref="LockTimeout" />.
    /// </summary>
    internal FbTransactionOptions DdlTransactionOptions() => new()
    {
        TransactionBehavior = FbTransactionBehavior.Wait | FbTransactionBehavior.ReadCommitted
                                                         | FbTransactionBehavior.RecVersion,
        WaitTimeout = LockTimeout
    };

    /// <summary>
    ///     The Firebird errors a racing applier produces: a unique key violation in the catalog
    ///     (335544665), a lock conflict at commit (SQLSTATE 40001, which carries 335544345) and a lock
    ///     timeout under <c>WAIT</c> (335544510).
    /// </summary>
    /// <remarks>
    ///     <c>unsuccessful metadata update</c> (335544351) is not one of them: it heads every failed DDL
    ///     statement, a name already taken or a column that does not exist as much as a lost race, so on
    ///     its own it would retry a failure that can only happen again.
    /// </remarks>
    internal static bool IsCatalogConflict(Exception exception)
    {
        foreach (var e in ExceptionChain.Flatten(exception))
        {
            if (e is not FbException firebird)
            {
                continue;
            }

            if (firebird.SQLSTATE == "40001" || HasErrorNumber(firebird, 335544665, 335544510, 335544345))
            {
                return true;
            }
        }

        return false;
    }

    internal static bool HasErrorNumber(FbException exception, params int[] numbers)
    {
        if (Array.IndexOf(numbers, exception.ErrorCode) >= 0)
        {
            return true;
        }

        foreach (FbError error in exception.Errors)
        {
            if (Array.IndexOf(numbers, error.Number) >= 0)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    ///     One <c>EXECUTE BLOCK</c>, because <c>DatabaseCleaner</c> runs the result as a single command.
    ///     The tables arrive children first and are emptied in that order.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Each table is looked up by its exact name, which is how a case-preserved one -- an EF Core
    ///         model's, say -- is stored, and only when there is no such table by the upper-case name
    ///         an undelimited one is folded to. It is never both: emptying <c>"Blogs"</c> leaves a
    ///         <c>BLOGS</c> beside it alone.
    ///     </para>
    ///     <para>
    ///         Identity columns are restarted so the next value is 1. Firebird 3 makes the next value one
    ///         past the value a column restarts with, and Firebird 4 and later make it the value itself,
    ///         so the block asks the engine which it is. No autonomous transaction: the restart would then
    ///         wait on the deletes, which are not committed yet.
    ///     </para>
    /// </remarks>
    public override string GenerateDeleteAllSql(IReadOnlyList<DbObjectName> tables, bool resetIdentity = true)
    {
        if (tables.Count == 0)
        {
            return string.Empty;
        }

        var builder = new StringBuilder();
        builder.Append("EXECUTE BLOCK AS\n");
        builder.Append("  DECLARE VARIABLE relation_name TYPE OF COLUMN RDB$RELATION_FIELDS.RDB$RELATION_NAME;\n");
        if (resetIdentity)
        {
            builder.Append("  DECLARE VARIABLE field_name TYPE OF COLUMN RDB$RELATION_FIELDS.RDB$FIELD_NAME;\n");
            builder.Append("  DECLARE VARIABLE restart_with INTEGER;\n");
        }

        builder.Append("BEGIN\n");

        foreach (var table in tables)
        {
            FirebirdObjectName.AssertDefaultSchema(table.Schema, $"table {table.Name}");

            builder.Append("  FOR SELECT RDB$RELATION_NAME FROM RDB$RELATIONS\n");
            builder.Append($"      WHERE {relationIs(table.Name)} AND RDB$VIEW_BLR IS NULL\n");
            builder.Append("      INTO :relation_name\n");
            builder.Append("  DO\n");
            builder.Append("    EXECUTE STATEMENT 'DELETE FROM \"' || REPLACE(TRIM(relation_name), '\"', '\"\"') || '\"';\n");
        }

        if (resetIdentity)
        {
            var names = tables.Select(x => relationIs(x.Name)).Join(" OR ");

            builder.Append("  restart_with = IIF(rdb$get_context('SYSTEM', 'ENGINE_VERSION') STARTING WITH '3.', 0, 1);\n");
            builder.Append("  FOR SELECT RDB$RELATION_NAME, RDB$FIELD_NAME FROM RDB$RELATION_FIELDS\n");
            builder.Append($"      WHERE RDB$GENERATOR_NAME IS NOT NULL AND ({names})\n");
            builder.Append("      INTO :relation_name, :field_name\n");
            builder.Append("  DO\n");
            builder.Append("    EXECUTE STATEMENT 'ALTER TABLE \"' || REPLACE(TRIM(relation_name), '\"', '\"\"') || '\" ALTER \"' || REPLACE(TRIM(field_name), '\"', '\"\"') || '\" RESTART WITH ' || restart_with;\n");
        }

        builder.Append("END");

        return builder.ToString();
    }

    /// <summary>
    ///     <c>RDB$RELATION_NAME</c> is the table <paramref name="name" /> means: the exact name, or failing
    ///     a table by that name, the upper-case one an undelimited name is folded to.
    /// </summary>
    private static string relationIs(string name)
    {
        var exact = FirebirdScript.Literal(name);
        var folded = SchemaUtils.CatalogName(name);
        if (folded == name)
        {
            return $"RDB$RELATION_NAME = {exact}";
        }

        return $"(RDB$RELATION_NAME = {exact} OR (RDB$RELATION_NAME = {FirebirdScript.Literal(folded)} AND NOT EXISTS "
               + $"(SELECT 1 FROM RDB$RELATIONS t WHERE t.RDB$RELATION_NAME = {exact} AND t.RDB$VIEW_BLR IS NULL)))";
    }
}
