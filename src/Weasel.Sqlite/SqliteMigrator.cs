using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Text;
using JasperFx;
using JasperFx.Core;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.Sqlite;

public class SqliteMigrator: Migrator
{
    public SqliteMigrator(): base(SqliteProvider.Instance.DefaultDatabaseSchemaName)
    {
    }

    public override IDatabaseProvider Provider => SqliteProvider.Instance;

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
    }

    public override bool MatchesConnection(DbConnection connection)
    {
        return connection is Microsoft.Data.Sqlite.SqliteConnection;
    }

    /// <summary>
    ///     Write out a templated SQL script with all rules
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="writeStep">A continuation to write the inner SQL</param>
    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
    {
        if (IsTransactional)
        {
            writer.WriteLine("BEGIN TRANSACTION;");
            writer.WriteLine();
        }

        writeStep(this, writer);

        if (IsTransactional)
        {
            writer.WriteLine();
            writer.WriteLine("COMMIT;");
        }
    }

    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        // SQLite doesn't support CREATE SCHEMA like PostgreSQL
        // SQLite only uses the "main" schema - no schema creation needed
    }

    public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        // SQLite doesn't support dropping schemas
    }

    protected override async Task executeDelta(
        SchemaMigration migration,
        DbConnection conn,
        AutoCreate autoCreate,
        IMigrationLogger logger,
        CancellationToken ct = default
    )
    {
        var rebuilt = migration.Deltas
            .OfType<Tables.TableDelta>()
            .Where(x => x.CanRebuildInPlace &&
                        (x.RequiresTableRecreation || x.Difference == SchemaPatchDifference.Invalid))
            .Select(x => x.Expected.Identifier)
            .ToArray();

        if (rebuilt.Length == 0)
        {
            await writeDeltasAsync(migration, conn, logger, false, ct).ConfigureAwait(false);
            return;
        }

        var foreignKeysWereOn = await readPragmaFlagAsync(conn, "foreign_keys", ct).ConfigureAwait(false);
        var legacyAlterTableWasOn = await readPragmaFlagAsync(conn, "legacy_alter_table", ct).ConfigureAwait(false);
        Exception? failure = null;

        try
        {
            await setPragmaFlagAsync(conn, "foreign_keys", false, ct).ConfigureAwait(false);
            await setPragmaFlagAsync(conn, "legacy_alter_table", true, ct).ConfigureAwait(false);

            await executeSqlAsync(conn, "BEGIN TRANSACTION;", ct).ConfigureAwait(false);

            try
            {
                await writeDeltasAsync(migration, conn, logger, true, ct).ConfigureAwait(false);

                // Only when enforcement was on to begin with, which is step 10 of SQLite's own
                // rebuild procedure. foreign_key_check reports every violation in the table, not
                // just the ones this rebuild could have caused, and a database that has been running
                // with foreign_keys OFF is allowed to hold dangling rows. Checking unconditionally
                // refuses those migrations and blames the rebuild for rows it never touched.
                if (foreignKeysWereOn)
                {
                    await assertForeignKeysStillResolveAsync(conn, rebuilt, ct).ConfigureAwait(false);
                }

                await executeSqlAsync(conn, "COMMIT;", ct).ConfigureAwait(false);
            }
            catch
            {
                await rollbackQuietlyAsync(conn).ConfigureAwait(false);
                throw;
            }
        }
        catch (Exception e)
        {
            failure = e;
        }

        await restorePragmasAsync(conn, foreignKeysWereOn, legacyAlterTableWasOn, failure).ConfigureAwait(false);

        if (failure != null)
        {
            ExceptionDispatchInfo.Capture(failure).Throw();
        }
    }

    private static async Task restorePragmasAsync(
        DbConnection conn,
        bool foreignKeys,
        bool legacyAlterTable,
        Exception? failure
    )
    {
        bool restored;

        try
        {
            await setPragmaFlagAsync(conn, "legacy_alter_table", legacyAlterTable, CancellationToken.None)
                .ConfigureAwait(false);
            await setPragmaFlagAsync(conn, "foreign_keys", foreignKeys, CancellationToken.None)
                .ConfigureAwait(false);

            restored = await readPragmaFlagAsync(conn, "foreign_keys", CancellationToken.None)
                .ConfigureAwait(false) == foreignKeys;
        }
        catch (DbException e)
        {
            restored = false;
            failure ??= e;
        }

        if (!restored)
        {
            throw new InvalidOperationException(
                $"SQLite foreign key enforcement could not be restored to {(foreignKeys ? "ON" : "OFF")} after a table " +
                "rebuild. PRAGMA foreign_keys is silently ignored inside a transaction, so a transaction is still open " +
                "on this connection and it must not be reused.",
                failure);
        }
    }

    private async Task writeDeltasAsync(
        SchemaMigration migration,
        DbConnection conn,
        IMigrationLogger logger,
        bool failureIsFatal,
        CancellationToken ct
    )
    {
        foreach (var delta in migration.Deltas)
        {
            var writer = new StringWriter();
            WriteUpdate(writer, delta);

            if (writer.ToString().Trim().IsNotEmpty())
            {
                await executeCommand(conn, logger, writer, failureIsFatal, ct).ConfigureAwait(false);
            }
        }

        // Inside writeDeltasAsync rather than after it, so a rebuild's deferred keys are added
        // within the same transaction as the rebuild and roll back with it. failureIsFatal has to
        // carry through for the same reason: swallowing a failure here would let a rebuild reach
        // COMMIT with a key missing.
        var deferred = new StringWriter();
        migration.WriteDeferredForeignKeys(deferred, this);

        if (deferred.ToString().Trim().IsNotEmpty())
        {
            await executeCommand(conn, logger, deferred, failureIsFatal, ct).ConfigureAwait(false);
        }
    }

    private static async Task<bool> readPragmaFlagAsync(DbConnection conn, string pragma, CancellationToken ct)
    {
        var cmd = conn.CreateCommand($"PRAGMA {pragma};");
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) ?? 0L) != 0L;
    }

    private static Task setPragmaFlagAsync(DbConnection conn, string pragma, bool value, CancellationToken ct)
    {
        return executeSqlAsync(conn, $"PRAGMA {pragma} = {(value ? "ON" : "OFF")};", ct);
    }

    private static async Task assertForeignKeysStillResolveAsync(
        DbConnection conn,
        IReadOnlyList<DbObjectName> rebuilt,
        CancellationToken ct
    )
    {
        foreach (var table in rebuilt)
        {
            var schema = SchemaUtils.EscapeLiteral(table.Schema);

            foreach (var name in await referencingTablesAsync(conn, table, ct).ConfigureAwait(false))
            {
                var cmd = conn.CreateCommand(
                    $"SELECT \"table\", \"parent\" FROM pragma_foreign_key_check('{SchemaUtils.EscapeLiteral(name)}', '{schema}') LIMIT 1;");

                await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                if (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    throw new InvalidOperationException(
                        $"Rebuilding table {table.QualifiedName} would leave rows in '{reader.GetString(0)}' referencing " +
                        $"missing rows in '{reader.GetString(1)}'. The migration has been rolled back and the database is unchanged.");
                }
            }
        }
    }

    private static async Task<IReadOnlyList<string>> referencingTablesAsync(
        DbConnection conn,
        DbObjectName table,
        CancellationToken ct
    )
    {
        var schema = SchemaUtils.EscapeLiteral(table.Schema);
        var cmd = conn.CreateCommand(
            $"SELECT DISTINCT m.name FROM {SchemaUtils.QuoteName(table.Schema)}.sqlite_master m, " +
            $"pragma_foreign_key_list(m.name, '{schema}') f " +
            $"WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite/_%' ESCAPE '/' " +
            $"AND lower(f.\"table\") = lower('{SchemaUtils.EscapeLiteral(table.Name)}');");

        var names = new List<string> { table.Name };

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = reader.GetString(0);
            if (!name.EqualsIgnoreCase(table.Name))
            {
                names.Add(name);
            }
        }

        return names;
    }

    private static async Task executeSqlAsync(DbConnection conn, string sql, CancellationToken ct)
    {
        var cmd = conn.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static async Task rollbackQuietlyAsync(DbConnection conn)
    {
        try
        {
            await executeSqlAsync(conn, "ROLLBACK;", CancellationToken.None).ConfigureAwait(false);
        }
        catch (DbException)
        {
        }
    }

    public override string ToExecuteScriptLine(string scriptName)
    {
        // SQLite CLI command to read script file
        return $".read {scriptName}";
    }

    /// <summary>
    ///     The character SQLite delimits identifiers with. <see cref="SchemaUtils.QuoteName" /> does double
    ///     an embedded <c>"</c>, but only quotes at all for reserved keywords, spaces, dashes and leading
    ///     digits -- an ordinary-looking name carrying a quote is written out raw.
    /// </summary>
    private const string UnsafeIdentifierCharacters = "\"";

    /// <summary>
    ///     SQLite itself allows identifiers up to 1073741824 characters, which is not a useful limit; this
    ///     is the practical one Weasel enforces, in line with the other providers.
    /// </summary>
    public int MaxIdentifierLength { get; set; } = 255;

    /// <summary>
    ///     Validates a database object name before it is written into DDL. See
    ///     <see cref="IdentifierValidation" /> for why each rule is here; before weasel#416 this checked
    ///     null/whitespace and length only.
    /// </summary>
    /// <remarks>
    ///     The single quote matters here because SQLite's introspection path interpolates the table name
    ///     into string literals -- <c>WHERE name = '...'</c> against <c>sqlite_master</c> and
    ///     <c>pragma_table_info('...')</c> in <c>Table.FetchExisting</c>.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The name cannot be safely written into DDL.</exception>
    public override void AssertValidIdentifier(string name)
    {
        AssertValidLocalIdentifier(name);

        if (name.Length > MaxIdentifierLength)
        {
            throw new InvalidOperationException(
                $"SQLite identifier '{name}' is too long ({name.Length} characters). Maximum recommended length is {MaxIdentifierLength}.");
        }
    }

    /// <summary>
    ///     The safety half of <see cref="AssertValidIdentifier" />, without the length limit.
    /// </summary>
    /// <remarks>
    ///     A column, primary key or check constraint name is only ever emitted inside its own
    ///     table's DDL and is never addressed by name afterwards, and the delta comparison already
    ///     reads both sides through TruncatedNameIdentifier so a name SQLite truncated still
    ///     matches. Refusing to create one would reject schemas the rest of Weasel handles
    ///     (weasel#485). The safety rules still apply in full.
    /// </remarks>
    public override void AssertValidLocalIdentifier(string name)
    {
        var problem = IdentifierValidation.FindProblem(name, UnsafeIdentifierCharacters);
        if (problem != null)
        {
            throw new InvalidOperationException($"SQLite identifier '{name}' is not valid because {problem}.");
        }
    }

    private static async Task executeCommand(DbConnection conn, IMigrationLogger logger, StringWriter writer,
        bool failureIsFatal, CancellationToken ct = default)
    {
        var cmd = conn.CreateCommand(writer.ToString());
        logger.SchemaChange(cmd.CommandText);

        try
        {
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            if (logger is DefaultMigrationLogger)
            {
                throw;
            }

            logger.OnFailure(cmd, e);

            if (failureIsFatal)
            {
                throw;
            }
        }
    }

    /// <summary>
    ///     No-op for SQLite. SQLite databases are automatically created when the connection is opened.
    /// </summary>
    public override Task EnsureDatabaseExistsAsync(DbConnection connection, CancellationToken ct = default)
    {
        return Task.CompletedTask;
    }

    public override string GenerateDeleteAllSql(IReadOnlyList<DbObjectName> tables, bool resetIdentity = true)
    {
        if (tables.Count == 0)
        {
            return string.Empty;
        }

        var sb = new StringBuilder();
        var names = tables.Select(SqliteObjectName.From).ToList();

        foreach (var table in names)
        {
            sb.AppendLine($"DELETE FROM {qualify(table.Schema, table.Name)};");
        }

        if (resetIdentity)
        {
            foreach (var group in names.GroupBy(x => x.Schema, StringComparer.OrdinalIgnoreCase))
            {
                var literals = string.Join(", ", group.Select(t => $"'{SchemaUtils.EscapeLiteral(t.Name)}'"));
                sb.AppendLine($"DELETE FROM {qualify(group.Key, "sqlite_sequence")} WHERE name IN ({literals});");
            }
        }

        return sb.ToString();
    }

    /// <summary>
    ///     Names the schema even for <c>main</c>, which the rest of Weasel.Sqlite leaves bare. DML
    ///     resolves an unqualified name against <c>temp</c> first, so a temp table silently takes the
    ///     delete meant for the main one; <c>sqlite_sequence</c> is per-database for the same reason.
    ///     See <c>delete_all_data_empties_main_even_when_a_temp_table_shadows_it</c>.
    /// </summary>
    private static string qualify(string schema, string name)
    {
        var owner = schema.IsEmpty() ? SqliteProvider.Instance.DefaultDatabaseSchemaName : schema;
        return $"{SchemaUtils.QuoteName(owner)}.{SchemaUtils.QuoteName(name)}";
    }

    public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null)
    {
        if (connection is not Microsoft.Data.Sqlite.SqliteConnection)
        {
            throw new ArgumentException("Expected SqliteConnection", nameof(connection));
        }

        var builder = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder(connection.ConnectionString);
        return new DatabaseWithTables(identifier ?? builder.DataSource ?? "sqlite", connection.ConnectionString);
    }
}
