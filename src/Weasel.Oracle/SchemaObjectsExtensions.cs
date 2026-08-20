using System.Data;
using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;

namespace Weasel.Oracle;

public static class SchemaObjectsExtensions
{
    internal static string ToIndexName(this DbObjectName name, string prefix, params string[] columnNames)
    {
        return $"{prefix}_{name.Name}_{columnNames.Join("_")}";
    }

    public static async Task ApplyChangesAsync(
        this ISchemaObject schemaObject,
        OracleConnection conn,
        CancellationToken ct = default
    )
    {
        // Oracle doesn't support multiple result sets, so use the Oracle-specific method
        var migration = await DetermineOracleMigrationAsync(conn, ct, schemaObject).ConfigureAwait(false);

        await new OracleMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct)
            .ConfigureAwait(false);
    }

    public static async Task Drop(this ISchemaObject schemaObject, OracleConnection conn, CancellationToken ct = default)
    {
        var writer = new StringWriter();
        schemaObject.WriteDropStatement(new OracleMigrator(), writer);

        await ExecuteOracleSqlAsync(conn, writer.ToString(), ct).ConfigureAwait(false);
    }

    public static async Task CreateAsync(this ISchemaObject schemaObject, OracleConnection conn, CancellationToken ct = default)
    {
        var writer = new StringWriter();
        schemaObject.WriteCreateStatement(new OracleMigrator(), writer);

        await ExecuteOracleSqlAsync(conn, writer.ToString(), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes SQL on Oracle, handling "/" statement separators.
    /// Oracle can only execute one statement at a time, so we split by "/" and execute each separately.
    /// </summary>
    private static async Task ExecuteOracleSqlAsync(OracleConnection conn, string sql, CancellationToken ct = default)
    {
        var statements = sql.Split(new[] { "\n/\n", "\n/", "/\n" }, StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .ToArray();

        foreach (var statement in statements)
        {
            await conn.CreateCommand(statement).ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    public static async Task EnsureSchemaExists(this OracleConnection conn, string schemaName,
        CancellationToken cancellation = default)
    {
        var shouldClose = false;
        if (conn.State != ConnectionState.Open)
        {
            shouldClose = true;
            await conn.OpenAsync(cancellation).ConfigureAwait(false);
        }

        try
        {
            var sql = OracleMigrator.CreateSchemaStatementFor(schemaName);

            await conn
                .CreateCommand(sql)
                .ExecuteNonQueryAsync(cancellation).ConfigureAwait(false);
        }
        finally
        {
            if (shouldClose)
            {
                await conn.CloseAsync().ConfigureAwait(false);
            }
        }
    }

    public static Task<IReadOnlyList<string?>> ActiveSchemaNamesAsync(
        this OracleConnection conn,
        CancellationToken ct = default
    )
    {
        return conn.CreateCommand("SELECT username FROM all_users ORDER BY username")
            .FetchListAsync<string>(cancellation: ct);
    }

    /// <summary>
    ///     Empty an Oracle schema of every object Weasel can create and every object type that
    ///     blocks a clean teardown if it is left behind.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <strong>It does not drop the schema itself.</strong> An Oracle schema is a user, and
    ///         the only statement that drops one is <c>DROP USER … CASCADE</c> — which a session
    ///         cannot run against its own user (ORA-01940: cannot drop a user that is currently
    ///         connected), which is exactly how Weasel and its tests use this. So the name is kept
    ///         for symmetry with the other four providers and the behaviour is stated here instead
    ///         (weasel#465).
    ///     </para>
    ///     <para>
    ///         PostgreSQL and MySQL delegate teardown to the server's own cascade and cannot fall
    ///         behind. This one enumerates, so it carries a standing cost: <em>every new creatable
    ///         object type has to be added here too.</em> SQL Server's teardown was silently broken
    ///         for views for exactly as long as nothing could create one (weasel#464).
    ///     </para>
    ///     <para>
    ///         Order matters. Triggers first, because a trigger can be owned here while firing on a
    ///         table elsewhere. Views ahead of tables, because <c>DROP TABLE … CASCADE
    ///         CONSTRAINTS</c> <em>invalidates</em> a dependent view rather than dropping it.
    ///         Materialized views ahead of tables for the same reason, and their container tables
    ///         are excluded from the table sweep — Oracle lists them in <c>all_tables</c>, and
    ///         dropping one directly fails. Types last, because a table column can be declared with
    ///         one.
    ///     </para>
    /// </remarks>
    public static async Task DropSchemaAsync(this OracleConnection conn, string schemaName, CancellationToken ct = default)
    {
        var upperSchema = SchemaUtils.EscapeLiteral(schemaName.ToUpperInvariant());

        async Task<IReadOnlyList<string?>> fetchAsync(string sql)
            => await conn.CreateCommand(sql).FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        var triggers = await fetchAsync(
            $"SELECT trigger_name FROM all_triggers WHERE owner = '{upperSchema}'").ConfigureAwait(false);

        var packages = await fetchAsync(
            $"SELECT object_name FROM all_objects WHERE owner = '{upperSchema}' AND object_type = 'PACKAGE'")
            .ConfigureAwait(false);

        var procedures = await fetchAsync(
            $"SELECT object_name FROM all_objects WHERE owner = '{upperSchema}' AND object_type = 'PROCEDURE'")
            .ConfigureAwait(false);

        var functions = await fetchAsync(
            $"SELECT object_name FROM all_objects WHERE owner = '{upperSchema}' AND object_type = 'FUNCTION'")
            .ConfigureAwait(false);

        var views = await fetchAsync(
            $"SELECT view_name FROM all_views WHERE owner = '{upperSchema}'").ConfigureAwait(false);

        var materializedViews = await fetchAsync(
            $"SELECT mview_name FROM all_mviews WHERE owner = '{upperSchema}'").ConfigureAwait(false);

        // Oracle lists a materialized view's container table and a nested table's storage table in
        // all_tables, and DROP TABLE against either one fails (ORA-12083 / ORA-22913). The mview
        // drop and the parent table drop take them.
        var tables = await fetchAsync(
            $"""
             SELECT table_name FROM all_tables
             WHERE owner = '{upperSchema}'
               AND nested = 'NO'
               AND table_name NOT IN (SELECT mview_name FROM all_mviews WHERE owner = '{upperSchema}')
             """).ConfigureAwait(false);

        var sequences = await fetchAsync(
            $"SELECT sequence_name FROM all_sequences WHERE sequence_owner = '{upperSchema}'").ConfigureAwait(false);

        var synonyms = await fetchAsync(
            $"SELECT synonym_name FROM all_synonyms WHERE owner = '{upperSchema}'").ConfigureAwait(false);

        var types = await fetchAsync(
            $"SELECT type_name FROM all_types WHERE owner = '{upperSchema}'").ConfigureAwait(false);

        var schema = SchemaUtils.QuoteName(schemaName);
        string Qualified(string? name) => $"{schema}.{SchemaUtils.QuoteName(name!)}";

        var drops = new List<string>();
        drops.AddRange(triggers.Select(name => $"DROP TRIGGER {Qualified(name)}"));
        drops.AddRange(packages.Select(name => $"DROP PACKAGE {Qualified(name)}"));
        drops.AddRange(procedures.Select(name => $"DROP PROCEDURE {Qualified(name)}"));
        drops.AddRange(functions.Select(name => $"DROP FUNCTION {Qualified(name)}"));
        drops.AddRange(views.Select(name => $"DROP VIEW {Qualified(name)}"));
        drops.AddRange(materializedViews.Select(name => $"DROP MATERIALIZED VIEW {Qualified(name)}"));
        drops.AddRange(tables.Select(name => $"DROP TABLE {Qualified(name)} CASCADE CONSTRAINTS"));
        drops.AddRange(sequences.Select(name => $"DROP SEQUENCE {Qualified(name)}"));
        drops.AddRange(synonyms.Select(name => $"DROP SYNONYM {Qualified(name)}"));
        // FORCE so a type still referenced by something this sweep could not see does not stop the
        // teardown; the reference is left invalid rather than the schema left dirty.
        drops.AddRange(types.Select(name => $"DROP TYPE {Qualified(name)} FORCE"));

        foreach (var drop in drops)
        {
            try
            {
                await conn.CreateCommand(drop).ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (OracleException ex) when (IsAlreadyGone(ex))
            {
                // Something else dropped it first, or it went with its parent -- a trigger with its
                // table, a package body with its package. Concurrent test execution hits this too.
            }
        }
    }

    /// <summary>
    ///     The "you asked me to drop something that is not there" family. Every one of these means
    ///     the teardown's goal is already met for that object, so swallowing them is safe; anything
    ///     else is a real failure and propagates.
    /// </summary>
    private static bool IsAlreadyGone(OracleException ex)
        => ex.Number switch
        {
            942 => true,   // ORA-00942: table or view does not exist
            1434 => true,  // ORA-01434: private synonym to be dropped does not exist
            2289 => true,  // ORA-02289: sequence does not exist
            4043 => true,  // ORA-04043: object does not exist
            4080 => true,  // ORA-04080: trigger does not exist
            12003 => true, // ORA-12003: materialized view does not exist
            _ => false
        };

    public static Task CreateSchemaAsync(this OracleConnection conn, string schemaName, CancellationToken ct = default)
    {
        return conn.CreateCommand(OracleMigrator.CreateSchemaStatementFor(schemaName)).ExecuteNonQueryAsync(ct);
    }

    public static async Task ResetSchemaAsync(this OracleConnection conn, string schemaName,
        CancellationToken ct = default)
    {
        try
        {
            await conn.DropSchemaAsync(schemaName, ct: ct).ConfigureAwait(false);
        }
        catch (OracleException e)
        {
            if (e.Message.Contains("deadlock"))
            {
                await Task.Delay(100, ct).ConfigureAwait(false);
                await conn.CloseAsync().ConfigureAwait(false);
                await conn.OpenAsync(ct).ConfigureAwait(false);
                await conn.DropSchemaAsync(schemaName, ct: ct).ConfigureAwait(false);
            }
            else
            {
                throw;
            }
        }

        // Create the schema - note that CreateSchemaStatementFor returns a PL/SQL block
        // that should be executed directly without "/" separator handling
        var sql = OracleMigrator.CreateSchemaStatementFor(schemaName);
        await conn.CreateCommand(sql).ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public static async Task<bool> FunctionExistsAsync(
        this OracleConnection conn,
        DbObjectName functionIdentifier,
        CancellationToken ct = default
    )
    {
        var sql = "SELECT 1 FROM all_objects WHERE object_name = :name AND owner = :schema AND object_type = 'FUNCTION'";
        await using var reader = await conn.CreateCommand(sql)
            .With("name", functionIdentifier.Name.ToUpperInvariant())
            .With("schema", functionIdentifier.Schema.ToUpperInvariant())
            .ExecuteReaderAsync(ct).ConfigureAwait(false);

        var result = await reader.ReadAsync(ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return result;
    }

    public static async Task<IReadOnlyList<DbObjectName>> ExistingTables(
        this OracleConnection conn,
        string? namePattern = null,
        CancellationToken ct = default
    )
    {
        var builder = new CommandBuilder();
        builder.Append("SELECT owner, table_name FROM all_tables");

        if (namePattern.IsNotEmpty())
        {
            builder.Append(" WHERE table_name LIKE :table_pattern");
            builder.AddNamedParameter("table_pattern", namePattern.ToUpperInvariant());
        }

        return await conn.FetchListAsync(builder, ReadDbObjectNameAsync, ct).ConfigureAwait(false);
    }

    public static async Task<IReadOnlyList<DbObjectName>> ExistingFunctionsAsync(
        this OracleConnection conn,
        string? namePattern = null,
        string[]? schemas = null,
        CancellationToken ct = default
    )
    {
        var builder = new CommandBuilder();
        builder.Append(
            "SELECT owner, object_name FROM all_objects WHERE object_type = 'FUNCTION'");

        if (namePattern.IsNotEmpty())
        {
            builder.Append(" AND object_name LIKE :name_pattern");
            builder.AddNamedParameter("name_pattern", namePattern.ToUpperInvariant());
        }

        if (schemas != null && schemas.Any())
        {
            builder.Append(" AND owner = :owner");
            builder.AddNamedParameter("owner", schemas[0].ToUpperInvariant());
        }

        return await conn.FetchListAsync(builder, ReadDbObjectNameAsync, ct).ConfigureAwait(false);
    }

    private static async Task<DbObjectName> ReadDbObjectNameAsync(DbDataReader reader, CancellationToken ct = default)
    {
        return new OracleObjectName(
            await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false),
            await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false)
        );
    }

    /// <summary>
    ///     Write the creation SQL for this ISchemaObject
    /// </summary>
    /// <param name="object"></param>
    /// <param name="rules"></param>
    /// <returns></returns>
    public static string ToCreateSql(this ISchemaObject @object, OracleMigrator rules)
    {
        var writer = new StringWriter();
        @object.WriteCreateStatement(rules, writer);

        return writer.ToString();
    }

    /// <summary>
    /// Perform any necessary migrations against a database for a supplied number of schema objects
    /// </summary>
    /// <param name="conn">A connection to the database you want to migrate. This method will open the connection if it is not already</param>
    /// <param name="schemaObject">A single schema object to be migrated</param>
    /// <param name="cancellationToken"></param>
    /// <param name="autoCreate">Optionally override the AutoCreate settings, the default is CreateOrUpdate</param>
    /// <returns>True if there was a migration made, false if no changes were detected</returns>
    public static async Task<bool> MigrateAsync(this ISchemaObject schemaObject, OracleConnection conn, CancellationToken? cancellationToken = default, AutoCreate autoCreate = AutoCreate.CreateOrUpdate)
    {
        cancellationToken ??= CancellationToken.None;

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(cancellationToken.Value).ConfigureAwait(false);
        }

        // Oracle doesn't support multiple result sets, so we must query each schema object separately
        var migration = await DetermineOracleMigrationAsync(conn, cancellationToken.Value, schemaObject).ConfigureAwait(false);
        if (migration.Difference == SchemaPatchDifference.None) return false;

        migration.AssertPatchingIsValid(autoCreate);

        var migrator = new OracleMigrator();
        await migrator.ApplyAllAsync(conn, migration, autoCreate, ct: cancellationToken.Value).ConfigureAwait(false);

        return true;
    }

    /// <summary>
    /// Perform any necessary migrations against a database for a supplied number of schema objects
    /// </summary>
    /// <param name="conn">A connection to the database you want to migrate. This method will open the connection if it is not already</param>
    /// <param name="schemaObjects">A collection of schema objects to migrate</param>
    /// <param name="cancellationToken"></param>
    /// <param name="autoCreate">Optionally override the AutoCreate settings, the default is CreateOrUpdate</param>
    /// <returns></returns>
    public static async Task<bool> MigrateAsync(this ISchemaObject[] schemaObjects, OracleConnection conn, CancellationToken? cancellationToken = default, AutoCreate autoCreate = AutoCreate.CreateOrUpdate)
    {
        cancellationToken ??= CancellationToken.None;

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(cancellationToken.Value).ConfigureAwait(false);
        }

        // Oracle doesn't support multiple result sets, so we must query each schema object separately
        var migration = await DetermineOracleMigrationAsync(conn, cancellationToken.Value, schemaObjects).ConfigureAwait(false);
        if (migration.Difference == SchemaPatchDifference.None) return false;

        migration.AssertPatchingIsValid(autoCreate);

        var migrator = new OracleMigrator();
        await migrator.ApplyAllAsync(conn, migration, autoCreate, ct: cancellationToken.Value).ConfigureAwait(false);

        return true;
    }

    /// <summary>
    /// Oracle-specific migration detection that queries each schema object separately.
    /// Oracle doesn't support multiple result sets in a single command like PostgreSQL or SQL Server,
    /// so we must execute each schema object's query individually.
    /// </summary>
    /// <summary>
    ///     Determine a migration for these objects, through the Oracle command builder.
    /// </summary>
    /// <remarks>
    ///     This used to sniff for <c>Tables.Table</c> and route it to
    ///     <c>Table.FindDeltaAsync</c>, because the generic path read columns only and would have
    ///     missed every index, foreign key and primary key. weasel#474 removed the reason:
    ///     <c>OracleDbCommandBuilder</c> splits the batch into one command per statement and the
    ///     reader chains across them, so an Oracle schema object can register as many introspection
    ///     queries as it needs. One path for every object type now, and no type test.
    /// </remarks>
    private static Task<SchemaMigration> DetermineOracleMigrationAsync(
        OracleConnection conn,
        CancellationToken ct,
        params ISchemaObject[] schemaObjects)
        => SchemaMigration.DetermineAsync(conn, new OracleDbCommandBuilder(), ct, schemaObjects);
}
