using System.Threading.Tasks;
using JasperFx;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     A migration that touches an existing schema must not require <c>CREATE</c> on the database.
///     <para>
///     <see cref="SchemaMigration.Schemas" /> is every schema any delta mentions, not the schemas that are
///     missing, so <c>PostgresqlMigrator.executeDelta</c> re-emits schema creation on every migration that
///     touches one - including a migration whose only work is adding a table to a schema that has been there
///     for months. PostgreSQL checks <c>CREATE</c> on the <em>database</em> before it evaluates
///     <c>CREATE SCHEMA</c>'s own <c>IF NOT EXISTS</c>, so on a least-privilege role that statement fails
///     with <c>42501 permission denied for database</c> even though there is nothing for it to do - and it is
///     the first statement in the script, so every table in the migration goes with it.
///     </para>
///     <para>
///     That role is not exotic. Granting an application <c>USAGE, CREATE</c> on one schema, and nothing on
///     the database, is the standard way to let it manage its own tables while a migration role owns
///     everything else; it is what Wolverine's PostgreSQL envelope storage documents. Before this fix such an
///     application could not start against a database that already had its schema.
///     </para>
///     <para>
///     <see cref="Weasel.SqlServer.SqlServerMigrator.CreateSchemaStatementFor" /> has always guarded on
///     <c>sys.schemas</c> for the same reason. These tests pin the PostgreSQL side to the same behaviour.
///     </para>
/// </summary>
[Collection("least_privilege")]
public class least_privilege_schema_creation: IntegrationContext
{
    private const string RoleName = "weasel_least_privilege";

    public least_privilege_schema_creation(): base("least_privilege")
    {
    }

    [Fact]
    public void schema_creation_is_guarded_by_an_existence_check()
    {
        var writer = new StringWriter();

        new PostgresqlMigrator().WriteSchemaCreationSql(["one"], writer);

        var sql = writer.ToString();

        sql.ShouldContain("IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'one')");

        // The concurrency handling from #282 has to survive the guard: the pre-check is not a substitute for
        // it, because two sessions can still both pass the check and race on the catalog insert.
        sql.ShouldContain("WHEN duplicate_schema THEN NULL;");
        sql.ShouldContain("WHEN unique_violation THEN NULL;");
    }

    [Fact]
    public void schema_name_is_escaped_into_the_existence_check()
    {
        var writer = new StringWriter();

        new PostgresqlMigrator().WriteSchemaCreationSql(["it's"], writer);

        writer.ToString().ShouldContain("nspname = 'it''s'");
    }

    /// <summary>
    ///     A schema name can reach the migrator already delimited: <see cref="DbObjectName.Parse" /> splits a
    ///     qualified name on <c>.</c> and keeps the parts exactly as written, and
    ///     <c>PostgresqlProvider.ToQualifiedName</c> passes a name the caller delimited straight through. The
    ///     check has to be made against the name <c>pg_namespace</c> holds, which is the bare one - otherwise
    ///     it asks for <c>nspname = '"MixedCase"'</c>, nothing ever matches, and the guard is permanently
    ///     false for exactly the names that had to be quoted.
    /// </summary>
    [Fact]
    public void a_quoted_schema_name_is_checked_by_the_name_the_catalog_holds()
    {
        // This used to assert that DbObjectName.Parse hands the schema back with its quotes, as the
        // precondition that made the guard's own unquoting necessary. weasel#499 moved that
        // normalization upstream into the provider ObjectName constructors, so a name reaching the
        // migrator through Parse now arrives bare and the precondition no longer holds -- see
        // object_name_normalization_conformance for the contract that replaced it.
        //
        // The guard keeps its own unquoting and this test keeps testing it, because
        // WriteSchemaCreationSql is public and takes bare strings: a caller can still hand it a
        // delimited name without going through Parse at all.
        var writer = new StringWriter();
        new PostgresqlMigrator().WriteSchemaCreationSql(["\"MixedCase\""], writer);
        var sql = writer.ToString();

        sql.ShouldContain("nspname = 'MixedCase'");
        sql.ShouldNotContain("nspname = '\"MixedCase\"'");

        // The create still emits the delimited form, because that is the name PostgreSQL has to be told
        // to make. Only the catalog lookup changes.
        sql.ShouldContain("CREATE SCHEMA IF NOT EXISTS \"MixedCase\"");
    }

    /// <summary>
    ///     An interior quote survives the round trip: <c>"it""s"</c> is the delimited spelling of the schema
    ///     literally named <c>it"s</c>, so the doubling is undone for the lookup and the single quote
    ///     escaping still applies on top of it.
    /// </summary>
    [Fact]
    public void undoubling_and_literal_escaping_compose()
    {
        var writer = new StringWriter();

        new PostgresqlMigrator().WriteSchemaCreationSql(["\"it\"\"s\""], writer);

        writer.ToString().ShouldContain("nspname = 'it\"s'");
    }

    /// <summary>
    ///     And the same thing against a real role. A schema whose name had to be quoted is the case the
    ///     unquoted check got wrong, so it is the case that has to be shown working end to end.
    /// </summary>
    [Fact]
    public async Task a_quoted_schema_name_does_not_reintroduce_the_permission_failure()
    {
        await GrantQuotedSchemaToLeastPrivilegeRoleAsync();

        var writer = new StringWriter();
        new PostgresqlMigrator().WriteSchemaCreationSql(["\"MixedCase\""], writer);

        await using var conn = new NpgsqlConnection(LeastPrivilegeConnectionString());
        await conn.OpenAsync();

        // Pre-fix the guard never matched, so this reached CREATE SCHEMA and threw
        // "42501 permission denied for database" on a schema that has been there all along.
        await conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();
    }

    /// <summary>
    ///     The regression itself, against a real role. Applying a table into a schema that already exists has
    ///     to succeed for a role that was granted the schema and nothing else.
    /// </summary>
    [Fact]
    public async Task can_apply_a_delta_to_an_existing_schema_without_create_on_the_database()
    {
        await ResetSchema();
        await GrantSchemaToLeastPrivilegeRoleAsync();

        var table = new Table(new PostgresqlObjectName(SchemaName, "documents"));
        table.AddColumn<int>("id").AsPrimaryKey();

        await using var conn = new NpgsqlConnection(LeastPrivilegeConnectionString());
        await conn.OpenAsync();

        var migration = await SchemaMigration.DetermineAsync(conn, table);
        migration.Difference.ShouldBe(SchemaPatchDifference.Create);

        // Pre-fix this threw 42501 on the CREATE SCHEMA that opens the script, and the table was never
        // created even though the role holds CREATE on the schema it belongs to.
        await new PostgresqlMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate);

        (await SchemaMigration.DetermineAsync(conn, table)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The other half: a genuinely missing schema still needs <c>CREATE</c> on the database, and the
    ///     failure has to stay a failure rather than being swallowed by the guard.
    /// </summary>
    /// <remarks>
    ///     This used to assert a raw <see cref="PostgresException" /> with
    ///     <c>SqlState == 42501</c>, which is exactly the bare signal weasel#598 is about: the only
    ///     thing telling a user what went wrong was a SQLSTATE in an exception nobody catches by
    ///     type. The failure is unchanged -- it is still a failure, still on the same statement,
    ///     and the <see cref="PostgresException" /> is still there as the inner exception -- but it
    ///     now arrives named, so this asserts the new shape rather than the old one.
    /// </remarks>
    [Fact]
    public async Task still_fails_when_the_schema_is_missing_and_the_role_cannot_create_one()
    {
        await ResetSchema();
        await GrantSchemaToLeastPrivilegeRoleAsync();

        var table = new Table(new PostgresqlObjectName("least_privilege_absent", "documents"));
        table.AddColumn<int>("id").AsPrimaryKey();

        await using var conn = new NpgsqlConnection(LeastPrivilegeConnectionString());
        await conn.OpenAsync();

        var migration = await SchemaMigration.DetermineAsync(conn, table);

        var exception = await Should.ThrowAsync<InsufficientDatabasePrivilegeException>(
            () => new PostgresqlMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate));

        exception.InnerException.ShouldBeOfType<PostgresException>()
            .SqlState.ShouldBe(PostgresErrorCodes.InsufficientPrivilege);

        // The role is named from the connection, so the message says who was refused rather than
        // leaving the reader to go and look at the connection string.
        exception.Role.ShouldBe(RoleName);
        exception.Message.ShouldContain(RoleName);

        // And the statement that was refused, which is the one thing a migration logger prints
        // and a caught exception used to lose.
        exception.Statement.ShouldContain("CREATE SCHEMA");

        exception.Message.ShouldContain("AutoCreate.None");
    }

    /// <summary>
    ///     The third permission failure from weasel#598, and the nastiest one: DDL against an
    ///     object that exists but belongs to somebody else. Unlike the schema case this one lands
    ///     mid-migration, after earlier statements have already auto-committed.
    /// </summary>
    [Fact]
    public async Task a_refused_alter_on_someone_elses_table_is_named_too()
    {
        await ResetSchema();
        await GrantSchemaToLeastPrivilegeRoleAsync();

        // Created and owned by the superuser; the least-privilege role holds USAGE + CREATE on the
        // schema, which lets it make its own tables but not alter this one.
        await using (var owner = new NpgsqlConnection(ConnectionSource.ConnectionString))
        {
            await owner.OpenAsync();
            await owner.CreateCommand(
                    $"""
                     create table {SchemaName}.owned_elsewhere (id int primary key);
                     grant select on {SchemaName}.owned_elsewhere to {RoleName};
                     """)
                .ExecuteNonQueryAsync();
        }

        var table = new Table(new PostgresqlObjectName(SchemaName, "owned_elsewhere"));
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");

        await using var conn = new NpgsqlConnection(LeastPrivilegeConnectionString());
        await conn.OpenAsync();

        var migration = await SchemaMigration.DetermineAsync(conn, table);
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        var exception = await Should.ThrowAsync<InsufficientDatabasePrivilegeException>(
            () => new PostgresqlMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate));

        exception.InnerException.ShouldBeOfType<PostgresException>()
            .SqlState.ShouldBe(PostgresErrorCodes.InsufficientPrivilege);
        exception.Statement.ShouldContain("owned_elsewhere");
    }

    private static string LeastPrivilegeConnectionString() =>
        new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            Username = RoleName,
            Password = RoleName
        }.ConnectionString;

    /// <remarks>
    ///     A role is cluster-wide rather than per database, so this tolerates one left behind by an earlier
    ///     run. <c>least_privilege_absent</c> is dropped so the negative test above has a schema that is
    ///     genuinely missing.
    /// </remarks>
    private async Task GrantSchemaToLeastPrivilegeRoleAsync()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await conn.CreateCommand(
                $"""
                 DO $$
                 BEGIN
                     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{RoleName}') THEN
                         CREATE ROLE {RoleName} LOGIN PASSWORD '{RoleName}';
                     END IF;
                 END $$;
                 DROP SCHEMA IF EXISTS least_privilege_absent CASCADE;
                 GRANT USAGE, CREATE ON SCHEMA {SchemaName} TO {RoleName};
                 """)
            .ExecuteNonQueryAsync();
    }

    /// <remarks>
    ///     Quoted, so PostgreSQL keeps the casing rather than folding it. This is deliberately not the
    ///     fixture's own schema: the point is a name that cannot be written bare.
    /// </remarks>
    private static async Task GrantQuotedSchemaToLeastPrivilegeRoleAsync()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await conn.CreateCommand(
                $"""
                 DO $$
                 BEGIN
                     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{RoleName}') THEN
                         CREATE ROLE {RoleName} LOGIN PASSWORD '{RoleName}';
                     END IF;
                 END $$;
                 CREATE SCHEMA IF NOT EXISTS "MixedCase";
                 GRANT USAGE, CREATE ON SCHEMA "MixedCase" TO {RoleName};
                 """)
            .ExecuteNonQueryAsync();
    }
}
