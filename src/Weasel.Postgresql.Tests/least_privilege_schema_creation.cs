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

        var exception = await Should.ThrowAsync<PostgresException>(
            () => new PostgresqlMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate));

        exception.SqlState.ShouldBe(PostgresErrorCodes.InsufficientPrivilege);
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
}
