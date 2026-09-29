using MySqlConnector;
using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests;

/// <summary>
///     A migration into a database that already exists must not require <c>CREATE</c> on that database.
/// </summary>
/// <remarks>
///     <para>
///         A MySQL schema is a database, and <c>SchemaMigration.Schemas</c> is every schema any delta
///         mentions rather than the ones that are missing, so <c>MySqlMigrator.executeDelta</c> opened
///         every migration with <c>CREATE DATABASE IF NOT EXISTS</c> for each of them. MySQL checks the
///         <c>CREATE</c> privilege on the database before it evaluates <c>IF NOT EXISTS</c>, so a user that
///         may alter its tables but not create databases was refused with <c>1044 Access denied ... to
///         database</c> for a database that was already there -- and it is the first statement of the
///         migration, so nothing in it was applied.
///     </para>
///     <para>
///         The PostgreSQL twin is <c>least_privilege_schema_creation</c> (weasel#495); SQL Server has
///         always guarded on <c>sys.schemas</c>.
///     </para>
///     <para>
///         Root credentials to create the restricted user, following <c>dropping_schemas</c>.
///     </para>
/// </remarks>
[Collection("integration")]
public class least_privilege_database_creation: IAsyncLifetime
{
    private const string UserName = "weasel_no_create";
    private const string Password = "P@55w0rd";

    private MySqlConnection theRootConnection = default!;

    private static string connectionStringFor(string user, string password, string database)
        => new MySqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            UserID = user, Password = password, Database = database
        }.ConnectionString;

    public async ValueTask InitializeAsync()
    {
        theRootConnection = new MySqlConnection(connectionStringFor("root", Password, "weasel_testing"));
        await theRootConnection.OpenAsync();

        // Everything an application needs to evolve tables that already exist, and no CREATE.
        await executeAsRootAsync($"CREATE USER IF NOT EXISTS '{UserName}'@'%' IDENTIFIED BY '{Password}'");
        await executeAsRootAsync(
            "GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, INDEX, DROP, REFERENCES "
            + $"ON `weasel_testing`.* TO '{UserName}'@'%'");
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            // A server-level account outlives the test database, so it is not left behind.
            await executeAsRootAsync($"DROP USER IF EXISTS '{UserName}'@'%'");
        }
        finally
        {
            await theRootConnection.CloseAsync();
            await theRootConnection.DisposeAsync();
        }
    }

    private async Task executeAsRootAsync(string sql)
    {
        await using var cmd = theRootConnection.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task can_apply_a_delta_to_an_existing_database_without_create_on_it()
    {
        await executeAsRootAsync("DROP TABLE IF EXISTS `weasel_testing`.`lp_documents`");
        await executeAsRootAsync("CREATE TABLE `weasel_testing`.`lp_documents` (id INT NOT NULL PRIMARY KEY)");

        var table = new Table("weasel_testing.lp_documents");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("title", "varchar(100)");
        table.Indexes.Add(new IndexDefinition("idx_lp_documents_title") { Columns = ["title"] });

        await using var conn = new MySqlConnection(connectionStringFor(UserName, Password, "weasel_testing"));
        await conn.OpenAsync(TestContext.Current.CancellationToken);

        // Pre-fix: InsufficientDatabasePrivilegeException wrapping "1044 Access denied for user
        // 'weasel_no_create'@'%' to database 'weasel_testing'", raised by CREATE DATABASE IF NOT EXISTS.
        await table.ApplyChangesAsync(conn, TestContext.Current.CancellationToken);

        (await table.FindDeltaAsync(conn, TestContext.Current.CancellationToken))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_database_that_is_missing_is_still_created()
    {
        var databaseName = $"weasel_created_{Guid.NewGuid():N}";

        try
        {
            var table = new Table(new MySqlObjectName(databaseName, "documents"));
            table.AddColumn<int>("id").AsPrimaryKey();

            await table.ApplyChangesAsync(theRootConnection, TestContext.Current.CancellationToken);

            (await table.FindDeltaAsync(theRootConnection, TestContext.Current.CancellationToken))
                .Difference.ShouldBe(SchemaPatchDifference.None);
        }
        finally
        {
            await theRootConnection.DropSchemaAsync(databaseName, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task a_missing_database_still_needs_the_privilege_to_create_it()
    {
        var table = new Table(new MySqlObjectName("weasel_not_granted", "documents"));
        table.AddColumn<int>("id").AsPrimaryKey();

        await using var conn = new MySqlConnection(connectionStringFor(UserName, Password, "weasel_testing"));
        await conn.OpenAsync(TestContext.Current.CancellationToken);

        await Should.ThrowAsync<InsufficientDatabasePrivilegeException>(
            () => table.ApplyChangesAsync(conn, TestContext.Current.CancellationToken));
    }
}
