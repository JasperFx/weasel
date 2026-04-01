using JasperFx;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Npgsql;
using Weasel.Core;
using Weasel.Postgresql;
using Weasel.SqlServer;
using Weasel.Sqlite;

namespace DocSamples;

public class ExtensionMethodSamples
{
    public async Task apply_changes_async_example()
    {
        Weasel.Postgresql.Tables.Table pgTable = null!;
        Weasel.SqlServer.Tables.Table ssTable = null!;
        Weasel.Sqlite.Tables.Table sqliteTable = null!;
        NpgsqlConnection npgsqlConnection = null!;
        SqlConnection sqlConnection = null!;
        SqliteConnection sqliteConnection = null!;

        #region sample_apply_changes_async
        // PostgreSQL
        await pgTable.ApplyChangesAsync(npgsqlConnection);

        // SQL Server
        await ssTable.ApplyChangesAsync(sqlConnection);

        // SQLite
        await sqliteTable.ApplyChangesAsync(sqliteConnection);
        #endregion
    }

    public async Task create_async_example()
    {
        Weasel.Postgresql.Tables.Table pgTable = null!;
        Weasel.SqlServer.Tables.Table ssTable = null!;
        NpgsqlConnection npgsqlConnection = null!;
        SqlConnection sqlConnection = null!;

        #region sample_create_async
        // PostgreSQL
        await pgTable.CreateAsync(npgsqlConnection);

        // SQL Server
        await ssTable.CreateAsync(sqlConnection);
        #endregion
    }

    public async Task drop_async_example()
    {
        Weasel.Postgresql.Tables.Table pgTable = null!;
        Weasel.SqlServer.Tables.Table ssTable = null!;
        NpgsqlConnection npgsqlConnection = null!;
        SqlConnection sqlConnection = null!;

        #region sample_drop_async
        // PostgreSQL
        await pgTable.DropAsync(npgsqlConnection);

        // SQL Server
        await ssTable.Drop(sqlConnection);
        #endregion
    }

    public async Task migrate_async_example()
    {
        Weasel.Postgresql.Tables.Table table = null!;
        NpgsqlConnection npgsqlConnection = null!;

        #region sample_migrate_async
        // PostgreSQL -- defaults to AutoCreate.CreateOrUpdate
        bool changed = await table.MigrateAsync(npgsqlConnection);

        // With explicit policy
        changed = await table.MigrateAsync(
            npgsqlConnection,
            autoCreate: AutoCreate.CreateOnly
        );
        #endregion
    }

    public async Task migrate_async_array_example()
    {
        SqlConnection sqlConnection = null!;
        Weasel.SqlServer.Tables.Table usersTable = null!;
        Weasel.SqlServer.Tables.Table ordersTable = null!;
        Weasel.SqlServer.Sequence sequence = null!;

        #region sample_migrate_async_array
        var objects = new ISchemaObject[] { usersTable, ordersTable, sequence };
        bool changed = await objects.MigrateAsync(sqlConnection);
        #endregion
    }

    public async Task ensure_schema_exists_example()
    {
        NpgsqlConnection npgsqlConnection = null!;
        SqlConnection sqlConnection = null!;

        #region sample_ensure_schema_exists
        // PostgreSQL
        await npgsqlConnection.EnsureSchemaExists("myapp");

        // SQL Server
        await sqlConnection.EnsureSchemaExists("myapp");
        #endregion
    }

    public async Task full_postgresql_example()
    {
        var connectionString = "Host=localhost;Database=mydb";

        #region sample_full_postgresql_extension_example
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        await using var conn = await dataSource.OpenConnectionAsync();

        // Ensure the schema exists
        await conn.EnsureSchemaExists("myapp");

        // Define a table
        var table = new Weasel.Postgresql.Tables.Table(new PostgresqlObjectName("myapp", "people"));
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email");

        // Apply changes -- creates the table if missing, updates if changed
        await table.ApplyChangesAsync(conn);
        #endregion
    }

    public async Task full_sqlserver_example()
    {
        var connectionString = "Server=localhost;Database=mydb;Trusted_Connection=True;TrustServerCertificate=True";

        #region sample_full_sqlserver_extension_example
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        await conn.EnsureSchemaExists("myapp");

        var table = new Weasel.SqlServer.Tables.Table(new SqlServerObjectName("myapp", "people"));
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email");

        await table.ApplyChangesAsync(conn);
        #endregion
    }

    public async Task full_sqlite_example()
    {
        #region sample_full_sqlite_extension_example
        await using var conn = new SqliteConnection("Data Source=myapp.db");
        await conn.OpenAsync();

        // Apply PRAGMA settings for performance
        var pragmas = new SqlitePragmaSettings
        {
            JournalMode = JournalMode.WAL,
            ForeignKeys = true
        };
        await pragmas.ApplyToConnectionAsync(conn);

        var table = new Weasel.Sqlite.Tables.Table("people");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email");

        await table.ApplyChangesAsync(conn);
        #endregion
    }
}
