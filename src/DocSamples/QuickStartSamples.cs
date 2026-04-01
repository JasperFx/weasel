using Microsoft.Data.SqlClient;
using Npgsql;
using Weasel.Core;
using Weasel.Postgresql;
using Weasel.Postgresql.Tables;
using Weasel.SqlServer;

namespace DocSamples;

public class QuickStartSamples
{
    public async Task postgresql_quickstart()
    {
        #region sample_postgresql_quickstart
        // Define a table
        var table = new Weasel.Postgresql.Tables.Table("myapp.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name").NotNull();
        table.AddColumn<string>("last_name").NotNull();
        table.AddColumn<string>("email");

        // Add a unique index on email
        table.ModifyColumn("email").AddIndex(i => i.IsUnique = true);

        // Connect and migrate
        var dataSource = NpgsqlDataSource.Create("Host=localhost;Database=mydb");
        await using var conn = await dataSource.OpenConnectionAsync();
        await table.MigrateAsync(conn);
        #endregion
    }

    public async Task sqlserver_quickstart()
    {
        #region sample_sqlserver_quickstart
        // Define a table
        var table = new Weasel.SqlServer.Tables.Table(new DbObjectName("dbo", "people"));
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name").NotNull();
        table.AddColumn<string>("last_name").NotNull();

        // Connect and migrate
        await using var conn = new SqlConnection("Server=localhost;Database=mydb;Trusted_Connection=True;TrustServerCertificate=True");
        await conn.OpenAsync();
        await table.MigrateAsync(conn);
        #endregion
    }
}
