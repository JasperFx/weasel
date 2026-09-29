using FirebirdSql.Data.FirebirdClient;
using Weasel.Core;
using Weasel.Firebird;
using Weasel.Firebird.Tables;

namespace DocSamples;

public class FirebirdSamples
{
    // index.md samples

    public string firebird_connection_string()
    {
        #region sample_firebird_connection_string
        var connectionString =
            "DataSource=localhost;Port=3050;Database=/var/lib/firebird/data/mydb.fdb;User=SYSDBA;Password=YourPassword;Charset=UTF8";
        #endregion

        return connectionString;
    }

    public void firebird_create_migrator()
    {
        #region sample_firebird_create_migrator
        var migrator = new FirebirdMigrator();
        #endregion
    }

    public async Task firebird_ensure_database_exists()
    {
        var connectionString =
            "DataSource=localhost;Port=3050;Database=/var/lib/firebird/data/mydb.fdb;User=SYSDBA;Password=YourPassword;Charset=UTF8";
        var migrator = new FirebirdMigrator();

        #region sample_firebird_ensure_database_exists
        await using var conn = new FbConnection(connectionString);
        await migrator.EnsureDatabaseExistsAsync(conn);
        // Opens mydb.fdb, and creates it only when the file is missing
        #endregion
    }

    public void firebird_identifier_length()
    {
        #region sample_firebird_identifier_length
        // Only for a database that no Firebird 3 server will open
        var migrator = new FirebirdMigrator { MaxIdentifierLength = 63 };
        #endregion
    }

    // tables.md samples

    public void firebird_define_table()
    {
        #region sample_firebird_define_table
        var table = new Table("users");

        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email").NotNull().AddIndex(idx => idx.IsUnique = true);
        table.AddColumn<DateTime>("created_at");
        #endregion
    }

    public void firebird_foreign_keys()
    {
        #region sample_firebird_foreign_keys
        var orders = new Table("orders");
        orders.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        orders.AddColumn<int>("user_id").NotNull()
            .ForeignKeyTo("users", "id", onDelete: CascadeAction.Cascade);
        #endregion
    }

    public void firebird_index_direction()
    {
        var table = new Table("orders");
        table.AddColumn<DateTime>("placed_at");

        #region sample_firebird_index_direction
        var index = new IndexDefinition("idx_orders_placed")
        {
            Columns = ["placed_at"],
            SortOrder = SortOrder.Desc
        };
        table.Indexes.Add(index);
        // CREATE DESCENDING INDEX idx_orders_placed ON orders (placed_at)
        #endregion
    }

    public async Task firebird_delta_detection()
    {
        var connectionString =
            "DataSource=localhost;Port=3050;Database=/var/lib/firebird/data/mydb.fdb;User=SYSDBA;Password=YourPassword;Charset=UTF8";
        var table = new Table("users");

        #region sample_firebird_delta_detection
        await using var conn = new FbConnection(connectionString);
        await conn.OpenAsync();

        var delta = await table.FindDeltaAsync(conn);
        #endregion
    }

    public void firebird_generate_ddl()
    {
        var table = new Table("users");

        #region sample_firebird_generate_ddl
        var migrator = new FirebirdMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        #endregion
    }
}
