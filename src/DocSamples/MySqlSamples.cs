using Weasel.Core;
using Weasel.MySql;
using Weasel.MySql.Tables;
using MySqlConnector;

namespace DocSamples;

public class MySqlSamples
{
    // index.md samples

    public void mysql_connection_string()
    {
        #region sample_mysql_connection_string
        var connectionString = "Server=localhost;Database=mydb;User=root;Password=YourPassword;";
        #endregion
    }

    public void mysql_create_migrator()
    {
        #region sample_mysql_create_migrator
        var migrator = new MySqlMigrator();
        #endregion
    }

    public async Task mysql_ensure_database_exists()
    {
        var connectionString = "Server=localhost;Database=mydb;User=root;Password=YourPassword;";
        var migrator = new MySqlMigrator();

        #region sample_mysql_ensure_database_exists
        await using var conn = new MySqlConnection(connectionString);
        await migrator.EnsureDatabaseExistsAsync(conn);
        // Generates: CREATE DATABASE IF NOT EXISTS `mydb`;
        #endregion
    }

    // tables.md samples

    public void mysql_define_table()
    {
        #region sample_mysql_define_table
        var table = new Table("users");

        table.AddColumn<int>("id").AsPrimaryKey().AutoNumber();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email").NotNull().AddIndex(idx => idx.IsUnique = true);
        table.AddColumn<DateTime>("created_at");
        #endregion
    }

    public void mysql_table_options()
    {
        var table = new Table("users");

        #region sample_mysql_table_options
        table.Engine = "InnoDB";       // default
        table.Charset = "utf8mb4";
        table.Collation = "utf8mb4_unicode_ci";
        #endregion
    }

    public void mysql_partitioning()
    {
        var table = new Table("users");

        #region sample_mysql_partitioning
        table.PartitionByRange("created_at");
        table.PartitionByHash("id");
        table.PartitionByList("region");
        table.PartitionByKey("id");
        table.PartitionCount = 4;  // for Hash or Key strategies
        #endregion
    }

    public async Task mysql_delta_detection()
    {
        var connectionString = "Server=localhost;Database=mydb;User=root;Password=YourPassword;";
        var table = new Table("users");

        #region sample_mysql_delta_detection
        await using var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();

        var delta = await table.FindDeltaAsync(conn);
        #endregion
    }

    public void mysql_generate_ddl()
    {
        var table = new Table("users");

        #region sample_mysql_generate_ddl
        var migrator = new MySqlMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        #endregion
    }

    // sequences.md samples

    public void mysql_define_sequence()
    {
        #region sample_mysql_define_sequence
        var seq = new Sequence("order_seq");
        seq.StartWith = 1000;
        seq.IncrementBy = 1;
        #endregion
    }

    public void mysql_sequence_create_ddl()
    {
        var seq = new Sequence("order_seq");

        #region sample_mysql_sequence_create_ddl
        var migrator = new MySqlMigrator();
        var writer = new StringWriter();
        seq.WriteCreateStatement(migrator, writer);
        #endregion
    }

    public void mysql_sequence_drop_ddl()
    {
        var seq = new Sequence("order_seq");
        var migrator = new MySqlMigrator();
        var writer = new StringWriter();

        #region sample_mysql_sequence_drop_ddl
        seq.WriteDropStatement(migrator, writer);
        // Output: DROP TABLE IF EXISTS `public`.`order_seq`;
        #endregion
    }

    public void mysql_sequence_delta_detection()
    {
        #region sample_mysql_sequence_delta_detection
        // Delta detection happens automatically during schema migration.
        // The sequence is created if the backing table does not exist.
        #endregion
    }
}
