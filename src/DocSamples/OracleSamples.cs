using Weasel.Core;
using Weasel.Oracle;
using Weasel.Oracle.Tables;
using Oracle.ManagedDataAccess.Client;

namespace DocSamples;

public class OracleSamples
{
    // index.md samples

    public void oracle_connection_string()
    {
        #region sample_oracle_connection_string
        var connectionString = "User Id=myuser;Password=mypass;Data Source=localhost:1521/XEPDB1;";
        #endregion
    }

    public void oracle_create_migrator()
    {
        #region sample_oracle_create_migrator
        var migrator = new OracleMigrator();
        #endregion
    }

    public async Task oracle_ensure_database_exists()
    {
        var connectionString = "User Id=myuser;Password=mypass;Data Source=localhost:1521/XEPDB1;";
        var migrator = new OracleMigrator();

        #region sample_oracle_ensure_database_exists
        await using var conn = new OracleConnection(connectionString);
        await migrator.EnsureDatabaseExistsAsync(conn);
        #endregion
    }

    // tables.md samples

    public void oracle_define_table()
    {
        #region sample_oracle_define_table
        var table = new Table("WEASEL.users");

        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email").NotNull().AddIndex(idx => idx.IsUnique = true);
        table.AddColumn<DateTime>("created_at");
        #endregion
    }

    public void oracle_foreign_keys()
    {
        #region sample_oracle_foreign_keys
        var orders = new Table("WEASEL.orders");
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.AddColumn<int>("user_id").NotNull()
            .ForeignKeyTo("WEASEL.users", "id", onDelete: CascadeAction.Cascade);
        #endregion
    }

    public void oracle_partitioning()
    {
        var table = new Table("WEASEL.users");

        #region sample_oracle_partitioning
        table.PartitionByRange("created_at");
        table.PartitionByHash("id");
        table.PartitionByList("region");
        #endregion
    }

    public async Task oracle_delta_detection()
    {
        var connectionString = "User Id=myuser;Password=mypass;Data Source=localhost:1521/XEPDB1;";
        var table = new Table("WEASEL.users");

        #region sample_oracle_delta_detection
        await using var conn = new OracleConnection(connectionString);
        await conn.OpenAsync();

        var delta = await table.FindDeltaAsync(conn);
        #endregion
    }

    public void oracle_generate_ddl()
    {
        var table = new Table("WEASEL.users");

        #region sample_oracle_generate_ddl
        var migrator = new OracleMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        #endregion
    }

    // sequences.md samples

    public void oracle_define_sequence()
    {
        #region sample_oracle_define_sequence
        // Simple sequence starting at 1
        var seq = new Sequence("WEASEL.order_seq");

        // Sequence with a custom start value
        var seq2 = new Sequence(
            DbObjectName.Parse(OracleProvider.Instance, "WEASEL.invoice_seq"),
            startWith: 1000
        );
        #endregion
    }

    public void oracle_sequence_with_table()
    {
        #region sample_oracle_sequence_with_table
        var table = new Table("WEASEL.orders");
        var seq = new Sequence("WEASEL.order_seq");

        table.AddColumn<long>("id").AsPrimaryKey()
            .DefaultValueFromSequence(seq);
        #endregion
    }

    public void oracle_sequence_create_ddl()
    {
        var seq = new Sequence("WEASEL.order_seq");

        #region sample_oracle_sequence_create_ddl
        var migrator = new OracleMigrator();
        var writer = new StringWriter();
        seq.WriteCreateStatement(migrator, writer);
        #endregion
    }

    public void oracle_sequence_drop_ddl()
    {
        var seq = new Sequence("WEASEL.order_seq");
        var migrator = new OracleMigrator();
        var writer = new StringWriter();

        #region sample_oracle_sequence_drop_ddl
        seq.WriteDropStatement(migrator, writer);
        #endregion
    }

    public async Task oracle_sequence_delta_detection()
    {
        var connectionString = "User Id=myuser;Password=mypass;Data Source=localhost:1521/XEPDB1;";
        var seq = new Sequence("WEASEL.order_seq");

        #region sample_oracle_sequence_delta_detection
        await using var conn = new OracleConnection(connectionString);
        await conn.OpenAsync();

        var delta = await seq.FindDeltaAsync(conn);
        // delta.Difference: None or Create
        #endregion
    }
}
