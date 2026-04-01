using Npgsql;
using Weasel.Core;
using Weasel.Postgresql;
using Weasel.Postgresql.Tables;

namespace DocSamples;

public class PostgresqlSequenceSamples
{
    public void create_sequence()
    {
        #region sample_pg_create_sequence
        // Basic sequence
        var sequence = new Sequence("public.order_number_seq");

        // Sequence with a start value
        var sequenceWithStart = new Sequence(
            new DbObjectName("public", "invoice_seq"),
            startWith: 1000);
        #endregion
    }

    public void owned_sequence()
    {
        #region sample_pg_owned_sequence
        var sequence = new Sequence("public.order_number_seq");
        sequence.Owner = new DbObjectName("public", "orders");
        sequence.OwnerColumn = "order_number";
        #endregion
    }

    public void sequence_with_table_column()
    {
        #region sample_pg_sequence_with_table_column
        var sequence = new Sequence(
            new DbObjectName("public", "order_number_seq"),
            startWith: 1000);

        var table = new Table("orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<long>("order_number")
            .DefaultValueFromSequence(sequence);
        #endregion
    }

    public async Task sequence_delta_detection()
    {
        #region sample_pg_sequence_delta_detection
        var dataSource = new NpgsqlDataSourceBuilder("Host=localhost;Database=mydb").Build();
        var sequence = new Sequence("public.order_number_seq");

        await using var conn = dataSource.CreateConnection();
        await conn.OpenAsync();

        var delta = await sequence.FindDeltaAsync(conn);
        // delta.Difference: None or Create
        #endregion
    }

    public void sequence_generate_ddl()
    {
        #region sample_pg_sequence_generate_ddl
        var sequence = new Sequence("public.order_number_seq");

        var migrator = new PostgresqlMigrator();
        var writer = new StringWriter();

        sequence.WriteCreateStatement(migrator, writer);
        // CREATE SEQUENCE public.order_number_seq START 1000;

        sequence.WriteDropStatement(migrator, writer);
        // DROP SEQUENCE IF EXISTS public.order_number_seq;
        #endregion
    }
}
