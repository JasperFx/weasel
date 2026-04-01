using Microsoft.Data.SqlClient;
using Npgsql;

namespace DocSamples;

#region sample_ICommandBuilder_interface
public interface ICommandBuilder_Sample
{
    string TenantId { get; set; }
    string? LastParameterName { get; }

    void Append(string sql);
    void Append(char character);
    void AppendParameter<T>(T value);
    void AppendWithParameters(string text);
    void StartNewCommand();
    // ... additional members
}
#endregion

public class CommandBuilderSamples
{
    public async Task postgresql_batch_builder_example()
    {
        var connectionString = "Host=localhost;Database=mydb";

        #region sample_postgresql_batch_builder
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        await using var batch = dataSource.CreateBatch();
        var builder = new Weasel.Postgresql.BatchBuilder(batch);

        // First command
        builder.Append("INSERT INTO people (name, email) VALUES (");
        builder.AppendParameter("Alice");
        builder.Append(", ");
        builder.AppendParameter("alice@example.com");
        builder.Append(")");

        // Second command in the same batch
        builder.StartNewCommand();
        builder.Append("INSERT INTO people (name, email) VALUES (");
        builder.AppendParameter("Bob");
        builder.Append(", ");
        builder.AppendParameter("bob@example.com");
        builder.Append(")");

        builder.Compile();

        // Both inserts execute in a single round trip
        await batch.ExecuteNonQueryAsync();
        #endregion
    }

    public async Task sqlserver_batch_builder_example()
    {
        var connectionString = "Server=localhost;Database=mydb;Trusted_Connection=True;TrustServerCertificate=True";

        #region sample_sqlserver_batch_builder
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        await using var batch = new SqlBatch { Connection = connection };
        var builder = new Weasel.SqlServer.BatchBuilder(batch);

        // First command
        builder.Append("INSERT INTO people (name, email) VALUES (");
        builder.AppendParameter("Alice");
        builder.Append(", ");
        builder.AppendParameter("alice@example.com");
        builder.Append(")");

        // Second command
        builder.StartNewCommand();
        builder.Append("INSERT INTO people (name, email) VALUES (");
        builder.AppendParameter("Bob");
        builder.Append(", ");
        builder.AppendParameter("bob@example.com");
        builder.Append(")");

        builder.Compile();
        await batch.ExecuteNonQueryAsync();
        #endregion
    }

    public void grouped_parameters_example()
    {
        var batch = new NpgsqlBatch();
        var builder = new Weasel.Postgresql.BatchBuilder(batch);

        #region sample_grouped_parameters
        var group = builder.CreateGroupedParameterBuilder(',');
        group.AppendParameter("value1");
        group.AppendParameter("value2");
        group.AppendParameter("value3");
        // Produces: $1,$2,$3 (PostgreSQL) or @p0,@p1,@p2 (SQL Server)
        #endregion
    }

    public async Task command_builder_internal_usage()
    {
        var connectionString = "Host=localhost;Database=mydb";
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        await using var connection = await dataSource.OpenConnectionAsync();
        Weasel.Core.ISchemaObject schemaObject = null!;
        var ct = CancellationToken.None;

        #region sample_command_builder_internal_usage
        // Used internally -- you typically interact with BatchBuilder instead
        var cmd = connection.CreateCommand();
        var cmdBuilder = new Weasel.Core.DbCommandBuilder(cmd);
        schemaObject.ConfigureQueryCommand(cmdBuilder);
        var reader = await Weasel.Core.DbCommandBuilderExtensions.ExecuteReaderAsync(connection, cmdBuilder, ct: ct);
        #endregion
    }
}
