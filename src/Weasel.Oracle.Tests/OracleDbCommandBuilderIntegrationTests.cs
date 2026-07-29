using Oracle.ManagedDataAccess.Client;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Oracle.Tests;

[Collection("integration")]
public class OracleDbCommandBuilderIntegrationTests: IAsyncLifetime
{
    private readonly OracleConnection theConnection = new(ConnectionSource.ConnectionString);

    public async ValueTask InitializeAsync()
    {
        await theConnection.OpenAsync();

        // In Oracle a schema *is* a user, so this has to go through Weasel rather than a plain
        // "create schema"
        await theConnection.ResetSchemaAsync("BATCHING");

        await theConnection.CreateCommand(
                "create table batching.messages (id raw(16) not null primary key, replayable number(1), expires timestamp with time zone)")
            .ExecuteNonQueryAsync();

        await theConnection.CreateCommand(
                "create table batching.batch_notes (note varchar2(100))")
            .ExecuteNonQueryAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await theConnection.CloseAsync();
        await theConnection.DisposeAsync();
    }

    /// <summary>
    ///     The whole point of the exercise: a batch built exactly the way a database-agnostic consumer
    ///     builds one for PostgreSQL or SQL Server executes correctly against Oracle, with the `:` bind
    ///     markers, the Guid-as-RAW and bool-as-NUMBER conversions, and one command per statement.
    /// </summary>
    [Fact]
    public async Task execute_a_multi_statement_batch_in_one_transaction()
    {
        var kept = Guid.NewGuid();
        var expired = Guid.NewGuid();

        var builder = new OracleDbCommandBuilder();

        builder.Append("insert into batching.messages (id, replayable, expires) values (");
        builder.AppendParameter(kept);
        builder.Append(", ");
        // AddNamedParameter deliberately does not touch the command text, so the marker is written
        // by hand -- this is exactly how Wolverine's replayable-message operation binds
        builder.Append(":replayable");
        builder.AddNamedParameter("replayable", true);
        builder.Append(", ");
        builder.AppendParameter(DateTimeOffset.UtcNow.AddDays(1));
        builder.Append(")");

        builder.StartNewCommand();

        builder.Append("insert into batching.messages (id, replayable, expires) values (");
        builder.AppendParameter(expired);
        builder.Append(", ");
        builder.Append(":replayable2");
        builder.AddNamedParameter("replayable2", false);
        builder.Append(", ");
        builder.AppendParameter(DateTimeOffset.UtcNow.AddDays(-1));
        builder.Append(")");

        builder.StartNewCommand();

        builder.Append("insert into batching.batch_notes (note) values (");
        builder.AppendParameter("batched");
        builder.Append(")");

        var commands = builder.CompileCommands();
        commands.Count.ShouldBe(3);

        await using var tx = (OracleTransaction)await theConnection.BeginTransactionAsync();
        foreach (var command in commands)
        {
            command.Connection = theConnection;
            command.Transaction = tx;
            await command.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();

        (await countAsync("select count(*) from batching.messages")).ShouldBe(2);
        (await countAsync("select count(*) from batching.batch_notes where note = 'batched'")).ShouldBe(1);

        // The Guid round-trips through RAW(16), and the bool through NUMBER(1)
        (await countAsync(
                $"select count(*) from batching.messages where id = '{Convert.ToHexString(kept.ToByteArray())}' and replayable = 1"))
            .ShouldBe(1);
        (await countAsync(
                $"select count(*) from batching.messages where id = '{Convert.ToHexString(expired.ToByteArray())}' and replayable = 0"))
            .ShouldBe(1);
    }

    /// <summary>
    ///     A batch that returns data: each statement gets its own reader, because ODP.NET cannot
    ///     hand back several result sets from one command.
    /// </summary>
    [Fact]
    public async Task read_results_from_each_command_in_the_batch()
    {
        var id = Guid.NewGuid();

        await theConnection.CreateCommand(
                "insert into batching.messages (id, replayable, expires) values (:id, 1, systimestamp)")
            .With("id", id.ToByteArray())
            .ExecuteNonQueryAsync();

        await theConnection.CreateCommand("insert into batching.batch_notes (note) values ('read me')")
            .ExecuteNonQueryAsync();

        var builder = new OracleDbCommandBuilder();

        builder.Append("select count(*) from batching.messages where id = ");
        builder.AppendParameter(id);

        builder.StartNewCommand();

        builder.Append("select note from batching.batch_notes where note = ");
        builder.AppendParameter("read me");

        var commands = builder.CompileCommands();
        commands.Count.ShouldBe(2);

        foreach (var command in commands)
        {
            command.Connection = theConnection;
        }

        await using (var reader = await commands[0].ExecuteReaderAsync())
        {
            (await reader.ReadAsync()).ShouldBeTrue();
            Convert.ToInt32(reader.GetValue(0)).ShouldBe(1);
        }

        await using (var reader = await commands[1].ExecuteReaderAsync())
        {
            (await reader.ReadAsync()).ShouldBeTrue();
            reader.GetString(0).ShouldBe("read me");
        }
    }

    /// <summary>
    ///     ODP.NET does not implement the ADO.NET batching API at all, which is why Oracle needs the
    ///     statement splitting in the first place. If this ever starts failing, ODP.NET has grown
    ///     <see cref="System.Data.Common.DbBatch" /> support and the splitting could be revisited.
    /// </summary>
    [Fact]
    public void odp_net_still_cannot_batch()
    {
        theConnection.CanCreateBatch.ShouldBeFalse();
        Should.Throw<NotSupportedException>(() => theConnection.CreateBatch());
    }

    private async Task<int> countAsync(string sql)
    {
        await using var command = theConnection.CreateCommand(sql);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }
}
