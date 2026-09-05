using System.Data.Common;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     The shape of <see cref="ITransactionParticipant{TConnection,TTransaction}" /> (weasel#561) —
///     the generic transaction-participant contract lifted out of Marten, Polecat, and Fisher, each
///     of which declared the same interface closed over its own provider types. What is pinned here
///     is exactly what the stores' aliasing depends on: a store can expose a derived interface
///     closing the generics without breaking implementors written against the old provider-typed
///     signature, and Fisher's default-implemented <c>AfterCommitAsync</c> is part of the shared
///     contract so a participant that does not need it declares one member, as before.
/// </summary>
/// <remarks>
///     The concrete pair the tests close the generic over is Microsoft.Data.Sqlite's, purely
///     because it is the one provider in this repository that needs no server — nothing here is
///     about SQLite.
/// </remarks>
public class transaction_participant_contract
{
    /// <summary>
    ///     A store's own <c>ITransactionParticipant</c> becomes a derived interface closing the
    ///     generics — this is the shape each of the three stores will alias with.
    /// </summary>
    public interface IStoreTransactionParticipant: ITransactionParticipant<SqliteConnection, SqliteTransaction>;

    [Fact]
    public async Task after_commit_defaults_to_a_completed_no_op()
    {
        // A participant declaring only BeforeCommitAsync — the pre-lift Marten/Polecat shape —
        // compiles, and the default AfterCommitAsync completes without doing anything.
        ITransactionParticipant<SqliteConnection, SqliteTransaction> participant = new BeforeOnlyParticipant();

        var task = participant.AfterCommitAsync(CancellationToken.None);

        task.IsCompletedSuccessfully.ShouldBeTrue();
        await task;
    }

    [Fact]
    public async Task an_overridden_after_commit_is_reached_through_the_contract()
    {
        // The default must not shadow an implementation that does override it — a store invoking
        // participants through the shared contract has to reach the override.
        var participant = new RecordingParticipant();
        ITransactionParticipant<SqliteConnection, SqliteTransaction> contract = participant;

        await contract.AfterCommitAsync(CancellationToken.None);

        participant.AfterCommitCalls.ShouldBe(1);
    }

    [Fact]
    public async Task a_participant_of_a_derived_store_interface_is_invoked_through_the_generic_member()
    {
        // The aliasing story in one test: an implementor of the store-shaped derived interface,
        // written with the provider-typed signature it always had, satisfies the generic member
        // implicitly — so a store's commit path can hold participants as the closed generic and
        // invoke them without knowing the derived interface exists.
        var participant = new StoreShapedParticipant();
        ITransactionParticipant<SqliteConnection, SqliteTransaction> contract = participant;

        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await using (var creating = connection.BeginTransaction())
        {
            var create = connection.CreateCommand();
            create.Transaction = creating;
            create.CommandText = "create table participant_rows(id integer primary key)";
            await create.ExecuteNonQueryAsync();
            await creating.CommitAsync();
        }

        await using (var transaction = connection.BeginTransaction())
        {
            await contract.BeforeCommitAsync(connection, transaction, CancellationToken.None);
            await transaction.CommitAsync();
        }

        var count = connection.CreateCommand();
        count.CommandText = "select count(*) from participant_rows";
        (await count.ExecuteScalarAsync()).ShouldBe(1L);
    }

    [Fact]
    public void a_provider_neutral_participant_satisfies_a_closed_shape()
    {
        // The contravariance the interface declares: a participant written once against the base
        // DbConnection/DbTransaction pair is usable wherever any store's closed shape is expected.
        ITransactionParticipant<DbConnection, DbTransaction> neutral = new ProviderNeutralParticipant();

        ITransactionParticipant<SqliteConnection, SqliteTransaction> closed = neutral;

        closed.ShouldBeSameAs(neutral);
    }

    [Fact]
    public void the_type_parameters_are_constrained_to_the_ado_net_base_types()
    {
        // The constraints are the contract's floor — a store cannot close the generics over
        // something that is not an ADO.NET connection/transaction pair, and a participant can
        // always fall back to the DbConnection/DbTransaction members.
        var arguments = typeof(ITransactionParticipant<,>).GetGenericArguments();

        arguments[0].GetGenericParameterConstraints().ShouldContain(typeof(DbConnection));
        arguments[1].GetGenericParameterConstraints().ShouldContain(typeof(DbTransaction));
    }

    private class BeforeOnlyParticipant: ITransactionParticipant<SqliteConnection, SqliteTransaction>
    {
        public Task BeforeCommitAsync(SqliteConnection connection, SqliteTransaction transaction,
            CancellationToken token)
        {
            return Task.CompletedTask;
        }
    }

    private class RecordingParticipant: ITransactionParticipant<SqliteConnection, SqliteTransaction>
    {
        public int AfterCommitCalls { get; private set; }

        public Task BeforeCommitAsync(SqliteConnection connection, SqliteTransaction transaction,
            CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public Task AfterCommitAsync(CancellationToken token)
        {
            AfterCommitCalls++;
            return Task.CompletedTask;
        }
    }

    private class StoreShapedParticipant: IStoreTransactionParticipant
    {
        public async Task BeforeCommitAsync(SqliteConnection connection, SqliteTransaction transaction,
            CancellationToken token)
        {
            // Writes on the connection it was handed, inside the transaction it was handed —
            // the one rule the contract's docs call the point.
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "insert into participant_rows(id) values (1)";
            await command.ExecuteNonQueryAsync(token);
        }
    }

    private class ProviderNeutralParticipant: ITransactionParticipant<DbConnection, DbTransaction>
    {
        public Task BeforeCommitAsync(DbConnection connection, DbTransaction transaction, CancellationToken token)
        {
            return Task.CompletedTask;
        }
    }
}
