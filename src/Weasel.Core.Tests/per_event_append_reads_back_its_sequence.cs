using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Exceptions;
using JasperFx.Events;
using Shouldly;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     marten#5454. The Quick modes' per-event <c>QuickAppendEventWithVersion</c> INSERT draws its <c>seq_id</c>
///     from a server-side <c>nextval()</c>. When the dialect says its suffix returns that value, the operation must
///     read it back onto <see cref="IEvent.Sequence" />, or a failed unit of work cannot tombstone the number.
/// </summary>
public class per_event_append_reads_back_its_sequence
{
    public record SomethingHappened(string Name);

    private sealed class FakeDialect(bool returnsSequence): IEventStoreSqlDialect
    {
        public RichEventStorageDescriptor BuildRichDescriptor(EventRegistry graph, IStorageSerializer serializer)
            => throw new NotSupportedException();

        public QuickEventStorageDescriptor BuildQuickDescriptor(EventRegistry graph, IStorageSerializer serializer)
            => new(string.Empty, string.Empty, string.Empty, _ => "{}", _ => null)
            {
                IsGuidStreamIdentity = true, AppendEventSqlReturnsSequence = returnsSequence
            };

        public QuickWithServerTimestampsEventStorageDescriptor BuildQuickWithServerTimestampsDescriptor(
            EventRegistry graph, IStorageSerializer serializer)
            => new(string.Empty, string.Empty, string.Empty, _ => "{}", _ => null)
            {
                IsGuidStreamIdentity = true, AppendEventSqlReturnsSequence = returnsSequence
            };
    }

    private static (IStorageOperation operation, IEvent @event) BuildOperation(EventAppendMode mode,
        bool returnsSequence)
    {
        var storage = EventStorageBuilder.Build<Guid>(new FakeDialect(returnsSequence), mode, null!, null!);
        var @event = new Event<SomethingHappened>(new SomethingHappened("one")) { Version = 1 };
        var stream = StreamAction.Start(Guid.NewGuid(), [@event]);
        return (storage.QuickAppendEventWithVersion(stream, @event), @event);
    }

    [Theory]
    [InlineData(EventAppendMode.Quick)]
    [InlineData(EventAppendMode.QuickWithServerTimestamps)]
    public void without_the_flag_the_operation_still_returns_no_data(EventAppendMode mode)
    {
        var (operation, _) = BuildOperation(mode, returnsSequence: false);

        operation.ShouldBeAssignableTo<NoDataReturnedCall>();
    }

    [Theory]
    [InlineData(EventAppendMode.Quick)]
    [InlineData(EventAppendMode.QuickWithServerTimestamps)]
    public async Task with_the_flag_the_operation_writes_the_returned_sequence_onto_the_event(EventAppendMode mode)
    {
        var (operation, @event) = BuildOperation(mode, returnsSequence: true);

        // The batch reader advances only past operations that are NOT marked as returning no data.
        operation.ShouldNotBeAssignableTo<NoDataReturnedCall>();
        operation.ShouldBeAssignableTo<IExceptionTransform>();

        using var table = new DataTable();
        table.Columns.Add("seq_id", typeof(long));
        table.Rows.Add(42L);
        await using var reader = table.CreateDataReader();

        await operation.PostprocessAsync(reader, new List<Exception>(), CancellationToken.None);

        @event.Sequence.ShouldBe(42L);
    }
}
