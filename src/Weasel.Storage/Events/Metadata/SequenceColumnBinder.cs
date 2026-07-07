#nullable enable
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// <see cref="IEventMetadataBinder"/> for the <c>seq_id</c> column on the
/// Rich (full-mode) append path. Binds <see cref="IEvent.Sequence"/> as a
/// bigint parameter — the appender pre-fetches a queue of sequence numbers
/// and assigns them to each event before <c>AppendEvent(...)</c> is called,
/// so the value is already populated on <see cref="IEvent.Sequence"/> by the
/// time this binder runs.
/// </summary>
public sealed class SequenceColumnBinder: IEventMetadataBinder
{
    private readonly IStorageDialect _dialect;

    public SequenceColumnBinder(IStorageDialect dialect)
    {
        _dialect = dialect;
    }

    public string ColumnName => "seq_id";
    public string ValueSql => "?";

    public void Bind(IGroupedParameterBuilder pb, StreamAction stream, IEvent @event, IStorageSession session)
    {
        var parameter = pb.AppendParameter(@event.Sequence);
        _dialect.SetParameterType(parameter, StorageColumnType.Long);
    }
}
