#nullable enable
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// <see cref="IEventMetadataBinder"/> for the optional <c>causation_id</c>
/// varchar column. Binds <see cref="IEvent.CausationId"/> as a string
/// parameter. Included in the descriptor's binder array iff causation-id
/// metadata is enabled. Write-only — the read-back goes through the events
/// table's own column reader.
/// </summary>
public sealed class CausationIdColumnBinder: IEventMetadataBinder
{
    private readonly IStorageDialect _dialect;

    public CausationIdColumnBinder(IStorageDialect dialect)
    {
        _dialect = dialect;
    }

    public string ColumnName => "causation_id";
    public string ValueSql => "?";

    public void Bind(IGroupedParameterBuilder pb, StreamAction stream, IEvent @event, IStorageSession session)
    {
        var parameter = pb.AppendParameter(@event.CausationId);
        _dialect.SetParameterType(parameter, StorageColumnType.String);
    }
}
