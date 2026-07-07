#nullable enable
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// <see cref="IEventMetadataBinder"/> for the optional <c>correlation_id</c>
/// varchar column. Symmetric with <see cref="CausationIdColumnBinder"/>.
/// </summary>
public sealed class CorrelationIdColumnBinder: IEventMetadataBinder
{
    private readonly IStorageDialect _dialect;

    public CorrelationIdColumnBinder(IStorageDialect dialect)
    {
        _dialect = dialect;
    }

    public string ColumnName => "correlation_id";
    public string ValueSql => "?";

    public void Bind(IGroupedParameterBuilder pb, StreamAction stream, IEvent @event, IStorageSession session)
    {
        var parameter = pb.AppendParameter(@event.CorrelationId);
        _dialect.SetParameterType(parameter, StorageColumnType.String);
    }
}
