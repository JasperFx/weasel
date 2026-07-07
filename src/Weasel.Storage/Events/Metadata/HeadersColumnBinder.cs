#nullable enable
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// <see cref="IEventMetadataBinder"/> for the optional <c>headers</c> JSONB
/// column. Write-only — no read-back. The descriptor builder includes this
/// binder in the array iff the event-store configuration has headers enabled.
/// </summary>
/// <remarks>
/// Demonstrates the "configurable presence" axis: the binder either is or
/// isn't in the descriptor's array. If headers aren't enabled, the binder
/// isn't in the array and the loop iteration count goes down by one — no
/// per-call cost when the feature is off.
/// </remarks>
public sealed class HeadersColumnBinder: IEventMetadataBinder
{
    private readonly IStorageDialect _dialect;

    public HeadersColumnBinder(IStorageDialect dialect)
    {
        _dialect = dialect;
    }

    public string ColumnName => "headers";

    public string ValueSql => "?";

    public void Bind(IGroupedParameterBuilder pb, StreamAction stream, IEvent @event, IStorageSession session)
    {
        // Bind a DBNull placeholder typed as the dialect's json column type,
        // then let WriteToParameter fill in the actual JSON bytes (skipping
        // the intermediate string allocation) or leave the parameter at DBNull
        // when Headers is null. The neutral AppendParameter writes DBNull.Value
        // as-is; the dialect maps StorageColumnType.Json to its provider json
        // type (Postgres jsonb).
        var parameter = pb.AppendParameter(System.DBNull.Value);
        _dialect.SetParameterType(parameter, StorageColumnType.Json);
        session.Serializer.WriteToParameter(parameter, @event.Headers);
    }

    // OnRead — default no-op (inherited from the interface default). Headers
    // are write-only; nothing comes back from the server for this column.
}
