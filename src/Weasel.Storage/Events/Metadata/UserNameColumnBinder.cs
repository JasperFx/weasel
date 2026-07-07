#nullable enable
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// <see cref="IEventMetadataBinder"/> for the optional <c>user_name</c>
/// varchar column. Binds <see cref="IEvent.UserName"/> as a string parameter.
/// </summary>
/// <remarks>
/// The session-level "user name" lives on <see cref="IMetadataContext.LastModifiedBy"/>,
/// but the event appender plumbs that into the per-event
/// <see cref="IEvent.UserName"/> before queuing the operation, and we bind
/// off the event so the SQL stays self-contained.
/// </remarks>
public sealed class UserNameColumnBinder: IEventMetadataBinder
{
    private readonly IStorageDialect _dialect;

    public UserNameColumnBinder(IStorageDialect dialect)
    {
        _dialect = dialect;
    }

    public string ColumnName => "user_name";
    public string ValueSql => "?";

    public void Bind(IGroupedParameterBuilder pb, StreamAction stream, IEvent @event, IStorageSession session)
    {
        var parameter = pb.AppendParameter(@event.UserName);
        _dialect.SetParameterType(parameter, StorageColumnType.String);
    }
}
