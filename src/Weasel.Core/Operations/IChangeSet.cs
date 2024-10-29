#nullable enable
using JasperFx.Events;

namespace Weasel.Core.Operations;

public interface IChangeSet
{
    IEnumerable<object> Updated { get; }
    IEnumerable<object> Inserted { get; }
    IEnumerable<IDeletion> Deleted { get; }

    IEnumerable<IEvent> GetEvents();

    IEnumerable<StreamAction> GetStreams();

    IChangeSet Clone();
}
