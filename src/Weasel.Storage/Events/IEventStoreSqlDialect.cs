#nullable enable
using JasperFx.Events;

namespace Weasel.Storage;

/// <summary>
/// Dialect seam for the closed-shape event-storage hierarchy. A store ships
/// its own implementation (Marten the Postgres one; Polecat the SQL-Server
/// one). The dialect builds per-mode descriptors end-to-end — SQL strings,
/// metadata-column ordering, and binder selection are all joint concerns the
/// dialect owns, so the SQL stays aligned with the parameter binds.
/// </summary>
/// <remarks>
/// <para>
/// One method per append mode returns the whole descriptor. The dialect's
/// implementation knows which columns are in the SQL (config-aware) and
/// which binders bind each metadata column, in lockstep. The event-store
/// configuration arrives as the neutral <see cref="EventRegistry"/> (a
/// store's own event graph derives from it and the dialect downcasts) and
/// serialization through the neutral <see cref="IStorageSerializer"/>.
/// </para>
/// </remarks>
public interface IEventStoreSqlDialect
{
    RichEventStorageDescriptor BuildRichDescriptor(EventRegistry graph, IStorageSerializer serializer);

    QuickEventStorageDescriptor BuildQuickDescriptor(EventRegistry graph, IStorageSerializer serializer);

    QuickWithServerTimestampsEventStorageDescriptor BuildQuickWithServerTimestampsDescriptor(
        EventRegistry graph, IStorageSerializer serializer);
}
