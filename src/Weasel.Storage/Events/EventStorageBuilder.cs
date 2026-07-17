#nullable enable
using System;
using JasperFx.Events;

namespace Weasel.Storage;

/// <summary>
/// Factory for the per-mode <see cref="EventStorage{TId}"/> subclass that matches
/// an event store's <see cref="EventAppendMode"/>. Picks ONE concrete subclass
/// per store; no per-call append-mode branching after startup. Relocated from
/// Marten (event E3) and made dialect-agnostic: it receives the
/// <see cref="IEventStoreSqlDialect"/> and append mode rather than hardcoding a
/// Postgres dialect, so Marten passes its <c>PostgresEventStoreDialect</c> and
/// Polecat passes its SQL-Server dialect.
/// </summary>
public static class EventStorageBuilder
{
    public static EventStorage<TId> Build<TId>(
        IEventStoreSqlDialect dialect,
        EventAppendMode appendMode,
        EventRegistry graph,
        IStorageSerializer serializer)
    {
        // The dialect owns descriptor construction end-to-end — SQL strings and
        // metadata-binder ordering are joint concerns it builds in lockstep.
        EventStorage<TId> storage = appendMode switch
        {
            EventAppendMode.Rich =>
                new RichEventStorage<TId>(dialect.BuildRichDescriptor(graph, serializer)),

            EventAppendMode.Quick =>
                new QuickEventStorage<TId>(dialect.BuildQuickDescriptor(graph, serializer)),

            EventAppendMode.QuickWithServerTimestamps =>
                new QuickWithServerTimestampsEventStorage<TId>(
                    dialect.BuildQuickWithServerTimestampsDescriptor(graph, serializer)),

            _ => throw new ArgumentOutOfRangeException(nameof(appendMode),
                $"Unsupported EventAppendMode for the closed-shape event-storage hierarchy: {appendMode}.")
        };

        // Append-mode-independent auxiliary operations (archive / tombstone / progression). Null unless
        // the dialect opts in; leaves the EventStorage methods throwing NotSupportedException otherwise.
        storage.AuxiliaryOperations = dialect.BuildAuxiliaryOperations(graph);

        return storage;
    }
}
