#nullable enable
using System;

namespace Weasel.Storage;

/// <summary>
/// Dialect-supplied factories for the event-store auxiliary write operations that sit alongside the
/// append/stream lifecycle: archive/un-archive, tombstone (hard delete), and projection-progression
/// upsert. These are append-mode-independent (the same regardless of Rich/Quick/QuickWithServerTimestamps)
/// but their SQL is dialect-specific — SQL Server uses <c>SYSDATETIMEOFFSET()</c> and a <c>MERGE</c> for
/// the progression upsert; Postgres uses <c>now()</c> and <c>ON CONFLICT</c>. A store's
/// <see cref="IEventStoreSqlDialect"/> vends this record from
/// <see cref="IEventStoreSqlDialect.BuildAuxiliaryOperations"/>; the built <see cref="EventStorage{TId}"/>
/// exposes them through <see cref="EventStorage{TId}.ArchiveStream"/> / <see cref="EventStorage{TId}.TombstoneStream"/>
/// / <see cref="EventStorage{TId}.UpdateProgress"/>.
/// </summary>
/// <remarks>
/// Every factory is optional (nullable). A dialect that does not supply a given factory leaves the
/// corresponding <see cref="EventStorage{TId}"/> method throwing <see cref="NotSupportedException"/>, so
/// adding this seam does not force any existing dialect to implement it — stores that still keep these
/// operations bespoke are unaffected.
/// </remarks>
public sealed record EventAuxiliaryOperations(
    Func<object, string, bool, IStorageOperation>? ArchiveStream = null,
    Func<object, string, IStorageOperation>? TombstoneStream = null,
    Func<string, long, bool, IStorageOperation>? UpdateProgress = null);
