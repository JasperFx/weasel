#nullable enable
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
///     Dialect-neutral storage operation: <see cref="Weasel.Core.IStorageOperation"/>
///     (DocumentType / Role / PostprocessAsync) plus the command-configuration entry point the
///     closed-shape unit of work drives against the neutral <see cref="ICommandBuilder"/>.
///     A store's own operation contract typically derives from this and re-declares
///     <c>ConfigureCommand</c> with its dialect-typed command builder, bridging back to this
///     neutral slot with a default interface method (the same pattern the Weasel SQL-generation
///     contracts use).
/// </summary>
public interface IStorageOperation: Core.IStorageOperation
{
    void ConfigureCommand(ICommandBuilder builder, IStorageSession session);
}
