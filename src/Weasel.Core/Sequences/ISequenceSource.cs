using System;

namespace Weasel.Core.Sequences;

/// <summary>
///     Resolves the Hi-Lo <see cref="ISequence" /> that backs a given document type's numeric id
///     generation. The database-agnostic seam the closed-shape identity strategies
///     (<see cref="Weasel.Core.Identity.IIdentification{TDoc,TId}" />) use instead of a store-specific
///     database type. Each store (Marten, Polecat) implements this over its own sequence registry.
/// </summary>
public interface ISequenceSource
{
    /// <summary>
    ///     The Hi-Lo sequence keyed to <paramref name="documentType" />.
    /// </summary>
    ISequence SequenceFor(Type documentType);
}
