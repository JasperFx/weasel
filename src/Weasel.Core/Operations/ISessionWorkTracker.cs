using JasperFx.Events;

namespace Weasel.Core.Operations;

public interface ISessionWorkTracker: IUnitOfWork, IChangeSet
{
    new List<StreamAction> Streams { get; }
    IReadOnlyList<IStorageOperation> AllOperations { get; }
    void Reset();
    void Add(IStorageOperation operation);
    void Sort();
    void Eject<T>(T document);
    void EjectAllOfType(Type type);
    bool TryFindStream(string streamKey, out StreamAction stream);
    bool TryFindStream(Guid streamId, out StreamAction stream);
    bool HasOutstandingWork();
    void EjectAll();

    /// <summary>
    /// Remove all outstanding operations for the designated document
    /// </summary>
    /// <param name="id"></param>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TId"></typeparam>
    void PurgeOperations<T, TId>(TId id) where T : notnull;
}
