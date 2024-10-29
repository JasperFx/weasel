using JasperFx.Core.Reflection;
using JasperFx.Events;

namespace Weasel.Core.Operations;

/// <summary>
/// Marker interface to help Unit of Work tracking
/// </summary>
/// <typeparam name="T"></typeparam>
/// <typeparam name="TId"></typeparam>
public interface IStorageOperation<T, TId>
{
    TId Id { get; }
}

public abstract class UnitOfWorkBase: ISessionWorkTracker
{
    private readonly IStorageSession _parent;
    protected readonly List<IStorageOperation> _eventOperations = new();
    protected readonly List<IStorageOperation> _operations = new();

    protected UnitOfWorkBase(IStorageSession parent)
    {
        _parent = parent;
    }

    protected UnitOfWorkBase(IEnumerable<IStorageOperation> operations)
    {
        _operations.AddRange(operations);
        // TODO -- set parent to be a nullo
    }

    public void Sort()
    {
        if (shouldSort(out var comparer))
        {
            var sorted = _operations.OrderBy(f => f, comparer).ToList();
            _operations.Clear();
            _operations.AddRange(sorted);
        }
    }

    protected abstract bool shouldSort(out IComparer<IStorageOperation> comparer);

    public void Reset()
    {
        _operations.Clear();
        _eventOperations.Clear();
        Streams.Clear();
    }

    public void Add(IStorageOperation operation)
    {
        if (operation is IDocumentStorageOperation o)
        {
            _operations.RemoveAll(x =>
                x is IDocumentStorageOperation && x.As<IDocumentStorageOperation>().Document == o.Document);
        }

        if (operation.DocumentType == typeof(IEvent))
        {
            _eventOperations.Add(operation);
        }
        else
        {
            _operations.Add(operation);
        }
    }

    public IReadOnlyList<IStorageOperation> AllOperations => _eventOperations.Concat(_operations).ToList();

    IEnumerable<IDeletion> IUnitOfWork.Deletions()
    {
        return _operations.OfType<IDeletion>();
    }

    IEnumerable<IDeletion> IUnitOfWork.DeletionsFor<T>()
    {
        return _operations.OfType<IDeletion>().Where(x => x.DocumentType.CanBeCastTo<T>());
    }

    IEnumerable<IDeletion> IUnitOfWork.DeletionsFor(Type documentType)
    {
        return _operations.OfType<IDeletion>().Where(x => x.DocumentType.CanBeCastTo(documentType));
    }

    IEnumerable<object> IUnitOfWork.Updates()
    {
        var fromTrackers = _parent.DetectChangedDocuments();

        return _operations
            .OfType<IDocumentStorageOperation>()
            .Where(x => x.Role() == OperationRole.Update || x.Role() == OperationRole.Upsert)
            .Select(x => x.Document).Union(fromTrackers);
    }

    IEnumerable<object> IUnitOfWork.Inserts()
    {
        return _operations
            .OfType<IDocumentStorageOperation>()
            .Where(x => x.Role() == OperationRole.Insert)
            .Select(x => x.Document);
    }

    IEnumerable<T> IUnitOfWork.UpdatesFor<T>()
    {
        var fromTrackers = _parent.ChangeTrackers
            .Where(x => x.Document.GetType().CanBeCastTo<T>())
            .Where(x => x.DetectChanges(_parent, out var _))
            .Select(x => x.Document).OfType<T>();

        return _operations
            .OfType<IDocumentStorageOperation>()
            .Where(x => x.Role() == OperationRole.Update || x.Role() == OperationRole.Upsert)
            .Select(x => x.Document)
            .OfType<T>()
            .Concat(fromTrackers)
            .Distinct();
    }

    IEnumerable<T> IUnitOfWork.InsertsFor<T>()
    {
        return _operations
            .OfType<IDocumentStorageOperation>()
            .Where(x => x.Role() == OperationRole.Insert)
            .Select(x => x.Document)
            .OfType<T>();
    }

    IEnumerable<T> IUnitOfWork.AllChangedFor<T>()
    {
        var fromTrackers = _parent.ChangeTrackers
            .Where(x => x.Document.GetType().CanBeCastTo<T>())
            .Where(x => x.DetectChanges(_parent, out var _))
            .Select(x => x.Document).OfType<T>();


        return _operations
            .OfType<IDocumentStorageOperation>()
            .Select(x => x.Document)
            .OfType<T>()
            .Concat(fromTrackers)
            .Distinct();
    }

    public List<StreamAction> Streams { get; } = new();

    IList<StreamAction> IUnitOfWork.Streams()
    {
        return Streams;
    }

    IEnumerable<IStorageOperation> IUnitOfWork.Operations()
    {
        return _operations;
    }

    IEnumerable<IStorageOperation> IUnitOfWork.OperationsFor<T>()
    {
        return _operations.Where(x => x.DocumentType.CanBeCastTo<T>());
    }

    IEnumerable<IStorageOperation> IUnitOfWork.OperationsFor(Type documentType)
    {
        return _operations.Where(x => x.DocumentType.CanBeCastTo(documentType));
    }

    IEnumerable<object> IChangeSet.Updated => _operations.OfType<IDocumentStorageOperation>()
        .Where(x => x.Role() == OperationRole.Update || x.Role() == OperationRole.Upsert).Select(x => x.Document);

    IEnumerable<object> IChangeSet.Inserted => _operations.OfType<IDocumentStorageOperation>()
        .Where(x => x.Role() == OperationRole.Insert).Select(x => x.Document);

    IEnumerable<IDeletion> IChangeSet.Deleted => _operations.OfType<IDeletion>();

    IEnumerable<IEvent> IChangeSet.GetEvents()
    {
        return Streams.SelectMany(x => x.Events);
    }

    IEnumerable<StreamAction> IChangeSet.GetStreams()
    {
        return Streams;
    }

    public IChangeSet Clone()
    {
        var clone = cloneChangeSet(_operations);
        clone.Streams.AddRange(Streams);

        return clone;
    }

    protected abstract UnitOfWorkBase cloneChangeSet(IEnumerable<IStorageOperation> operations);

    public void Eject<T>(T document)
    {
        var operations = operationsFor(typeof(T));
        var matching = operations.OfType<IDocumentStorageOperation>().Where(x => ReferenceEquals(document, x.Document))
            .ToArray();

        foreach (var operation in matching) _operations.Remove(operation);
    }

    public void EjectAllOfType(Type type)
    {
        var operations = operationsFor(type);
        var matching = operations.OfType<IDocumentStorageOperation>().ToArray();

        foreach (var operation in matching) _operations.Remove(operation);
    }

    private IEnumerable<IStorageOperation> operationsFor(Type documentType)
    {
        return _operations.Where(x => x.DocumentType == documentType);
    }

    public bool HasOutstandingWork()
    {
        return _operations.Any() || Streams.Any(x => x.Events.Count > 0) || _eventOperations.Any();
    }

    public void EjectAll()
    {
        _operations.Clear();
        _eventOperations.Clear();
        Streams.Clear();
    }

    public void PurgeOperations<T, TId>(TId id) where T : notnull
    {
        _operations.RemoveAll(op => op is IStorageOperation<T, TId> storage && storage.Id.Equals(id));
    }

    public bool TryFindStream(string streamKey, out StreamAction stream)
    {
        stream = Streams
            .FirstOrDefault(x => x.Key == streamKey);

        return stream != null;
    }

    public bool TryFindStream(Guid streamId, out StreamAction stream)
    {
        stream = Streams
            .FirstOrDefault(x => x.Id == streamId);

        return stream != null;
    }
}
