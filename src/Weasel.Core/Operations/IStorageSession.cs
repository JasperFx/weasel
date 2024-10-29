using JasperFx;
using Weasel.Core.Operations.DirtyTracking;
using Weasel.Core.Serialization;

namespace Weasel.Core.Operations;

public interface IStorageSession : IOperationContext
{
    ISerializer Serializer { get; }
    IList<IChangeTracker> ChangeTrackers { get; }

    int UpdateBatchSize();
    DbObjectName TableNameFor(Type documentType);

    IEnumerable<object> DetectChangedDocuments();

    /// <summary>
    /// Meant to facilitate automatic dirty checking
    /// </summary>
    /// <param name="document"></param>
    /// <typeparam name="T"></typeparam>
    IStorageOperation UpsertDirtyCheckedDocument<T>(T document);
}
