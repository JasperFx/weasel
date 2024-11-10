using JasperFx;
using Weasel.Core.Serialization;

namespace Weasel.Core.Operations;

public interface IStorageSession : IOperationContext
{
    ISerializer Serializer { get; }

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
