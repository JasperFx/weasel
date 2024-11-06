using Weasel.Core.Serialization;

namespace Weasel.Core.Operations;

public interface IOperationSession
{
    ISerializer Serializer { get; }
    string TenantId { get; }

    int UpdateBatchSize();
    DbObjectName TableNameFor(Type documentType);

    // TODO -- encapsulate the following two methods!!!!

    IEnumerable<object> DetectChangedDocuments();

    /// <summary>
    /// Meant to facilitate automatic dirty checking
    /// </summary>
    /// <param name="document"></param>
    /// <typeparam name="T"></typeparam>
    void UpsertDirtyCheckedDocument<T>(T document);
}
