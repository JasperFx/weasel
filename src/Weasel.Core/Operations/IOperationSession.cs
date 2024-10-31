using Weasel.Core.Serialization;

namespace Weasel.Core.Operations;

public interface IOperationSession
{
    ISerializer Serializer { get; }
    string TenantId { get; }

    int UpdateBatchSize();
}
