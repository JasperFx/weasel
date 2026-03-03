using System.Data.Common;

namespace Weasel.Core;

public interface IStorageOperation
{
    Type DocumentType { get; }
    OperationRole Role();
    Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token);
}
