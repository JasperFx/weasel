using System.Data.Common;

namespace Weasel.Core.Operations;

public interface IStorageOperation<TCommandBuilder, TSession>: IQueryHandler<TCommandBuilder, TSession>
    where TSession : IOperationSession
{
    Type DocumentType { get; }

    void Postprocess(DbDataReader reader, IList<Exception> exceptions);

    Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token);

    OperationRole Role();
}
