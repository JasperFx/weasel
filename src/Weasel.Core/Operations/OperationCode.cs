using System.Data.Common;
using Weasel.Core.Serialization;

namespace Weasel.Core.Operations;

public interface IOperationSession
{
    ISerializer Serializer { get; }
    public string TenantId { get; }
}

public interface IQueryHandler<TCommandBuilder, TSession>
    where TSession : IOperationSession
{
    void ConfigureCommand(TCommandBuilder builder, TSession session);
}

public interface IQueryHandler<T, TCommandBuilder, TSession> : IQueryHandler<TCommandBuilder, TSession>
    where TSession : IOperationSession
{
    T Handle(DbDataReader reader, TSession session);

    Task<T> HandleAsync(DbDataReader reader, TSession session, CancellationToken token);

    Task<int> StreamJson(Stream stream, DbDataReader reader, CancellationToken token);
}

public interface IStorageOperation<TCommandBuilder, TSession>: IQueryHandler<TCommandBuilder, TSession>
    where TSession : IOperationSession
{
    Type DocumentType { get; }

    void Postprocess(DbDataReader reader, IList<Exception> exceptions);

    Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token);

    OperationRole Role();
}

