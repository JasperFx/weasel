using System.Data.Common;

namespace Weasel.Core.Operations;

public interface IQueryHandler<TCommandBuilder, TSession>
    where TSession : IStorageSession
{
    void ConfigureCommand(TCommandBuilder builder, TSession session);
}

public interface IQueryHandler<T, TCommandBuilder, TSession> : IQueryHandler<TCommandBuilder, TSession>
    where TSession : IStorageSession
{
    T Handle(DbDataReader reader, TSession session);

    Task<T> HandleAsync(DbDataReader reader, TSession session, CancellationToken token);

    Task<int> StreamJson(Stream stream, DbDataReader reader, CancellationToken token);
}




