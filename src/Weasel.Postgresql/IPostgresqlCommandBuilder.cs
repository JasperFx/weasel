using Npgsql;
using NpgsqlTypes;
using Weasel.Core.Operations;

namespace Weasel.Postgresql;

public interface IPostgresqlCommandBuilder : ICommandBuilder
{
    NpgsqlParameter AppendParameter(object? value, NpgsqlDbType? dbType);
}
