using Npgsql;
using NpgsqlTypes;
using Weasel.Core;
using Weasel.Core.Operations;

namespace Weasel.Postgresql;

public interface ICommandBuilder : ICommandBuilder<NpgsqlParameter, NpgsqlDbType>
{

}
