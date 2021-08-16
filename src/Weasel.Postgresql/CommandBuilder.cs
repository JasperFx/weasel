using System;
using Npgsql;
using NpgsqlTypes;
using Weasel.Core;

namespace Weasel.Postgresql
{
    public class CommandBuilder : CommandBuilderBase<NpgsqlCommand, NpgsqlParameter, NpgsqlConnection, NpgsqlTransaction, NpgsqlDbType, NpgsqlDataReader>
    {
        public CommandBuilder() : this(new NpgsqlCommand())
        {
        }

        public CommandBuilder(NpgsqlCommand command) : base(PostgresqlProvider.Instance, ':', command)
        {
        }

        /// <summary>
        ///     Append a parameter with the supplied value to the underlying command parameter
        ///     collection and adds the parameter usage to the SQL
        /// </summary>
        /// <param name="value"></param>
        /// <param name="dbType"></param>
        public void AppendParameter(string[] values)
        {
            AppendParameter(values, NpgsqlDbType.Varchar | NpgsqlDbType.Array);
        }


    }
}