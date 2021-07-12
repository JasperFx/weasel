using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Baseline;
using Npgsql;
using NpgsqlTypes;
using Weasel.Core;

namespace Weasel.Postgresql
{
    public static class CommandExtensions
    {
        public static NpgsqlParameter AddParameter(this NpgsqlCommand command, object value, NpgsqlDbType? dbType = null)
        {
            return PostgresqlProvider.Instance.AddParameter(command, value, dbType);
        }


        /// <summary>
        /// Finds or adds a new parameter with the specified name and returns the parameter
        /// </summary>
        /// <param name="command"></param>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <param name="dbType"></param>
        /// <returns></returns>
        public static NpgsqlParameter AddNamedParameter(this NpgsqlCommand command, string name, object value, NpgsqlDbType? dbType = null)
        {
            return PostgresqlProvider.Instance.AddNamedParameter(command, name, value, dbType);
        }
        


        public static NpgsqlCommand With(this NpgsqlCommand command, string name, string[] value)
        {
            PostgresqlProvider.Instance.AddNamedParameter(command, name, value, NpgsqlDbType.Array | NpgsqlDbType.Varchar);
            return command;
        }


        public static NpgsqlCommand Returns(this NpgsqlCommand command, string name, NpgsqlDbType type)
        {
            var parameter = command.AddParameter(name);
            parameter.ParameterName = name;
            parameter.NpgsqlDbType = type;
            parameter.Direction = ParameterDirection.ReturnValue;
            return command;
        }

        public static NpgsqlCommand CreateCommand(this NpgsqlConnection conn, string command, NpgsqlTransaction tx = null)
        {
            return new NpgsqlCommand(command, conn)
            {
                Transaction = tx
            };
        }

    }
}
