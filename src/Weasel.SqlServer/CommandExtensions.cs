using System;
using System.Data;
using System.Data.SqlClient;
using Weasel.Core;

namespace Weasel.SqlServer
{
    public static class CommandExtensions
    {
        public static SqlParameter AddParameter(this SqlCommand command, object value, SqlDbType? dbType = null)
        {
            return SqlServerProvider.Instance.AddParameter(command, value, dbType);
        }


        /// <summary>
        ///     Finds or adds a new parameter with the specified name and returns the parameter
        /// </summary>
        /// <param name="command"></param>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <param name="dbType"></param>
        /// <returns></returns>
        public static SqlParameter AddNamedParameter(this SqlCommand command, string name, object value,
            SqlDbType? dbType = null)
        {
            return SqlServerProvider.Instance.AddNamedParameter(command, name, value, dbType);
        }

        public static SqlCommand Returns(this SqlCommand command, string name, SqlDbType type)
        {
            var parameter = command.AddParameter(name);
            parameter.ParameterName = name;
            parameter.SqlDbType = type;
            parameter.Direction = ParameterDirection.ReturnValue;
            return command;
        }

        public static SqlCommand CreateCommand(this SqlConnection conn, string command, SqlTransaction tx = null)
        {
            return new(command, conn)
            {
                Transaction = tx
            };
        }

        /// <summary>
        /// Finds or adds a new parameter with the specified name and returns the parameter
        /// </summary>
        /// <param name="command"></param>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <param name="dbType"></param>
        /// <returns></returns>
        public static SqlCommand With(this SqlCommand command, string name, object value, SqlDbType? dbType = null)
        {
            SqlServerProvider.Instance.AddNamedParameter(command, name, value, dbType);
            return command;
        }
        
        public static SqlCommand With(this SqlCommand command, string name, DateTime value)
        {
            SqlServerProvider.Instance.AddNamedParameter(command, name, value, SqlDbType.DateTime);
            return command;
        }
        
        public static SqlCommand With(this SqlCommand command, string name, DateTimeOffset value)
        {
            SqlServerProvider.Instance.AddNamedParameter(command, name, value, SqlDbType.DateTimeOffset);
            return command;
        }



    }
}