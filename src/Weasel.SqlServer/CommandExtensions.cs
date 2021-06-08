using System;
using System.Data;
using System.Data.SqlClient;

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


        public static SqlCommand With(this SqlCommand command, string name, Guid value)
        {
            SqlServerProvider.Instance.AddNamedParameter(command, name, value);
            return command;
        }

        public static SqlCommand With(this SqlCommand command, string name, string value)
        {
            SqlServerProvider.Instance.AddNamedParameter(command, name, value);
            return command;
        }

        public static SqlCommand With(this SqlCommand command, string name, object value)
        {
            SqlServerProvider.Instance.AddNamedParameter(command, name, value);
            return command;
        }

        public static SqlCommand With(this SqlCommand command, string name, object value, SqlDbType dbType)
        {
            SqlServerProvider.Instance.AddNamedParameter(command, name, value, dbType);
            return command;
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


        public static SqlCommand CallsSproc(this SqlCommand cmd, DbObjectName function)
        {
            if (cmd == null)
            {
                throw new ArgumentNullException(nameof(cmd));
            }

            if (function == null)
            {
                throw new ArgumentNullException(nameof(function));
            }

            cmd.CommandText = function.QualifiedName;
            cmd.CommandType = CommandType.StoredProcedure;

            return cmd;
        }
    }
}