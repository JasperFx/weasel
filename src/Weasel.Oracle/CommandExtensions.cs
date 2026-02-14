using System.Data;
using Oracle.ManagedDataAccess.Client;

namespace Weasel.Oracle;

public static class CommandExtensions
{
    public static OracleParameter AddParameter(this OracleCommand command, object value, OracleDbType? dbType = null)
    {
        return OracleProvider.Instance.AddParameter(command, value, dbType);
    }

    /// <summary>
    ///     Finds or adds a new parameter with the specified name and returns the parameter
    /// </summary>
    /// <param name="command"></param>
    /// <param name="name"></param>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    /// <returns></returns>
    public static OracleParameter AddNamedParameter(this OracleCommand command, string name, object value,
        OracleDbType? dbType = null)
    {
        return OracleProvider.Instance.AddNamedParameter(command, name, value, dbType);
    }

    public static OracleCommand Returns(this OracleCommand command, string name, OracleDbType type)
    {
        var parameter = command.AddParameter(name);
        parameter.ParameterName = name;
        parameter.OracleDbType = type;
        parameter.Direction = ParameterDirection.ReturnValue;
        return command;
    }

    public static OracleCommand CreateCommand(this OracleConnection conn, string command, OracleTransaction? tx = null)
    {
        var cmd = new OracleCommand(command, conn);
        cmd.BindByName = true;
        if (tx != null)
        {
            cmd.Transaction = tx;
        }
        return cmd;
    }

    /// <summary>
    ///     Finds or adds a new parameter with the specified name and returns the command
    /// </summary>
    /// <param name="command"></param>
    /// <param name="name"></param>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    /// <returns></returns>
    public static OracleCommand With(this OracleCommand command, string name, object value, OracleDbType? dbType = null)
    {
        if (value is Guid guidValue)
            return With(command, name, guidValue);
        OracleProvider.Instance.AddNamedParameter(command, name, value, dbType);
        return command;
    }

    public static OracleCommand With(this OracleCommand command, string name, DateTime value)
    {
        OracleProvider.Instance.AddNamedParameter(command, name, value, OracleDbType.Date);
        return command;
    }

    public static OracleCommand With(this OracleCommand command, string name, DateTimeOffset value)
    {
        OracleProvider.Instance.AddNamedParameter(command, name, value, OracleDbType.TimeStampTZ);
        return command;
    }

    public static OracleCommand With(this OracleCommand command, string name, Guid value)
    {
        command.Parameters.Add(new OracleParameter(name, OracleDbType.Raw) { Value = value.ToByteArray() });
        return command;
    }
}
