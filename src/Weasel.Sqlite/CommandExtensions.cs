using System.Data;
using Microsoft.Data.Sqlite;

namespace Weasel.Sqlite;

public static class CommandExtensions
{
    public static SqliteParameter AddParameter(this SqliteCommand command, object value, SqliteType? dbType = null)
    {
        return SqliteProvider.Instance.AddParameter(command, value, dbType);
    }

    /// <summary>
    ///     Finds or adds a new parameter with the specified name and returns the parameter
    /// </summary>
    /// <param name="command"></param>
    /// <param name="name"></param>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    /// <returns></returns>
    public static SqliteParameter AddNamedParameter(this SqliteCommand command, string name, object value,
        SqliteType? dbType = null)
    {
        return SqliteProvider.Instance.AddNamedParameter(command, name, value, dbType);
    }

    public static SqliteCommand Returns(this SqliteCommand command, string name, SqliteType type)
    {
        var parameter = command.AddParameter(name);
        parameter.ParameterName = name;
        parameter.SqliteType = type;
        parameter.Direction = ParameterDirection.ReturnValue;
        return command;
    }

    public static SqliteCommand CreateCommand(this SqliteConnection conn, string command, SqliteTransaction? tx = null)
    {
        var cmd = new SqliteCommand(command, conn);
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
    public static SqliteCommand With(this SqliteCommand command, string name, object value, SqliteType? dbType = null)
    {
        SqliteProvider.Instance.AddNamedParameter(command, name, value, dbType);
        return command;
    }

    public static SqliteCommand With(this SqliteCommand command, string name, DateTime value)
    {
        SqliteProvider.Instance.AddNamedParameter(command, name, value, SqliteType.Text);
        return command;
    }

    public static SqliteCommand With(this SqliteCommand command, string name, DateTimeOffset value)
    {
        SqliteProvider.Instance.AddNamedParameter(command, name, value, SqliteType.Text);
        return command;
    }
}
