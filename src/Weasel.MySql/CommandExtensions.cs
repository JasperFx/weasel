using MySqlConnector;

namespace Weasel.MySql;

public static class CommandExtensions
{
    public static MySqlCommand With(this MySqlCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
        return command;
    }

    public static MySqlCommand With(this MySqlCommand command, string name, object? value, MySqlDbType dbType)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        parameter.MySqlDbType = dbType;
        command.Parameters.Add(parameter);
        return command;
    }

    public static MySqlCommand CreateCommand(this MySqlConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd;
    }
}
