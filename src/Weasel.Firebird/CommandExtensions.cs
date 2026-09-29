using FirebirdSql.Data.FirebirdClient;

namespace Weasel.Firebird;

public static class CommandExtensions
{
    public static FbParameter AddParameter(this FbCommand command, object? value, FbDbType? dbType = null)
    {
        return FirebirdProvider.Instance.AddParameter(command, FirebirdProvider.NormalizeValue(value),
            dbType ?? (value is DateTimeOffset ? FbDbType.TimeStampTZ : null));
    }

    /// <summary>
    ///     Finds or adds a new parameter with the specified name and returns the parameter
    /// </summary>
    public static FbParameter AddNamedParameter(this FbCommand command, string name, object? value,
        FbDbType? dbType = null)
    {
        var parameter = FirebirdProvider.Instance.AddNamedParameter(command, name,
            FirebirdProvider.NormalizeValue(value), dbType ?? (value is DateTimeOffset ? FbDbType.TimeStampTZ : null));

        return parameter;
    }

    public static FbCommand CreateCommand(this FbConnection conn, string sql, FbTransaction? tx = null)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        if (tx != null)
        {
            cmd.Transaction = tx;
        }

        return cmd;
    }

    /// <summary>
    ///     Finds or adds a new parameter with the specified name and returns the command
    /// </summary>
    public static FbCommand With(this FbCommand command, string name, object? value, FbDbType? dbType = null)
    {
        command.AddNamedParameter(name, value, dbType);
        return command;
    }
}
