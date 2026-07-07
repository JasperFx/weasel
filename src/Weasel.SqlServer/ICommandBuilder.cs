using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Weasel.SqlServer;

/// <summary>
///     SQL Server command-builder surface. Derives from the dialect-neutral
///     <see cref="Weasel.Core.ICommandBuilder" /> (which contributes <see cref="Weasel.Core.ICommandBuilder.Append(string)" />,
///     <see cref="Weasel.Core.ICommandBuilder.AppendWithDbParameters(string)" />, <c>AddParameters</c>, tenant id, etc.)
///     and adds the SqlClient-typed overloads that return <see cref="SqlParameter" />.
/// </summary>
public interface ICommandBuilder: Weasel.Core.ICommandBuilder
{
    SqlParameter AppendParameter<T>(T value);
    SqlParameter AppendParameter<T>(T value, SqlDbType dbType);

    /// <summary>
    ///     SqlClient-typed override of <see cref="Weasel.Core.ICommandBuilder.AppendParameter(object)" />.
    /// </summary>
    new SqlParameter AppendParameter(object value);
    SqlParameter AppendParameter(object? value, SqlDbType? dbType);

    /// <summary>
    ///     SqlClient-typed override of
    ///     <see cref="Weasel.Core.ICommandBuilder.CreateGroupedParameterBuilder(char?)" /> returning the
    ///     provider grouped-parameter builder.
    /// </summary>
    new IGroupedParameterBuilder CreateGroupedParameterBuilder(char? seperator = null);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <returns></returns>
    SqlParameter[] AppendWithParameters(string text);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="placeholder"></param>
    /// <returns></returns>
    SqlParameter[] AppendWithParameters(string text, char placeholder);
}

/// <summary>
///     SQL Server grouped-parameter surface. Derives from the dialect-neutral
///     <see cref="Weasel.Core.IGroupedParameterBuilder" /> and adds the SqlClient-typed overloads that
///     return <see cref="SqlParameter" />.
/// </summary>
public interface IGroupedParameterBuilder: Weasel.Core.IGroupedParameterBuilder
{
    SqlParameter AppendParameter<T>(T? value) where T : notnull;
    SqlParameter AppendParameter<T>(T? value, SqlDbType dbType) where T : notnull;
}

public sealed class GroupedParameterBuilder: IGroupedParameterBuilder
{
    private readonly ICommandBuilder _commandBuilder;
    private readonly char? _seperator;
    private int _count;

    public GroupedParameterBuilder(ICommandBuilder commandBuilder, char? seperator)
    {
        _commandBuilder = commandBuilder;
        _seperator = seperator;
    }

    public SqlParameter AppendParameter<T>(T? value) where T : notnull
    {
        if (_count > 0 && _seperator.HasValue)
            _commandBuilder.Append(_seperator.Value);

        _count++;
        return _commandBuilder.AppendParameter(value);
    }

    public SqlParameter AppendParameter<T>(T? value, SqlDbType dbType) where T : notnull
    {
        if (_count > 0 && _seperator.HasValue)
            _commandBuilder.Append(_seperator.Value);

        _count++;
        return _commandBuilder.AppendParameter(value, dbType);
    }

    DbParameter Weasel.Core.IGroupedParameterBuilder.AppendParameter(object? value)
    {
        if (_count > 0 && _seperator.HasValue)
            _commandBuilder.Append(_seperator.Value);

        _count++;
        return _commandBuilder.AppendParameter(value ?? DBNull.Value);
    }
}
