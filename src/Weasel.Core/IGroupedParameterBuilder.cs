using System.Data.Common;

namespace Weasel.Core;

/// <summary>
///     Dialect-neutral surface for appending a separated group of positional parameters against an
///     <see cref="ICommandBuilder" />. A database-agnostic consumer (e.g. a shared closed-shape storage
///     runtime) can bind a run of parameters without referencing <c>Weasel.Postgresql</c> or
///     <c>Weasel.SqlServer</c>. Each provider's own <c>IGroupedParameterBuilder</c> derives from this
///     and adds the provider-typed <c>AppendParameter</c> overloads (returning the native parameter).
/// </summary>
public interface IGroupedParameterBuilder
{
    /// <summary>
    ///     Append the next parameter in the group through the ADO.NET-neutral value path, emitting the
    ///     group separator first for every parameter after the first. A <see langword="null" /> value is
    ///     written as <see cref="DBNull" />. Returns the newly created parameter upcast to the
    ///     dialect-neutral <see cref="DbParameter" /> so a caller can set <see cref="DbParameter.DbType" />
    ///     on it if needed.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    DbParameter AppendParameter(object? value);
}

/// <summary>
///     Dialect-neutral <see cref="IGroupedParameterBuilder" /> that binds through the neutral
///     <see cref="ICommandBuilder.AppendParameter(object)" />. Any provider command builder that only
///     needs to satisfy the neutral contract (rather than expose provider-typed grouped overloads) can
///     hand one of these back from <see cref="ICommandBuilder.CreateGroupedParameterBuilder(char?)" />.
/// </summary>
public sealed class GroupedParameterBuilder: IGroupedParameterBuilder
{
    private readonly ICommandBuilder _commandBuilder;
    private readonly char? _separator;
    private int _count;

    public GroupedParameterBuilder(ICommandBuilder commandBuilder, char? separator)
    {
        _commandBuilder = commandBuilder;
        _separator = separator;
    }

    public DbParameter AppendParameter(object? value)
    {
        if (_count > 0 && _separator.HasValue)
            _commandBuilder.Append(_separator.Value);

        _count++;
        return _commandBuilder.AppendParameter(value ?? DBNull.Value);
    }
}
