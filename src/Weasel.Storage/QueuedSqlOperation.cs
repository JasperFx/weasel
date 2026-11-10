#nullable enable
using System.Data.Common;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
///     Arbitrary SQL enrolled in a session's unit of work, so it commits in the same transaction as the
///     documents and events — what a store's <c>QueueSqlCommand</c> queues.
/// </summary>
/// <remarks>
///     <para>
///         Each placeholder character in the text becomes one positional parameter, bound to the
///         value at the same index; the counts must match. A trailing <c>;</c> is trimmed: the session
///         separates statements, so this belongs in a batch whose builder really splits commands — see
///         <see cref="OperationBatchExecutor.Configure" />.
///     </para>
///     <para>
///         Values are bound by <see cref="BindParameter" />, which assigns the value (or
///         <see cref="DBNull" />). A provider that has to say more about the type — an
///         <c>NpgsqlDbType</c>, or SQLite's storage of a <see cref="Guid" /> or timestamp — overrides it.
///     </para>
///     <para>
///         <see cref="DocumentType" /> is <see cref="object" /> unless set: queued SQL names its own tables,
///         and a session must not try to create a document table for it. <see cref="Role" /> is
///         <see cref="OperationRole.Other" />.
///     </para>
///     <para>
///         ⚠️ Marked <see cref="NoDataReturnedCall" />, so a <see cref="ResultSetWalk.SkipNoDataReturned" />
///         walk does not advance past it. Queued SQL that <em>returns rows</em> would misalign every
///         operation after it under that walk.
///     </para>
/// </remarks>
public class QueuedSqlOperation: IStorageOperation, NoDataReturnedCall
{
    private readonly object?[] _parameterValues;

    /// <param name="commandText">The SQL, with one <paramref name="placeholder" /> per parameter.</param>
    /// <param name="placeholder">The character marking a parameter, usually <c>?</c>.</param>
    /// <param name="parameterValues">One value per placeholder, in order.</param>
    public QueuedSqlOperation(string commandText, char placeholder, params object?[] parameterValues)
    {
        ArgumentNullException.ThrowIfNull(commandText);

        CommandText = commandText.TrimEnd().TrimEnd(';');
        Placeholder = placeholder;
        _parameterValues = parameterValues ?? [];
    }

    /// <summary>The SQL as it will be sent, trailing <c>;</c> trimmed.</summary>
    public string CommandText { get; }

    /// <summary>The character that marks a parameter.</summary>
    public char Placeholder { get; }

    /// <summary>The parameter values, one per placeholder.</summary>
    public IReadOnlyList<object?> ParameterValues => _parameterValues;

    /// <summary>The type reported to the session; <see cref="object" /> by default.</summary>
    public Type DocumentType { get; init; } = typeof(object);

    /// <inheritdoc />
    public OperationRole Role() => OperationRole.Other;

    /// <inheritdoc />
    public void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        var parameters = builder.AppendWithDbParameters(CommandText, Placeholder);

        if (parameters.Length != _parameterValues.Length)
        {
            throw new InvalidOperationException(
                $"Wrong number of parameter values for queued SQL '{CommandText}': the statement has "
                + $"{parameters.Length} '{Placeholder}' placeholders and {_parameterValues.Length} values "
                + "were supplied. If the SQL contains a literal "
                + $"'{Placeholder}', use a different placeholder character.");
        }

        for (var i = 0; i < parameters.Length; i++)
        {
            BindParameter(parameters[i], _parameterValues[i]);
        }
    }

    /// <summary>
    ///     Bind one value. The default assigns it, or <see cref="DBNull.Value" /> for <see langword="null" />.
    /// </summary>
    protected virtual void BindParameter(DbParameter parameter, object? value)
    {
        parameter.Value = value ?? DBNull.Value;
    }

    /// <inheritdoc />
    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
