using System.Data.Common;
using JasperFx.Core.Exceptions;

namespace Weasel.Core.Operations;

public class OperationPage
{
    protected readonly ICommandBuilder _builder;
    private readonly List<IStorageOperation> _operations = new();
    private IStorageSession _session;

    public OperationPage(IStorageSession session, ICommandBuilder builder)
    {
        _session = session;
        _builder = builder;
    }

    public OperationPage(IStorageSession session, ICommandBuilder builder, IReadOnlyList<IStorageOperation> operations) : this(session, builder)
    {
        _operations.AddRange(operations);
        foreach (var operation in operations)
        {
            _builder.StartNewCommand();
            operation.ConfigureCommand(_builder, _session);
        }

        Count = _operations.Count;
    }

    public ICommandBuilder Builder => _builder;

    public int Count { get; private set; }
    public IReadOnlyList<IStorageOperation> Operations => _operations;

    public void ReleaseSession()
    {
        _session = default;
    }

    public void Append(IStorageOperation operation)
    {
        if (_session == null) return;

        Count++;
        _builder.StartNewCommand();
        operation.ConfigureCommand(
            _builder,
            _session ?? throw new InvalidOperationException("Session already released!")
        );

        _operations.Add(operation);
    }

    public async Task ApplyCallbacksAsync(DbDataReader reader,
        IList<Exception> exceptions,
        CancellationToken token)
    {
        var first = _operations.First();

        if (!(first is NoDataReturnedCall))
        {
            await first.PostprocessAsync(reader, exceptions, token).ConfigureAwait(false);
            try
            {
                await reader.NextResultAsync(token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                if (first is IExceptionTransform t && t.TryTransform(e, out var transformed))
                {
                    throw transformed;
                }

                throw;
            }
        }
        else if (first is AssertsOnCallback)
        {
            await first.PostprocessAsync(reader, exceptions, token).ConfigureAwait(false);
        }

        foreach (var operation in _operations.Skip(1))
        {
            if (operation is NoDataReturnedCall)
            {
                continue;
            }

            await operation.PostprocessAsync(reader, exceptions, token).ConfigureAwait(false);
            try
            {
                await reader.NextResultAsync(token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                if (operation is IExceptionTransform t && t.TryTransform(e, out var transformed))
                {
                    throw transformed;
                }

                throw;
            }
        }
    }
}

