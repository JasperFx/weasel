using System.Data.Common;
using JasperFx.Core.Exceptions;
using NSubstitute;
using Weasel.Core;
using Weasel.Storage;

namespace Weasel.Core.Tests.StorageHelpers;

using IStorageOperation = Weasel.Storage.IStorageOperation;

/// <summary>
///     Hand-rolled operations for the storage-helper tests. Real classes rather than substitutes where
///     the helper type-tests the operation (IDeletion, IDocumentStorageOperation, NoDataReturnedCall,
///     IExceptionTransform), because a substitute for one interface is not an instance of the others.
/// </summary>
internal class FakeOperation: IStorageOperation
{
    private readonly OperationRole _role;

    public FakeOperation(OperationRole role, Type? documentType = null, string sql = "")
    {
        _role = role;
        DocumentType = documentType ?? typeof(object);
        Sql = sql;
    }

    public string Sql { get; }
    public Type DocumentType { get; }
    public OperationRole Role() => _role;

    public List<object?> Read { get; } = [];

    public Func<DbDataReader, IList<Exception>, Task>? OnPostprocess { get; set; }

    public void ConfigureCommand(ICommandBuilder builder, IStorageSession session) => builder.Append(Sql);

    public virtual async Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
        if (OnPostprocess is not null)
        {
            await OnPostprocess(reader, exceptions);
            return;
        }

        while (await reader.ReadAsync(token))
        {
            Read.Add(reader.GetValue(0));
        }
    }
}

internal sealed class FakeNoDataOperation(string sql): FakeOperation(OperationRole.Other, null, sql), NoDataReturnedCall
{
    public bool Postprocessed { get; private set; }

    public override Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
        Postprocessed = true;
        return Task.CompletedTask;
    }
}

internal sealed class FakeTransformingOperation(string sql, Exception replacement)
    : FakeOperation(OperationRole.Insert, null, sql), IExceptionTransform
{
    public bool TryTransform(Exception original, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Exception? transformed)
    {
        transformed = replacement;
        return true;
    }
}

internal sealed class FakeDocumentOperation(object document, OperationRole role, IChangeTracker? tracker = null)
    : FakeOperation(role, document.GetType()), IDocumentStorageOperation
{
    public object Document { get; } = document;

    public IChangeTracker ToTracker(IStorageSession session)
        => tracker ?? FakeTracker.For(Document);
}

internal sealed class FakeDeletion(Type documentType, object id, OperationRole role = OperationRole.Deletion)
    : FakeOperation(role, documentType), IDeletion
{
    public object Document { get; set; } = null!;
    public object Id { get; set; } = id;
}

internal static class FakeTracker
{
    public static IChangeTracker For(object document)
    {
        var tracker = Substitute.For<IChangeTracker>();
        tracker.Document.Returns(document);
        return tracker;
    }
}
