#nullable enable
namespace Weasel.Storage;

/// <summary>
///     Optional per-store seam for translating provider exceptions raised by the closed-shape
///     write operations into store-specific exceptions (e.g. a unique-constraint violation on
///     the document table into a document-already-exists exception). Supplied to the
///     <see cref="DocumentStorageDescriptor{TDoc,TId}"/> by the owning store's descriptor
///     builder; the operations pass their table name, document type, and identity so the
///     transform can scope its match.
/// </summary>
public interface IOperationExceptionTransform
{
    bool TryTransform(Exception original, string tableName, Type documentType, object id, out Exception? transformed);
}
