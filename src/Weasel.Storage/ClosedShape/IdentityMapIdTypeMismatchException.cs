#nullable enable
namespace Weasel.Storage;

/// <summary>
///     A session's identity map holds documents of <see cref="DocumentType" /> under a different id
///     type than the storage reading or writing it. Two storages disagree about the document's id
///     type, which is a mapping error rather than a missing document, so it is refused rather than
///     treated as a miss.
/// </summary>
/// <remarks>
///     Not named <c>DocumentIdTypeMismatchException</c> on purpose: Marten has a public type by that
///     name, and a Marten file importing both namespaces would no longer compile (CS0104).
/// </remarks>
public class IdentityMapIdTypeMismatchException: Exception
{
    public IdentityMapIdTypeMismatchException(Type documentType, Type idType)
        : base($"The identity map for document type {documentType.FullName} does not hold ids of type "
               + $"{idType.FullName}. Two storages disagree about the document's id type.")
    {
        DocumentType = documentType;
        IdType = idType;
    }

    public Type DocumentType { get; }

    public Type IdType { get; }
}
