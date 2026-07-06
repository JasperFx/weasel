#nullable enable
using System.Reflection;

namespace Weasel.Storage;

/// <summary>
///     Database-neutral view of a duplicated field that the closed-shape storage runtime
///     consumes off <see cref="IDocumentStorage"/> — exposes only the members the storage /
///     patch code needs (the resolved member chain, the column name, and the pre-rendered
///     update fragment). A store's LINQ member model keeps its richer, dialect-typed duplicated
///     field type and implements this view.
/// </summary>
public interface IDuplicatedField
{
    /// <summary>The resolved member chain the duplicated column is derived from.</summary>
    MemberInfo[] Members { get; }

    /// <summary>The duplicated column's name.</summary>
    string ColumnName { get; }

    /// <summary>The <c>"col" = ...</c> assignment fragment used when patch/update SQL refreshes the column.</summary>
    string UpdateSqlFragment();
}
