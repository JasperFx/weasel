#nullable enable
using System.Data.Common;

namespace Weasel.Storage;

/// <summary>
///     Marker base for the row-materialization selectors the closed-shape document storage
///     runtime builds from a select clause (see <see cref="ISelectClause.BuildSelector"/>).
/// </summary>
public interface ISelector
{
}

/// <summary>
///     Materializes a <typeparamref name="T"/> from the current row of a
///     <see cref="DbDataReader"/>.
/// </summary>
public interface ISelector<T>: ISelector
{
    T Resolve(DbDataReader reader);

    Task<T> ResolveAsync(DbDataReader reader, CancellationToken token);
}
