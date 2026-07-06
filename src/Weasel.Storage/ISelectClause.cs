#nullable enable
using Weasel.Core.SqlGeneration;

namespace Weasel.Storage;

/// <summary>
///     Dialect-neutral select-clause contract for the closed-shape document storage runtime:
///     where the document rows come from (<see cref="FromObject"/>), what is selected
///     (<see cref="SelectFields"/> / <see cref="SelectedType"/>), and how rows are materialized
///     (<see cref="BuildSelector"/>). A store's LINQ layer typically derives its own richer
///     select-clause interface from this one, adding query-handler / statistics members that are
///     specific to that store's query pipeline.
/// </summary>
public interface ISelectClause: ISqlFragment
{
    string FromObject { get; }

    Type SelectedType { get; }

    string[] SelectFields();

    ISelector BuildSelector(IStorageSession session);
}
