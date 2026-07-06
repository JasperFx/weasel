#nullable enable
using Weasel.Core;
using Weasel.Core.SqlGeneration;

namespace Weasel.Storage;

/// <summary>
///     A SQL fragment that also identifies the operation category it performs (delete, update,
///     etc.) — used by the closed-shape document storage to describe its delete fragments.
/// </summary>
public interface IOperationFragment: ISqlFragment
{
    OperationRole Role();
}
