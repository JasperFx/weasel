using JasperFx.Descriptors;

namespace Weasel.Core.Migrations;

/// <summary>
///     Service that exposes multiple databases for migration operations. This
///     was intended for "database per tenant" type scenarios
/// </summary>
public interface IDatabaseSource : IDatabaseUser
{
    /// <summary>
    ///     Resolve a list of the known databases
    /// </summary>
    /// <returns></returns>
    ValueTask<IReadOnlyList<IDatabase>> BuildDatabases();

}
