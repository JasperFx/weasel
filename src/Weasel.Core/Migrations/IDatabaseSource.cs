using System.Collections.Generic;

namespace Weasel.Core.Migrations
{
    /// <summary>
    /// Service that exposes multiple databases for migration operations. This
    /// was intended for "database per tenant" type scenarios
    /// </summary>
    public interface IDatabaseSource
    {
        /// <summary>
        /// Resolve a list of the known databases
        /// </summary>
        /// <returns></returns>
        IReadOnlyList<IDatabase> BuildDatabases();
    }
}
