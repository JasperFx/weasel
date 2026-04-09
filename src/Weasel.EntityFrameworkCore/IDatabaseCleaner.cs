using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore;

/// <summary>
///     Service for resetting an EF Core database to a clean state during testing.
///     Registered as a singleton via <c>services.AddDatabaseCleaner&lt;TContext&gt;()</c>.
///     The table dependency graph and generated SQL are memoized on first use.
/// </summary>
public interface IDatabaseCleaner<TContext> where TContext : DbContext
{
    /// <summary>
    ///     Deletes all data from tables managed by the DbContext in foreign-key-safe order.
    /// </summary>
    Task DeleteAllDataAsync(CancellationToken ct = default);

    /// <summary>
    ///     Deletes all data and then runs all registered <see cref="IInitialData{TContext}" /> seeders.
    /// </summary>
    Task ResetAllDataAsync(CancellationToken ct = default);

    /// <summary>
    ///     Deletes all data from tables managed by the DbContext using the supplied connection.
    ///     Use this overload for multi-tenant scenarios where you have a per-tenant connection.
    /// </summary>
    Task DeleteAllDataAsync(DbConnection connection, CancellationToken ct = default);

    /// <summary>
    ///     Deletes all data using the supplied connection, then runs all registered
    ///     <see cref="IInitialData{TContext}" /> seeders against a DbContext built from that connection.
    ///     Use this overload for multi-tenant scenarios.
    /// </summary>
    Task ResetAllDataAsync(DbConnection connection, CancellationToken ct = default);
}
