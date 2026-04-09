using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Weasel.EntityFrameworkCore;

public static class DatabaseCleanerExtensions
{
    /// <summary>
    ///     Registers <see cref="IDatabaseCleaner{TContext}" /> as a singleton service.
    ///     The cleaner memoizes the table dependency graph and generated SQL on first use.
    /// </summary>
    public static IServiceCollection AddDatabaseCleaner<TContext>(this IServiceCollection services)
        where TContext : DbContext
    {
        services.AddSingleton<IDatabaseCleaner<TContext>, DatabaseCleaner<TContext>>();
        return services;
    }

    /// <summary>
    ///     Registers an <see cref="IInitialData{TContext}" /> implementation that seeds data
    ///     after <see cref="IDatabaseCleaner{TContext}.ResetAllDataAsync" />.
    ///     Multiple seeders execute in registration order.
    /// </summary>
    public static IServiceCollection AddInitialData<TContext, TData>(this IServiceCollection services)
        where TContext : DbContext
        where TData : class, IInitialData<TContext>
    {
        services.AddTransient<IInitialData<TContext>, TData>();
        return services;
    }
}
