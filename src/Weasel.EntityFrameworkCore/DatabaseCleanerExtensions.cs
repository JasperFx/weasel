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

    /// <summary>
    ///     Registers an inline <see cref="IInitialData{TContext}" /> seeder that executes
    ///     <paramref name="apply" /> after <see cref="IDatabaseCleaner{TContext}.ResetAllDataAsync" />.
    ///     Useful for small amounts of seed data where authoring a dedicated
    ///     <see cref="IInitialData{TContext}" /> class is unwarranted. Registered as a singleton;
    ///     multiple lambda seeders execute in registration order alongside class-based seeders.
    /// </summary>
    /// <param name="services">The DI service collection.</param>
    /// <param name="apply">
    ///     Delegate invoked with a scoped <typeparamref name="TContext" /> and the caller's
    ///     cancellation token. Implementations should call <c>SaveChangesAsync</c> as appropriate.
    /// </param>
    public static IServiceCollection AddInitialData<TContext>(
        this IServiceCollection services,
        Func<TContext, CancellationToken, Task> apply) where TContext : DbContext
    {
        if (apply is null) throw new ArgumentNullException(nameof(apply));

        services.AddSingleton<IInitialData<TContext>>(new LambdaInitialData<TContext>(apply));
        return services;
    }
}
