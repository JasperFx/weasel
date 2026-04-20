using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore;

/// <summary>
///     Adapter that turns a delegate into an <see cref="IInitialData{TContext}" />.
///     Useful for seeding small amounts of data inline from a composition-root
///     registration without authoring a dedicated class — see
///     <see cref="DatabaseCleanerExtensions.AddInitialData{TContext}(Microsoft.Extensions.DependencyInjection.IServiceCollection, System.Func{TContext, System.Threading.CancellationToken, System.Threading.Tasks.Task})" />.
/// </summary>
/// <typeparam name="TContext">The <see cref="DbContext" /> the seeder writes to.</typeparam>
public sealed class LambdaInitialData<TContext> : IInitialData<TContext> where TContext : DbContext
{
    private readonly Func<TContext, CancellationToken, Task> _apply;

    public LambdaInitialData(Func<TContext, CancellationToken, Task> apply)
    {
        _apply = apply ?? throw new ArgumentNullException(nameof(apply));
    }

    public Task Populate(TContext context, CancellationToken cancellation) => _apply(context, cancellation);
}
