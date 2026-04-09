using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore;

/// <summary>
///     Implement to define seed data that is applied after a database reset.
///     Register implementations in your DI container with
///     <c>services.AddInitialData&lt;TContext, TData&gt;()</c>.
///     Registered implementations execute in registration order.
/// </summary>
public interface IInitialData<in TContext> where TContext : DbContext
{
    Task Populate(TContext context, CancellationToken cancellation);
}
