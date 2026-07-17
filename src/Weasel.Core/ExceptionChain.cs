namespace Weasel.Core;

/// <summary>
///     Helpers for looking through exception wrapping.
/// </summary>
public static class ExceptionChain
{
    /// <summary>
    ///     Enumerates the supplied exception and everything nested inside it -- inner exceptions, and every
    ///     branch of any <see cref="AggregateException" />.
    ///     <para>
    ///     Exists for <see cref="Migrator.IsTransientConnectionFailure" /> implementations (weasel#356). The
    ///     database drivers rarely surface a connection failure bare: the provider-specific exception
    ///     carrying the error code is typically wrapped, and multi-host/failover data sources aggregate one
    ///     failure per host. A predicate that only inspected the outermost exception would be inert for
    ///     exactly the cases it exists to catch.
    ///     </para>
    /// </summary>
    public static IEnumerable<Exception> Flatten(Exception exception)
    {
        if (exception is AggregateException aggregate)
        {
            foreach (var inner in aggregate.Flatten().InnerExceptions)
            foreach (var e in Flatten(inner))
            {
                yield return e;
            }

            yield break;
        }

        for (var e = exception; e != null; e = e.InnerException)
        {
            yield return e;

            if (e.InnerException is AggregateException nested)
            {
                foreach (var inner in Flatten(nested)) yield return inner;
                yield break;
            }
        }
    }
}
