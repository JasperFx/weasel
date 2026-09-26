using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     Gets the command of a query EF Core has already prepared, so the batch runs it with the values EF Core
///     read when preparing it. The public <c>CreateDbCommand()</c> would prepare the query again from its LINQ
///     expression and read those values a second time.
///     <para>
///     This is the one place <see cref="BatchedQuery" /> uses an EF Core internal API,
///     <c>IRelationalQueryingEnumerable</c>, unchanged since EF Core 5.0. If EF Core stops providing it, this
///     returns null and <see cref="BatchedQuery" /> runs each query on its own round trip instead of failing.
///     </para>
/// </summary>
internal static class QueryCommand
{
    public static DbCommand? TryCreate(object preparedQuery)
    {
        try
        {
            return create(preparedQuery);
        }
        // The interface or its method no longer exists in this version of EF Core
        catch (TypeLoadException)
        {
            return null;
        }
        catch (MissingMethodException)
        {
            return null;
        }
    }

    // In its own method, so a missing interface fails when this method is compiled, inside TryCreate's try
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static DbCommand? create(object preparedQuery)
    {
#pragma warning disable EF1001 // Internal EF Core API usage, see above
        return (preparedQuery as IRelationalQueryingEnumerable)?.CreateDbCommand();
#pragma warning restore EF1001
    }
}
