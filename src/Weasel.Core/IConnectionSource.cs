using System.Data.Common;

namespace Weasel.Core;

/// <summary>
///     Interface for services that can create a new connection object
///     to the underlying database
/// </summary>
public interface IConnectionSource: IConnectionSource<DbConnection>
{
}

/// <summary>
///     Interface for services that can create a new connection object
///     to the underlying database
/// </summary>
/// <typeparam name="T"></typeparam>
public interface IConnectionSource<T> where T : DbConnection
{
    /// <summary>
    ///     Fetch a connection to the tenant database
    /// </summary>
    /// <returns></returns>
    public T CreateConnection();
}
