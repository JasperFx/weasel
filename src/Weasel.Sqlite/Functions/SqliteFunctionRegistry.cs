namespace Weasel.Sqlite.Functions;

/// <summary>
/// Registry for SQLite functions and collations that should be registered on connections.
/// Unlike schema objects, SQLite functions and collations exist only for the connection lifetime
/// and must be re-registered each time a connection is opened.
/// </summary>
public class SqliteFunctionRegistry
{
    private readonly List<SqliteFunction> _functions = new();
    private readonly List<SqliteCollation> _collations = new();

    /// <summary>
    /// All registered functions.
    /// </summary>
    public IReadOnlyList<SqliteFunction> Functions => _functions.AsReadOnly();

    /// <summary>
    /// All registered collations.
    /// </summary>
    public IReadOnlyList<SqliteCollation> Collations => _collations.AsReadOnly();

    /// <summary>
    /// Add a scalar function with no parameters.
    /// </summary>
    public ScalarFunction<TResult> AddScalar<TResult>(string name, Func<TResult> func, bool isDeterministic = true)
    {
        var function = new ScalarFunction<TResult>(name, func) { IsDeterministic = isDeterministic };
        _functions.Add(function);
        return function;
    }

    /// <summary>
    /// Add a scalar function with 1 parameter.
    /// </summary>
    public ScalarFunction<TResult> AddScalar<TResult>(string name, Func<object?, TResult> func, bool isDeterministic = true)
    {
        var function = new ScalarFunction<TResult>(name, func) { IsDeterministic = isDeterministic };
        _functions.Add(function);
        return function;
    }

    /// <summary>
    /// Add a scalar function with 2 parameters.
    /// </summary>
    public ScalarFunction<TResult> AddScalar<TResult>(string name, Func<object?, object?, TResult> func, bool isDeterministic = true)
    {
        var function = new ScalarFunction<TResult>(name, func) { IsDeterministic = isDeterministic };
        _functions.Add(function);
        return function;
    }

    /// <summary>
    /// Add a scalar function with 3 parameters.
    /// </summary>
    public ScalarFunction<TResult> AddScalar<TResult>(string name, Func<object?, object?, object?, TResult> func, bool isDeterministic = true)
    {
        var function = new ScalarFunction<TResult>(name, func) { IsDeterministic = isDeterministic };
        _functions.Add(function);
        return function;
    }

    /// <summary>
    /// Add a scalar function with 4 parameters.
    /// </summary>
    public ScalarFunction<TResult> AddScalar<TResult>(string name, Func<object?, object?, object?, object?, TResult> func, bool isDeterministic = true)
    {
        var function = new ScalarFunction<TResult>(name, func) { IsDeterministic = isDeterministic };
        _functions.Add(function);
        return function;
    }

    /// <summary>
    /// Add an aggregate function.
    /// </summary>
    public AggregateFunction<TAccumulate, TResult> AddAggregate<TAccumulate, TResult>(
        string name,
        TAccumulate seed,
        Func<TAccumulate, object?, TAccumulate> func,
        Func<TAccumulate, TResult>? resultSelector = null,
        bool isDeterministic = true)
    {
        var function = new AggregateFunction<TAccumulate, TResult>(name, seed, func, resultSelector)
        {
            IsDeterministic = isDeterministic
        };
        _functions.Add(function);
        return function;
    }

    /// <summary>
    /// Add a pre-created function to the registry.
    /// </summary>
    public void Add(SqliteFunction function)
    {
        if (function == null)
        {
            throw new ArgumentNullException(nameof(function));
        }

        _functions.Add(function);
    }

    /// <summary>
    /// Add a collation, used in SQL as <c>COLLATE name</c>. The comparison returns a negative
    /// number, zero or a positive number, as <see cref="string.Compare(string, string)" /> does.
    /// </summary>
    public SqliteCollation AddCollation(string name, Comparison<string> comparison)
    {
        var collation = new SqliteCollation(name, comparison);
        _collations.Add(collation);
        return collation;
    }

    /// <summary>
    /// Register all functions and collations on the given connection.
    /// </summary>
    public void RegisterAll(Microsoft.Data.Sqlite.SqliteConnection connection)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        if (connection.State != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException("Connection must be open before registering functions");
        }

        foreach (var function in _functions)
        {
            function.Register(connection);
        }

        foreach (var collation in _collations)
        {
            collation.Register(connection);
        }
    }

    /// <summary>
    /// Register all functions and collations asynchronously.
    /// </summary>
    public Task RegisterAllAsync(Microsoft.Data.Sqlite.SqliteConnection connection, CancellationToken ct = default)
    {
        // Function registration is synchronous in Microsoft.Data.Sqlite
        // Wrap in Task.Run to make it awaitable and respect cancellation
        return Task.Run(() =>
        {
            ct.ThrowIfCancellationRequested();
            RegisterAll(connection);
        }, ct);
    }

    /// <summary>
    /// Remove all functions and collations from the registry.
    /// </summary>
    public void Clear()
    {
        _functions.Clear();
        _collations.Clear();
    }
}
