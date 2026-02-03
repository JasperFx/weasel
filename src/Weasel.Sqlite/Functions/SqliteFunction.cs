namespace Weasel.Sqlite.Functions;

/// <summary>
/// Base class for SQLite function abstractions.
/// Unlike PostgreSQL/SQL Server, SQLite functions are not stored in the database schema.
/// They must be registered on each connection when opened.
/// </summary>
public abstract class SqliteFunction
{
    /// <summary>
    /// The name of the function as it will be called in SQL.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Whether the function is deterministic (same inputs always produce same output).
    /// Deterministic functions can be optimized better by SQLite.
    /// </summary>
    public bool IsDeterministic { get; set; } = true;

    protected SqliteFunction(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Function name cannot be null or empty", nameof(name));
        }

        Name = name;
    }

    /// <summary>
    /// Register this function on the given connection.
    /// </summary>
    public abstract void Register(Microsoft.Data.Sqlite.SqliteConnection connection);
}

/// <summary>
/// Represents a scalar function (takes inputs, returns single value).
/// Example: UPPER(text) -> TEXT, ABS(number) -> NUMBER
/// </summary>
public class ScalarFunction<TResult> : SqliteFunction
{
    private readonly Func<TResult>? _func0;
    private readonly Func<object?, TResult>? _func1;
    private readonly Func<object?, object?, TResult>? _func2;
    private readonly Func<object?, object?, object?, TResult>? _func3;
    private readonly Func<object?, object?, object?, object?, TResult>? _func4;
    private readonly int _parameterCount;

    /// <summary>
    /// Create a scalar function with no parameters.
    /// </summary>
    public ScalarFunction(string name, Func<TResult> func) : base(name)
    {
        _func0 = func ?? throw new ArgumentNullException(nameof(func));
        _parameterCount = 0;
    }

    /// <summary>
    /// Create a scalar function with 1 parameter.
    /// </summary>
    public ScalarFunction(string name, Func<object?, TResult> func) : base(name)
    {
        _func1 = func ?? throw new ArgumentNullException(nameof(func));
        _parameterCount = 1;
    }

    /// <summary>
    /// Create a scalar function with 2 parameters.
    /// </summary>
    public ScalarFunction(string name, Func<object?, object?, TResult> func) : base(name)
    {
        _func2 = func ?? throw new ArgumentNullException(nameof(func));
        _parameterCount = 2;
    }

    /// <summary>
    /// Create a scalar function with 3 parameters.
    /// </summary>
    public ScalarFunction(string name, Func<object?, object?, object?, TResult> func) : base(name)
    {
        _func3 = func ?? throw new ArgumentNullException(nameof(func));
        _parameterCount = 3;
    }

    /// <summary>
    /// Create a scalar function with 4 parameters.
    /// </summary>
    public ScalarFunction(string name, Func<object?, object?, object?, object?, TResult> func) : base(name)
    {
        _func4 = func ?? throw new ArgumentNullException(nameof(func));
        _parameterCount = 4;
    }

    public override void Register(Microsoft.Data.Sqlite.SqliteConnection connection)
    {
        switch (_parameterCount)
        {
            case 0:
                connection.CreateFunction(Name, _func0, IsDeterministic);
                break;
            case 1:
                connection.CreateFunction(Name, _func1!, IsDeterministic);
                break;
            case 2:
                connection.CreateFunction(Name, _func2!, IsDeterministic);
                break;
            case 3:
                connection.CreateFunction(Name, _func3!, IsDeterministic);
                break;
            case 4:
                connection.CreateFunction(Name, _func4!, IsDeterministic);
                break;
            default:
                throw new InvalidOperationException($"Unsupported parameter count: {_parameterCount}");
        }
    }
}

/// <summary>
/// Represents an aggregate function (processes multiple rows, returns single value).
/// Example: SUM(column) -> NUMBER, AVG(column) -> NUMBER, STRING_AGG(column) -> TEXT
/// </summary>
public class AggregateFunction<TAccumulate, TResult> : SqliteFunction
{
    private readonly TAccumulate _seed;
    private readonly Func<TAccumulate, object?, TAccumulate> _func;
    private readonly Func<TAccumulate, TResult>? _resultSelector;

    /// <summary>
    /// Create an aggregate function.
    /// </summary>
    /// <param name="name">Function name</param>
    /// <param name="seed">Initial accumulator value</param>
    /// <param name="func">Function to apply for each row (accumulator, currentValue) => newAccumulator</param>
    /// <param name="resultSelector">Function to convert final accumulator to result (optional)</param>
    public AggregateFunction(
        string name,
        TAccumulate seed,
        Func<TAccumulate, object?, TAccumulate> func,
        Func<TAccumulate, TResult>? resultSelector = null) : base(name)
    {
        _seed = seed;
        _func = func ?? throw new ArgumentNullException(nameof(func));
        _resultSelector = resultSelector;
    }

    public override void Register(Microsoft.Data.Sqlite.SqliteConnection connection)
    {
        if (_resultSelector != null)
        {
            connection.CreateAggregate<object?, TAccumulate, TResult>(Name, _seed, _func, _resultSelector, IsDeterministic);
        }
        else
        {
            connection.CreateAggregate<object?, TAccumulate>(Name, _seed, _func, IsDeterministic);
        }
    }
}
