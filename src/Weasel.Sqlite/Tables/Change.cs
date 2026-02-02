namespace Weasel.Sqlite.Tables;

public class Change<T>
{
    public Change(T expected, T actual)
    {
        Expected = expected;
        Actual = actual;
    }

    public T Expected { get; }
    public T Actual { get; }
}
