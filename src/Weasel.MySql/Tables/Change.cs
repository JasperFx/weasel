namespace Weasel.MySql.Tables;

public class Change<T> where T : class
{
    public Change(T expected, T actual)
    {
        Expected = expected;
        Actual = actual;
    }

    public T Expected { get; }
    public T Actual { get; }
}
