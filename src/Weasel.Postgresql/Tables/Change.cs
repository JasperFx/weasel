namespace Weasel.Postgresql.Tables;

internal class Change<T>
{
    public Change(T expected, T actual)
    {
        Expected = expected;
        Actual = actual;
    }

    public T Expected { get; }
    public T Actual { get; }
}
