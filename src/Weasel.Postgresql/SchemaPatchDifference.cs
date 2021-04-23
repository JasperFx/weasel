namespace Weasel.Postgresql
{
    public enum SchemaPatchDifference
    {
        None = 3,
        Create = 2,
        Update = 1,
        Invalid = 0
    }
}
