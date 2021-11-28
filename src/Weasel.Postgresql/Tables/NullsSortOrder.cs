namespace Weasel.Postgresql.Tables
{
    /// <summary>
    /// Specifies the null sort order
    /// </summary>
    public enum NullsSortOrder
    {
        // specifies that nulls sort before non-nulls
        First,
        // specifies that nulls sort after non-nulls
        Last
    }
}
