namespace Weasel.Postgresql.Tables
{
    /// <summary>
    /// Specifies the null sort order
    /// </summary>
    public enum NullsSortOrder
    {
        // nulls sort is not set
        None,
        // nulls sort before non-nulls
        First,
        // nulls sort after non-nulls
        Last
    }
}
