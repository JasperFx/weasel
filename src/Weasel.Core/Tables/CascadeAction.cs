namespace Weasel.Core.Tables;

/// <summary>
/// Defines the referential action for foreign key constraints
/// </summary>
public enum CascadeAction
{
    SetNull,
    SetDefault,
    /// <summary>
    /// PostgreSQL specific - equivalent to NoAction in SQL Server
    /// </summary>
    Restrict,
    NoAction,
    Cascade
}
