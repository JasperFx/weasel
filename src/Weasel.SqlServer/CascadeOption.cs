namespace Weasel.SqlServer;

/// <summary>
/// SQL Server-specific cascade action enum (no Restrict support)
/// </summary>
public enum CascadeAction
{
    SetNull = Weasel.Core.Tables.CascadeAction.SetNull,
    SetDefault = Weasel.Core.Tables.CascadeAction.SetDefault,
    NoAction = Weasel.Core.Tables.CascadeAction.NoAction,
    Cascade = Weasel.Core.Tables.CascadeAction.Cascade
}
