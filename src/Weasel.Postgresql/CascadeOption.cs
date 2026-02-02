namespace Weasel.Postgresql;

/// <summary>
/// PostgreSQL-specific cascade action enum (includes Restrict)
/// </summary>
public enum CascadeAction
{
    SetNull = Weasel.Core.Tables.CascadeAction.SetNull,
    SetDefault = Weasel.Core.Tables.CascadeAction.SetDefault,
    Restrict = Weasel.Core.Tables.CascadeAction.Restrict,
    NoAction = Weasel.Core.Tables.CascadeAction.NoAction,
    Cascade = Weasel.Core.Tables.CascadeAction.Cascade
}
