namespace Weasel.Core;

/// <summary>
/// Specifies the action to take when a referenced row is deleted or updated
/// </summary>
public enum CascadeAction
{
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault
}
