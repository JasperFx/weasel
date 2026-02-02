namespace Weasel.Oracle;

/// <summary>
/// Specifies the action to take when a referenced row is deleted or updated
/// </summary>
[Obsolete("Use Weasel.Core.CascadeAction instead. This type will be removed in a future version.")]
public enum CascadeAction
{
    SetNull,
    SetDefault,
    NoAction,
    Cascade
}
