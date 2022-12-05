namespace Weasel.Postgresql;

public enum CascadeAction
{
    SetNull,
    SetDefault,
    Restrict,
    NoAction,
    Cascade
}
