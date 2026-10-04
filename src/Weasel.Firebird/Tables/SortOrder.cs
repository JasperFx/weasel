namespace Weasel.Firebird.Tables;

/// <summary>
///     The direction of a whole Firebird index. Firebird has no per-column direction: an index is
///     ascending or descending as a whole.
/// </summary>
public enum SortOrder
{
    Asc,
    Desc
}
