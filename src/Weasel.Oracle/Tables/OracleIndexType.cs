namespace Weasel.Oracle.Tables;

/// <summary>
/// Oracle index types
/// </summary>
public enum OracleIndexType
{
    /// <summary>
    /// Standard B-tree index (default)
    /// </summary>
    BTree,

    /// <summary>
    /// Bitmap index - efficient for low-cardinality columns
    /// </summary>
    Bitmap,

    /// <summary>
    /// Function-based index - index on an expression
    /// </summary>
    FunctionBased
}
