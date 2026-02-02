using JasperFx.Core;

namespace Weasel.Core.Tables;

/// <summary>
/// Abstract base class for column check constraints
/// </summary>
public abstract class ColumnCheckBase
{
    /// <summary>
    /// The database name for the check constraint. This can be null for unnamed constraints.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Returns the SQL declaration for this check constraint
    /// </summary>
    public abstract string Declaration();

    /// <summary>
    /// Returns the full SQL declaration including the CONSTRAINT clause if named
    /// </summary>
    public string FullDeclaration()
    {
        if (Name.IsEmpty())
        {
            return Declaration();
        }

        return $"CONSTRAINT {Name} {Declaration()}";
    }
}
