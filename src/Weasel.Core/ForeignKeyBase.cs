namespace Weasel.Core;

/// <summary>
/// Base class for foreign key definitions across all database providers
/// </summary>
public abstract class ForeignKeyBase : INamed
{
    protected ForeignKeyBase(string name)
    {
        Name = name;
    }

    /// <summary>
    /// The name of the foreign key constraint
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// The column names in the child/dependent table that make up the foreign key
    /// </summary>
    public abstract string[] ColumnNames { get; set; }

    /// <summary>
    /// The column names in the parent/principal table that are referenced
    /// </summary>
    public abstract string[] LinkedNames { get; set; }

    /// <summary>
    /// The referenced/principal table
    /// </summary>
    public DbObjectName? LinkedTable { get; set; }

    /// <summary>
    /// The cascade action to take when a referenced row is deleted
    /// </summary>
    public CascadeAction DeleteAction { get; set; } = CascadeAction.NoAction;

    /// <summary>
    /// The cascade action to take when a referenced row is updated
    /// </summary>
    public CascadeAction UpdateAction { get; set; } = CascadeAction.NoAction;
}
