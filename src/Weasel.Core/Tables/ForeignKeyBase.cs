using JasperFx.Core;
using JasperFx.Core.Reflection;

namespace Weasel.Core.Tables;

/// <summary>
/// Exception thrown when a foreign key is misconfigured
/// </summary>
public class MisconfiguredForeignKeyException : Exception
{
    public MisconfiguredForeignKeyException(string? message) : base(message)
    {
    }
}

/// <summary>
/// Abstract base class for foreign key constraints that defines common properties and behavior
/// shared between PostgreSQL and SQL Server implementations.
/// </summary>
public abstract class ForeignKeyBase : INamed
{
    protected ForeignKeyBase(string name)
    {
        Name = name;
    }

    /// <summary>
    /// The column names in the source table that participate in this foreign key
    /// </summary>
    public virtual string[] ColumnNames { get; set; } = null!;

    /// <summary>
    /// The column names in the referenced table that are linked to
    /// </summary>
    public virtual string[] LinkedNames { get; set; } = null!;

    /// <summary>
    /// The identifier of the referenced table
    /// </summary>
    public DbObjectName LinkedTable { get; set; } = null!;

    /// <summary>
    /// The referential action to take on DELETE
    /// </summary>
    public CascadeAction OnDelete { get; set; } = CascadeAction.NoAction;

    /// <summary>
    /// The referential action to take on UPDATE
    /// </summary>
    public CascadeAction OnUpdate { get; set; } = CascadeAction.NoAction;

    /// <summary>
    /// The name of this foreign key constraint
    /// </summary>
    public string Name { get; set; }

    protected bool Equals(ForeignKeyBase other)
    {
        return Name == other.Name && ColumnNames.SequenceEqual(other.ColumnNames) &&
               LinkedNames.SequenceEqual(other.LinkedNames) && Equals(LinkedTable, other.LinkedTable) &&
               OnDelete == other.OnDelete && OnUpdate == other.OnUpdate;
    }

    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj))
        {
            return false;
        }

        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (!obj.GetType().CanBeCastTo(GetType()))
        {
            return false;
        }

        return Equals((ForeignKeyBase)obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = Name != null ? Name.GetHashCode() : 0;
            hashCode = (hashCode * 397) ^ (ColumnNames != null ? ColumnNames.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (LinkedNames != null ? LinkedNames.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (LinkedTable != null ? LinkedTable.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (int)OnDelete;
            hashCode = (hashCode * 397) ^ (int)OnUpdate;
            return hashCode;
        }
    }

    /// <summary>
    /// Generates the DDL to add this foreign key constraint to a table
    /// </summary>
    public abstract string ToDDL(DbObjectName parentIdentifier);

    /// <summary>
    /// Writes the ADD CONSTRAINT statement for this foreign key
    /// </summary>
    public abstract void WriteAddStatement(DbObjectName parentIdentifier, TextWriter writer);

    /// <summary>
    /// Writes the DROP CONSTRAINT statement for this foreign key
    /// </summary>
    public virtual void WriteDropStatement(DbObjectName parentIdentifier, TextWriter writer)
    {
        writer.WriteLine($"ALTER TABLE {parentIdentifier} DROP CONSTRAINT IF EXISTS {Name};");
    }
}
