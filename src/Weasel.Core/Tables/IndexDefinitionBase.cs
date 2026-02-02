using JasperFx.Core;

namespace Weasel.Core.Tables;

/// <summary>
/// Specifies the sort order for index columns
/// </summary>
public enum SortOrder
{
    Asc,
    Desc
}

/// <summary>
/// Abstract base class for index definitions that defines common properties and behavior
/// shared between PostgreSQL and SQL Server implementations.
/// </summary>
public abstract class IndexDefinitionBase : INamed
{
    protected string? _indexName;

    protected IndexDefinitionBase(string indexName)
    {
        _indexName = indexName;
    }

    protected IndexDefinitionBase()
    {
    }

    /// <summary>
    /// Sort order for the index (ASC or DESC)
    /// </summary>
    public SortOrder SortOrder { get; set; } = SortOrder.Asc;

    /// <summary>
    /// Indicates whether this is a unique index
    /// </summary>
    public virtual bool IsUnique { get; set; }

    /// <summary>
    /// The columns that make up this index
    /// </summary>
    public abstract string[]? Columns { get; set; }

    /// <summary>
    /// The constraint expression for a partial index (WHERE clause)
    /// </summary>
    public string? Predicate { get; set; }

    /// <summary>
    /// Set a non-default fill factor on this index
    /// </summary>
    public abstract int? FillFactor { get; set; }

    /// <summary>
    /// The name of this index
    /// </summary>
    public string Name
    {
        get
        {
            if (_indexName.IsNotEmpty())
            {
                return _indexName;
            }

            return DeriveIndexName();
        }
        set => _indexName = value;
    }

    /// <summary>
    /// Derives a default name for the index when not explicitly specified
    /// </summary>
    protected virtual string DeriveIndexName()
    {
        throw new NotSupportedException();
    }

    /// <summary>
    /// Sets the index expression against the supplied columns
    /// </summary>
    public virtual IndexDefinitionBase AgainstColumns(params string[] columns)
    {
        Columns = columns;
        return this;
    }

    /// <summary>
    /// Generates the DDL for creating this index
    /// </summary>
    public abstract string ToDDL(DbObjectName tableIdentifier);

    /// <summary>
    /// Checks if this index definition matches another index definition
    /// </summary>
    public abstract bool Matches(IndexDefinitionBase actual, DbObjectName tableIdentifier);

    /// <summary>
    /// Asserts that this index definition matches another, throwing an exception if not
    /// </summary>
    public virtual void AssertMatches(IndexDefinitionBase actual, DbObjectName tableIdentifier)
    {
        if (!Matches(actual, tableIdentifier))
        {
            throw new Exception(
                $"Index did not match, expected{Environment.NewLine}{ToDDL(tableIdentifier)}{Environment.NewLine}but got:{Environment.NewLine}{actual.ToDDL(tableIdentifier)}");
        }
    }
}
