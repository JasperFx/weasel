using System.Text;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Sqlite.Tables;

/// <summary>
/// Represents an index definition for SQLite tables.
/// SQLite only supports B-tree indexes, but supports partial and expression indexes.
/// </summary>
public class IndexDefinition: INamed
{
    private string? _indexName;

    public IndexDefinition(string indexName)
    {
        _indexName = indexName;
    }

    protected IndexDefinition()
    {
    }

    /// <summary>
    /// Sort order for index columns (ASC or DESC)
    /// </summary>
    public SortOrder SortOrder { get; set; } = SortOrder.Asc;

    /// <summary>
    /// Create a UNIQUE index
    /// </summary>
    public bool IsUnique { get; set; }

    /// <summary>
    /// Columns to index. Can be column names or expressions.
    /// </summary>
    public virtual string[]? Columns { get; set; }

    /// <summary>
    /// WHERE clause for partial indexes
    /// </summary>
    public string? Predicate { get; set; }

    /// <summary>
    /// The collation sequence to use for text columns
    /// </summary>
    public string? Collation { get; set; }

    /// <summary>
    /// For expression-based indexes, the full expression to index
    /// Use this for complex expressions including JSON path extraction
    /// </summary>
    public string? Expression { get; set; }

    public string Name
    {
        get
        {
            if (_indexName.IsNotEmpty())
            {
                return _indexName;
            }

            return deriveIndexName();
        }
        set => _indexName = value;
    }

    public string QuotedName => SchemaUtils.QuoteName(Name);

    protected virtual string deriveIndexName()
    {
        throw new NotSupportedException();
    }

    /// <summary>
    /// Set the Index expression against the supplied columns
    /// </summary>
    public IndexDefinition AgainstColumns(params string[] columns)
    {
        Columns = columns;
        return this;
    }

    /// <summary>
    /// Create an expression index for JSON path extraction.
    /// Example: json_extract(data, '$.name')
    /// </summary>
    /// <param name="columnName">The JSON column name</param>
    /// <param name="jsonPath">The JSON path (e.g., '$.name')</param>
    public IndexDefinition ForJsonPath(string columnName, string jsonPath)
    {
        Expression = $"json_extract({SchemaUtils.QuoteName(columnName)}, '{jsonPath}')";
        return this;
    }

    /// <summary>
    /// Create an expression index with custom expression
    /// </summary>
    public IndexDefinition WithExpression(string expression)
    {
        Expression = expression;
        return this;
    }

    /// <summary>
    /// Generate DDL statement for creating the index
    /// SQLite syntax: CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (columns) [WHERE predicate]
    /// </summary>
    public string ToDDL(Table parent)
    {
        var builder = new StringBuilder();

        builder.Append("CREATE ");

        if (IsUnique)
        {
            builder.Append("UNIQUE ");
        }

        builder.Append("INDEX IF NOT EXISTS ");
        builder.Append(QuotedName);
        builder.Append(" ON ");
        builder.Append(parent.Identifier);
        builder.Append(" ");

        // Build the index columns/expression
        builder.Append(BuildIndexExpression());

        // Add WHERE clause for partial index
        if (Predicate.IsNotEmpty())
        {
            builder.Append(" WHERE ");
            builder.Append(Predicate);
        }

        builder.Append(";");

        return builder.ToString();
    }

    private string BuildIndexExpression()
    {
        // If we have an explicit expression, use that
        if (Expression.IsNotEmpty())
        {
            var sortOrderStr = SortOrder == SortOrder.Desc ? " DESC" : "";
            var collationStr = Collation.IsNotEmpty() ? $" COLLATE {Collation}" : "";
            return $"({Expression}{collationStr}{sortOrderStr})";
        }

        // Otherwise build from columns
        if (Columns == null || !Columns.Any())
        {
            throw new InvalidOperationException($"Index '{Name}' must have either Columns or Expression defined");
        }

        var columnParts = new List<string>();
        foreach (var col in Columns)
        {
            var parts = new List<string>();
            parts.Add(SchemaUtils.QuoteName(col));

            if (Collation.IsNotEmpty())
            {
                parts.Add($"COLLATE {Collation}");
            }

            if (SortOrder == SortOrder.Desc)
            {
                parts.Add("DESC");
            }

            columnParts.Add(parts.Join(" "));
        }

        return $"({columnParts.Join(", ")})";
    }

    public void WriteDropStatement(Table parent, TextWriter writer)
    {
        writer.WriteDropIndex(parent, this);
    }

    protected bool Equals(IndexDefinition other)
    {
        return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);
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

        if (obj.GetType() != GetType())
        {
            return false;
        }

        return Equals((IndexDefinition)obj);
    }

    public override int GetHashCode()
    {
        return Name != null ? Name.ToLowerInvariant().GetHashCode() : 0;
    }

    public override string ToString()
    {
        return $"Index '{Name}' {(IsUnique ? "UNIQUE " : "")}on columns: {Columns?.Join(", ") ?? Expression ?? ""}";
    }
}
