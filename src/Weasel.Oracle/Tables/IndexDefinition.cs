using System.Text;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Oracle.Tables;

public class IndexDefinition: INamed
{
    private readonly IList<string> _columns = new List<string>();

    private string? _indexName;

    public IndexDefinition(string indexName)
    {
        _indexName = indexName;
    }

    protected IndexDefinition()
    {
    }

    public SortOrder SortOrder { get; set; } = SortOrder.Asc;

    public bool IsUnique { get; set; }

    /// <summary>
    /// The type of Oracle index (BTree, Bitmap, FunctionBased)
    /// </summary>
    public OracleIndexType IndexType { get; set; } = OracleIndexType.BTree;

    /// <summary>
    /// For function-based indexes, the expression to index on
    /// </summary>
    public string? FunctionExpression { get; set; }

    /// <summary>
    /// Optional tablespace for the index
    /// </summary>
    public string? Tablespace { get; set; }

    public string[] Columns
    {
        get => _columns.ToArray();
        set
        {
            _columns.Clear();
            _columns.AddRange(value);
        }
    }

    /// <summary>
    ///     The constraint expression for a partial index (WHERE clause)
    /// </summary>
    public string? Predicate { get; set; }

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

    protected virtual string deriveIndexName()
    {
        throw new NotSupportedException();
    }

    /// <summary>
    ///     Set the Index expression against the supplied columns
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public IndexDefinition AgainstColumns(params string[] columns)
    {
        _columns.Clear();
        _columns.AddRange(columns);
        return this;
    }

    public string ToDDL(Table parent)
    {
        var builder = new StringBuilder();

        builder.Append("CREATE ");

        if (IndexType == OracleIndexType.Bitmap)
        {
            builder.Append("BITMAP ");
        }

        if (IsUnique)
        {
            builder.Append("UNIQUE ");
        }

        builder.Append("INDEX ");
        builder.Append(parent.Identifier.Schema);
        builder.Append(".");
        builder.Append(Name);
        builder.Append(" ON ");
        builder.Append(parent.Identifier);
        builder.Append(" ");
        builder.Append(correctedExpression());

        if (Tablespace.IsNotEmpty())
        {
            builder.Append($" TABLESPACE {Tablespace}");
        }

        return builder.ToString();
    }

    private string correctedExpression()
    {
        if (IndexType == OracleIndexType.FunctionBased && FunctionExpression.IsNotEmpty())
        {
            return $"({FunctionExpression})";
        }

        if (_columns == null || !_columns.Any())
        {
            throw new InvalidOperationException("IndexDefinition requires at least one field");
        }

        var expression = Columns.Join(", ");

        if (SortOrder != SortOrder.Asc)
        {
            expression += " DESC";
        }

        return $"({expression})";
    }

    public bool Matches(IndexDefinition actual, Table parent)
    {
        var expectedSql = CanonicizeDdl(this, parent);
        var actualSql = CanonicizeDdl(actual, parent);

        return expectedSql.Equals(actualSql, StringComparison.OrdinalIgnoreCase);
    }

    public void AssertMatches(IndexDefinition actual, Table parent)
    {
        var expectedSql = CanonicizeDdl(this, parent);
        var actualSql = CanonicizeDdl(actual, parent);

        if (!expectedSql.Equals(actualSql, StringComparison.OrdinalIgnoreCase))
        {
            throw new Exception(
                $"Index did not match, expected{Environment.NewLine}{expectedSql}{Environment.NewLine}but got:{Environment.NewLine}{actualSql}");
        }
    }

    public static string CanonicizeDdl(IndexDefinition index, Table parent)
    {
        return index.ToDDL(parent)
            .Replace("\"\"", "\"")
            .Replace("  ", " ")
            .Replace("(", "")
            .Replace(")", "")
            .ToUpperInvariant()
            .TrimEnd();
    }

    public void AddColumn(string columnName)
    {
        _columns.Add(columnName);
    }
}
