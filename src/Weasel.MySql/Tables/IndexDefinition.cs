using System.Text;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.MySql.Tables;

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

    public MySqlIndexType IndexType { get; set; } = MySqlIndexType.BTree;
    public SortOrder SortOrder { get; set; } = SortOrder.Asc;
    public bool IsUnique { get; set; }

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
    ///     The constraint expression for a partial index (WHERE clause).
    ///     Note: MySQL doesn't support partial indexes natively in the same way as PostgreSQL.
    /// </summary>
    public string? Predicate { get; set; }

    /// <summary>
    ///     For FULLTEXT indexes, specify the parser to use (e.g., "ngram" or "mecab").
    /// </summary>
    public string? FulltextParser { get; set; }

    /// <summary>
    ///     Prefix length for string columns (e.g., VARCHAR(255) indexed with prefix of 10).
    /// </summary>
    public int? PrefixLength { get; set; }

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

        switch (IndexType)
        {
            case MySqlIndexType.Fulltext:
                builder.Append("FULLTEXT ");
                break;
            case MySqlIndexType.Spatial:
                builder.Append("SPATIAL ");
                break;
            default:
                if (IsUnique)
                {
                    builder.Append("UNIQUE ");
                }
                break;
        }

        builder.Append("INDEX ");
        builder.Append($"`{Name}`");
        builder.Append(" ON ");
        builder.Append(parent.Identifier.QualifiedName);
        builder.Append(" ");
        builder.Append(correctedExpression());

        // Add index type for non-fulltext/spatial
        if (IndexType == MySqlIndexType.Hash)
        {
            builder.Append(" USING HASH");
        }
        else if (IndexType == MySqlIndexType.BTree && !IsUnique)
        {
            // BTree is default, only explicit if needed
        }

        if (IndexType == MySqlIndexType.Fulltext && FulltextParser.IsNotEmpty())
        {
            builder.Append($" WITH PARSER {FulltextParser}");
        }

        builder.Append(";");

        return builder.ToString();
    }

    private string correctedExpression()
    {
        if (!Columns.Any())
        {
            throw new InvalidOperationException("IndexDefinition requires at least one column");
        }

        var columns = Columns.Select(c =>
        {
            var col = $"`{c}`";
            if (PrefixLength.HasValue)
            {
                col += $"({PrefixLength.Value})";
            }

            // Fulltext and spatial indexes don't support ASC/DESC
            if (IndexType != MySqlIndexType.Fulltext && IndexType != MySqlIndexType.Spatial)
            {
                if (SortOrder == SortOrder.Desc)
                {
                    col += " DESC";
                }
            }

            return col;
        });

        return $"({columns.Join(", ")})";
    }

    public bool Matches(IndexDefinition actual, Table parent)
    {
        var expectedSql = CanonicizeDdl(this, parent);
        var actualSql = CanonicizeDdl(actual, parent);

        return expectedSql == actualSql;
    }

    public void AssertMatches(IndexDefinition actual, Table parent)
    {
        var expectedSql = CanonicizeDdl(this, parent);
        var actualSql = CanonicizeDdl(actual, parent);

        if (expectedSql != actualSql)
        {
            throw new Exception(
                $"Index did not match, expected{Environment.NewLine}{expectedSql}{Environment.NewLine}but got:{Environment.NewLine}{actualSql}");
        }
    }

    public static string CanonicizeDdl(IndexDefinition index, Table parent)
    {
        return index.ToDDL(parent)
            .Replace("`", "")
            .Replace("  ", " ")
            .ToUpperInvariant()
            .TrimEnd(';');
    }

    public void AddColumn(string columnName)
    {
        _columns.Add(columnName);
    }
}
