using System.Text;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.MySql.Tables;

public class IndexDefinition: ITableIndex
{
    private readonly IList<string> _columns = new List<string>();
    private string? _indexName;

    public IndexDefinition(string indexName)
    {
        _indexName = SchemaUtils.Unquote(indexName);
    }

    protected IndexDefinition()
    {
    }

    public MySqlIndexType IndexType { get; set; } = MySqlIndexType.BTree;

    /// <summary>
    ///     The direction of every key column at once. <see cref="SortOrder.Desc" /> writes <c>DESC</c> after
    ///     each of them; use <see cref="DescendingColumns" /> for an index that mixes directions.
    /// </summary>
    public SortOrder SortOrder { get; set; } = SortOrder.Asc;

    /// <summary>
    ///     Key columns that sort descending, when the index mixes directions -- <c>(a, b DESC, c)</c>, which
    ///     <see cref="SortOrder" /> cannot say.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         An index read back from the database carries its direction here, one entry per column that
    ///         <c>information_schema.STATISTICS</c> reports with a <c>COLLATION</c> of <c>D</c>, and the
    ///         comparison renders both sides per column. An index whose every column is descending
    ///         therefore matches a model that says <see cref="SortOrder.Desc" />.
    ///     </para>
    ///     <para>
    ///         MySQL honours a key column's direction from 8.0 on. 5.7 parses <c>DESC</c> and ignores it,
    ///         reporting every column ascending, so a descending declaration reports drift there.
    ///     </para>
    /// </remarks>
    public ISet<string> DescendingColumns { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    public bool IsUnique { get; set; }

    public string[] Columns
    {
        get => _columns.ToArray();
        set
        {
            _columns.Clear();
            _columns.AddRange(value.Select(SchemaUtils.Unquote));
        }
    }

    /// <summary>
    ///     Always <c>null</c>. MySQL has no partial indexes, so the setter throws rather than
    ///     accepting a predicate it will not honour.
    /// </summary>
    /// <remarks>
    ///     This property used to be settable and did nothing at all — never emitted into DDL, never
    ///     read during delta detection. A caller who set one got an index silently wider than the
    ///     one they asked for, with no error and no drift to notice (weasel#449). The supported way
    ///     to index a subset of rows on MySQL is a generated column carrying the condition, indexed
    ///     normally.
    /// </remarks>
    public string? Predicate
    {
        get => null;
        set
        {
            if (value != null)
            {
                throw new NotSupportedException(
                    "MySQL does not support partial indexes. Add a generated column for the condition "
                    + "and index that instead.");
            }
        }
    }

    string[]? Weasel.Core.ITableIndex.IncludeColumns
    {
        get => null;
        set => throw new NotSupportedException("Covering (INCLUDE) indexes are not supported by this database provider");
    }

    string? Weasel.Core.ITableIndex.Method
    {
        get => IndexType.ToString();
        set
        {
            if (value == null)
            {
                IndexType = MySqlIndexType.BTree;
            }
            else if (Enum.TryParse<MySqlIndexType>(value, ignoreCase: true, out var known))
            {
                IndexType = known;
            }
            else
            {
                throw new NotSupportedException($"MySQL does not support index method '{value}'");
            }
        }
    }

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
        set => _indexName = SchemaUtils.Unquote(value);
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
        _columns.AddRange(columns.Select(SchemaUtils.Unquote));
        return this;
    }

    bool Weasel.Core.ITableIndex.HasProviderSpecificOptions
        => SortOrder != SortOrder.Asc
           || DescendingColumns.Count > 0
           || FulltextParser.IsNotEmpty()
           || PrefixLength.HasValue;

    string Weasel.Core.ITableIndex.ToDDL(Weasel.Core.ITable parent) => ToDDL((Table)parent);

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
        builder.Append($"{SchemaUtils.QuoteName(Name)}");
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

        var descending = DescendingColumns.Select(SchemaUtils.Unquote).ToHashSet(StringComparer.OrdinalIgnoreCase);

        // A name here that is not a key column is always a mistake -- a typo, or a column that was
        // renamed and left behind. Ignoring it would emit an index whose direction is not what the
        // model says.
        var stray = descending.Where(x => !Columns.Contains(x, StringComparer.OrdinalIgnoreCase)).ToArray();
        if (stray.Any())
        {
            throw new InvalidOperationException(
                $"Index {Name} marks {stray.Join(", ")} descending, but {(stray.Length == 1 ? "it is not a key column" : "they are not key columns")} of the index. Key columns are: {Columns.Join(", ")}.");
        }

        var columns = Columns.Select(c =>
        {
            var col = $"{SchemaUtils.QuoteName(c)}";
            if (PrefixLength.HasValue)
            {
                col += $"({PrefixLength.Value})";
            }

            // Fulltext and spatial indexes don't support ASC/DESC
            if (IndexType != MySqlIndexType.Fulltext && IndexType != MySqlIndexType.Spatial)
            {
                if (SortOrder == SortOrder.Desc || descending.Contains(c))
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
        _columns.Add(SchemaUtils.Unquote(columnName));
    }
}
