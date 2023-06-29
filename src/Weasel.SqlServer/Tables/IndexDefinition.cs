using System.Text;
using JasperFx.Core;

namespace Weasel.SqlServer.Tables;

public class IndexDefinition: INamed
{
    private readonly IList<string> _columns = new List<string>();
    private readonly IList<string> _includedColumns = new List<string>();

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

    public string[] Columns
    {
        get => _columns.ToArray();
        set
        {
            _columns.Clear();
            _columns.AddRange(value);
        }
    }

    public string[] IncludedColumns
    {
        get => _includedColumns.ToArray();
        set
        {
            _includedColumns.Clear();
            _includedColumns.AddRange(value);
        }
    }

    /// <summary>
    ///     The constraint expression for a partial index.
    /// </summary>
    public string? Predicate { get; set; }

    /// <summary>
    ///     Set a non-default fill factor on this index
    /// </summary>
    public int? FillFactor { get; set; }

    public bool IsClustered { get; set; }


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

        if (IsClustered)
        {
            builder.Append("CLUSTERED ");
        }

        if (IsUnique)
        {
            builder.Append("UNIQUE ");
        }

        builder.Append("INDEX ");

        builder.Append(Name);


        builder.Append(" ON ");
        builder.Append(parent.Identifier);

        builder.Append(" ");
        builder.Append(correctedExpression());

        if (Predicate.IsNotEmpty())
        {
            builder.Append(" WHERE ");
            builder.Append($"({Predicate})");
        }

        if (FillFactor.HasValue && FillFactor > 0)
        {
            builder.Append($" WITH (fillfactor={FillFactor})");
        }

        if (_includedColumns.Any())
        {
            builder.Append(" INCLUDE (");
            builder.Append(_includedColumns.Join(", "));
            builder.Append(')');
        }

        builder.Append(";");


        return builder.ToString();
    }

    private string correctedExpression()
    {
        if (!Columns.Any())
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
        var expectedExpression = correctedExpression();

        var expectedSql = CanonicizeDdl(this, parent);

        var actualSql = CanonicizeDdl(actual, parent);

        return expectedSql == actualSql;
    }

    public void AssertMatches(IndexDefinition actual, Table parent)
    {
        var expectedExpression = correctedExpression();

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
                .Replace("\"\"", "\"")
                .Replace("  ", " ")
                .Replace("(", "")
                .Replace(")", "")
                .Replace("INDEX CONCURRENTLY", "INDEX")
                .Replace("::text", "")
                .Replace(" ->> ", "->>")
                .Replace("->", "->").TrimEnd(new[] { ';' })
            ;
    }

    public void AddColumn(string columnName)
    {
        _columns.Add(columnName);
    }
}
