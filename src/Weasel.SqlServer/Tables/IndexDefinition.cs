using System.Text;
using JasperFx.Core;
using Weasel.Core;
using Weasel.Core.Tables;

namespace Weasel.SqlServer.Tables;

/// <summary>
/// SQL Server-specific index definition implementation
/// </summary>
public class IndexDefinition : IndexDefinitionBase, INamed
{
    private readonly IList<string> _columns = new List<string>();
    private readonly IList<string> _includedColumns = new List<string>();
    private int? _fillFactor;

    public IndexDefinition(string indexName) : base(indexName)
    {
    }

    protected IndexDefinition() : base()
    {
    }

    public override string[]? Columns
    {
        get => _columns.ToArray();
        set
        {
            _columns.Clear();
            if (value != null)
            {
                _columns.AddRange(value);
            }
        }
    }

    /// <summary>
    /// The columns to include in the index (non-key columns)
    /// </summary>
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
    /// Set a non-default fill factor on this index
    /// </summary>
    public override int? FillFactor
    {
        get => _fillFactor;
        set => _fillFactor = value;
    }

    /// <summary>
    /// Indicates whether this is a clustered index
    /// </summary>
    public bool IsClustered { get; set; }

    protected override string DeriveIndexName()
    {
        throw new NotSupportedException();
    }

    /// <summary>
    /// Set the Index expression against the supplied columns
    /// </summary>
    public new IndexDefinition AgainstColumns(params string[] columns)
    {
        _columns.Clear();
        _columns.AddRange(columns);
        return this;
    }

    /// <summary>
    /// Adds a column to the index
    /// </summary>
    public void AddColumn(string columnName)
    {
        _columns.Add(columnName);
    }

    public string ToDDL(Table parent)
    {
        return ToDDL(parent.Identifier);
    }

    public override string ToDDL(DbObjectName tableIdentifier)
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
        builder.Append(tableIdentifier);

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
        if (Columns == null || !Columns.Any())
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
        return Matches(actual, parent.Identifier);
    }

    public override bool Matches(IndexDefinitionBase actual, DbObjectName tableIdentifier)
    {
        if (actual is not IndexDefinition sqlActual)
        {
            return false;
        }

        var expectedSql = CanonicizeDdl(this, tableIdentifier);
        var actualSql = CanonicizeDdl(sqlActual, tableIdentifier);

        return expectedSql == actualSql;
    }

    public void AssertMatches(IndexDefinition actual, Table parent)
    {
        AssertMatches(actual, parent.Identifier);
    }

    public override void AssertMatches(IndexDefinitionBase actual, DbObjectName tableIdentifier)
    {
        if (actual is not IndexDefinition sqlActual)
        {
            throw new Exception("Expected SQL Server IndexDefinition");
        }

        var expectedSql = CanonicizeDdl(this, tableIdentifier);
        var actualSql = CanonicizeDdl(sqlActual, tableIdentifier);

        if (expectedSql != actualSql)
        {
            throw new Exception(
                $"Index did not match, expected{Environment.NewLine}{expectedSql}{Environment.NewLine}but got:{Environment.NewLine}{actualSql}");
        }
    }

    public static string CanonicizeDdl(IndexDefinition index, Table parent)
    {
        return CanonicizeDdl(index, parent.Identifier);
    }

    public static string CanonicizeDdl(IndexDefinition index, DbObjectName tableIdentifier)
    {
        return index.ToDDL(tableIdentifier)
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
}
