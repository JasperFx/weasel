using System.Text;
using System.Text.RegularExpressions;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Firebird.Tables;

public class IndexDefinition: ITableIndex
{
    private static readonly Regex WhereKeyword = new(@"^\s*WHERE\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private readonly List<string> _columns = new();
    private string? _indexName;

    public IndexDefinition(string indexName)
    {
        _indexName = SchemaUtils.Unquote(indexName);
    }

    protected IndexDefinition()
    {
    }

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

    public string[] Columns
    {
        get => _columns.ToArray();
        set
        {
            _columns.Clear();
            _columns.AddRange(value.Select(SchemaUtils.Unquote));
        }
    }

    public bool IsUnique { get; set; }

    /// <summary>
    ///     The direction of the whole index. Firebird has no per-column direction, so
    ///     <see cref="SortOrder.Desc" /> is the only way to say "descending": every key sorts that way.
    /// </summary>
    public SortOrder SortOrder { get; set; } = SortOrder.Asc;

    /// <summary>
    ///     Key columns that sort descending, for the model shared with the other providers. Firebird
    ///     accepts this only when it names every key column -- which is a descending index -- and refuses
    ///     a mix when the DDL is rendered, before anything runs, because a Firebird index has one
    ///     direction.
    /// </summary>
    public ISet<string> DescendingColumns { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     An expression index: <c>CREATE INDEX … COMPUTED BY (expression)</c>, in place of
    ///     <see cref="Columns" />. Written without its outer parentheses.
    /// </summary>
    public string? Expression { get; set; }

    /// <summary>
    ///     A partial index's condition, without the <c>WHERE</c>. Firebird 5 and later only; an earlier
    ///     server refuses it as a syntax error.
    /// </summary>
    public string? Predicate { get; set; }

    /// <summary>
    ///     Read from the catalog: the index exists but is switched off (<c>ALTER INDEX … INACTIVE</c>).
    ///     A model's index is always active, so an inactive one compares as different and is rebuilt.
    /// </summary>
    public bool IsInactive { get; internal set; }

    string[]? ITableIndex.Columns
    {
        get => Columns;
        set => Columns = value ?? [];
    }

    string[]? ITableIndex.IncludeColumns
    {
        get => null;
        set
        {
            if (value is { Length: > 0 })
            {
                throw new NotSupportedException("Firebird does not support covering (INCLUDE) indexes");
            }
        }
    }

    string? ITableIndex.Method
    {
        get => null;
        set
        {
            if (value != null)
            {
                throw new NotSupportedException(
                    $"Firebird does not support index method '{value}'; every Firebird index is a B-tree");
            }
        }
    }

    bool ITableIndex.HasProviderSpecificOptions => isDescending || Expression.IsNotEmpty();

    string ITableIndex.ToDDL(ITable parent) => ToDDL((Table)parent);

    /// <summary>
    ///     Set the index expression against the supplied columns
    /// </summary>
    public IndexDefinition AgainstColumns(params string[] columns)
    {
        Columns = columns;
        return this;
    }

    public void AddColumn(string columnName)
    {
        _columns.Add(SchemaUtils.Unquote(columnName));
    }

    private bool isDescending => SortOrder == SortOrder.Desc || DescendingColumns.Count > 0;

    /// <summary>
    ///     The name as Firebird's catalog stores it, for <paramref name="parent" />.
    /// </summary>
    internal string CatalogName(Table parent) => SchemaUtils.CatalogName(Name, parent.PreserveIdentifierCase);

    /// <summary>
    ///     This index's <c>CREATE INDEX</c> statement, unterminated and unguarded.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///     The index mixes directions, marks a column descending that is not a key column, or has
    ///     neither columns nor an expression.
    /// </exception>
    public string ToDDL(Table parent)
    {
        var builder = new StringBuilder("CREATE ");

        if (IsUnique)
        {
            builder.Append("UNIQUE ");
        }

        if (resolveDescending())
        {
            builder.Append("DESCENDING ");
        }

        builder.Append("INDEX ");
        builder.Append(SchemaUtils.QuoteName(Name, parent.PreserveIdentifierCase));
        builder.Append(" ON ");
        builder.Append(parent.QuotedName);

        if (Expression.IsNotEmpty())
        {
            builder.Append(" COMPUTED BY (").Append(Expression!.Trim()).Append(')');
        }
        else
        {
            if (_columns.Count == 0)
            {
                throw new InvalidOperationException($"Index {Name} has neither columns nor an expression");
            }

            builder.Append(" (")
                .Append(_columns.Select(x => SchemaUtils.QuoteName(x, parent.PreserveIdentifierCase)).Join(", "))
                .Append(')');
        }

        if (Predicate.IsNotEmpty())
        {
            builder.Append(" WHERE ").Append(Predicate!.Trim());
        }

        return builder.ToString();
    }

    /// <summary>
    ///     Firebird's one direction per index, from the two ways the shared model can say it.
    /// </summary>
    private bool resolveDescending()
    {
        if (DescendingColumns.Count == 0)
        {
            return SortOrder == SortOrder.Desc;
        }

        if (Expression.IsNotEmpty())
        {
            throw new InvalidOperationException(
                $"Index {Name} is built from an expression, so DescendingColumns cannot apply to it. Set SortOrder to Desc instead.");
        }

        var descending = DescendingColumns.Select(SchemaUtils.Unquote).ToArray();
        var stray = descending.Where(x => !_columns.Contains(x, StringComparer.OrdinalIgnoreCase)).ToArray();
        if (stray.Any())
        {
            throw new InvalidOperationException(
                $"Index {Name} marks {stray.Join(", ")} descending, but {(stray.Length == 1 ? "it is not a key column" : "they are not key columns")} of the index. Key columns are: {Columns.Join(", ")}.");
        }

        var ascending = _columns.Where(x => !descending.Contains(x, StringComparer.OrdinalIgnoreCase)).ToArray();
        if (ascending.Any())
        {
            throw new InvalidOperationException(
                $"Index {Name} mixes directions ({ascending.Join(", ")} ascending, {descending.Join(", ")} descending), but a Firebird index is ascending or descending as a whole. Mark every key column descending, or set SortOrder.");
        }

        return true;
    }

    /// <summary>
    ///     Does <paramref name="actual" /> -- read from the catalog -- have this index's shape? Names are
    ///     compared ignoring case, as Firebird resolves them, and an expression or condition ignoring
    ///     case and whitespace outside its literals.
    /// </summary>
    public bool Matches(IndexDefinition actual, Table parent)
    {
        // isDescending rather than resolveDescending: a comparison reports, it does not refuse. A mixed
        // direction is refused when the index is rendered, which is still before anything runs.
        return IsUnique == actual.IsUnique
               && isDescending == actual.isDescending
               && IsInactive == actual.IsInactive
               && Columns.SequenceEqual(actual.Columns, StringComparer.OrdinalIgnoreCase)
               && ExpressionText.Canonical(Expression) == ExpressionText.Canonical(actual.Expression)
               && ExpressionText.Canonical(Predicate) == ExpressionText.Canonical(actual.Predicate);
    }

    public void AssertMatches(IndexDefinition actual, Table parent)
    {
        if (!Matches(actual, parent))
        {
            throw new Exception(
                $"Index did not match, expected{Environment.NewLine}{ToDDL(parent)}{Environment.NewLine}but got:{Environment.NewLine}{actual.ToDDL(parent)}{(actual.IsInactive ? " (inactive)" : "")}");
        }
    }

    /// <summary>
    ///     An expression source as <c>RDB$INDICES</c> stores it, <c>(UPPER(NAME))</c>, without the outer
    ///     parentheses the server keeps.
    /// </summary>
    internal static string? ReadExpression(string? source) => ExpressionText.WithoutOuterParentheses(source);

    /// <summary>
    ///     A condition source as Firebird 5 stores it, <c>WHERE b &gt; 0</c>, without the keyword.
    /// </summary>
    internal static string? ReadPredicate(string? source)
        => source.IsEmpty() ? null : WhereKeyword.Replace(source!, "").Trim();
}
