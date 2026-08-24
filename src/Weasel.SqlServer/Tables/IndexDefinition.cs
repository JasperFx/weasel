using System.Text;
using System.Text.RegularExpressions;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.SqlServer.Tables;

public class IndexDefinition: ITableIndex
{
    private readonly IList<string> _columns = new List<string>();
    private readonly IList<string> _includedColumns = new List<string>();

    private string? _indexName;

    public IndexDefinition(string indexName)
    {
        _indexName = SchemaUtils.Unbracket(indexName);
    }

    protected IndexDefinition()
    {
    }

    public SortOrder SortOrder { get; set; } = SortOrder.Asc;

    /// <summary>
    ///     Key columns that sort descending, when the index mixes directions.
    /// </summary>
    /// <remarks>
    ///     <see cref="SortOrder" /> applies to the index as a whole and appends a single trailing
    ///     <c>DESC</c>, which can only describe an index whose last key column is the descending one.
    ///     SQL Server sets the direction per column, so an index on <c>(a, b DESC, c)</c> has no
    ///     representation in <see cref="SortOrder" /> at all.
    /// </remarks>
    public ISet<string> DescendingColumns { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Compare per-column key direction as well as the column list. Off by default.
    /// </summary>
    /// <remarks>
    ///     Direction is read from the database faithfully into <see cref="DescendingColumns" />, but
    ///     comparing it against a model that never stated one would report a difference the user
    ///     cannot resolve — and the migration "fixing" it would drop the index and recreate it from
    ///     the coarse rendering, silently flipping columns from DESC to ASC. So the comparison is
    ///     opt-in, and the opt-in is this flag rather than "is <see cref="DescendingColumns" />
    ///     populated": inferring intent from a populated collection means one stale or misspelled
    ///     entry silently turns strict comparison on.
    /// </remarks>
    public bool CompareColumnDirection { get; set; }

    public bool IsUnique { get; set; }

    public string[] Columns
    {
        get => _columns.ToArray();
        set
        {
            _columns.Clear();
            _columns.AddRange(value.Select(SchemaUtils.Unbracket));
        }
    }

    public string[] IncludedColumns
    {
        get => _includedColumns.ToArray();
        set
        {
            _includedColumns.Clear();
            _includedColumns.AddRange(value.Select(SchemaUtils.Unbracket));
        }
    }

    string[]? ITableIndex.IncludeColumns
    {
        get => _includedColumns.Any() ? _includedColumns.ToArray() : null;
        set => IncludedColumns = value ?? [];
    }

    string? ITableIndex.Method
    {
        get => null;
        set
        {
            if (value != null)
            {
                throw new NotSupportedException("SQL Server indexes do not have pluggable access methods");
            }
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
        set => _indexName = SchemaUtils.Unbracket(value);
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
        _columns.AddRange(columns.Select(SchemaUtils.Unbracket));
        return this;
    }

    public string ToDDL(Table parent) => ToDDL(parent, true);

    /// <summary>
    ///     Render the index, optionally ignoring <see cref="DescendingColumns" /> and falling back to
    ///     the coarse trailing <c>DESC</c>. The coarse form is what comparison uses unless the model
    ///     opted into <see cref="CompareColumnDirection" />.
    /// </summary>
    internal string ToDDL(Table parent, bool usePerColumnDirection)
    {
        var builder = new StringBuilder();

        builder.Append("CREATE ");

        // UNIQUE comes before CLUSTERED; the reverse order is a syntax error.
        if (IsUnique)
        {
            builder.Append("UNIQUE ");
        }

        if (IsClustered)
        {
            builder.Append("CLUSTERED ");
        }

        builder.Append("INDEX ");

        // Bracketed: real databases carry index names that are not regular identifiers -- an
        // unfilled "<Name of Missing Index, sysname,>" template turns up more than once in the wild.
        builder.Append(SchemaUtils.QuoteName(Name));


        builder.Append(" ON ");
        builder.Append(parent.Identifier);

        builder.Append(" ");
        builder.Append(correctedExpression(usePerColumnDirection));

        // Clause order is fixed by SQL Server's grammar: INCLUDE, then WHERE, then WITH. Emitting
        // them in any other order is a syntax error, which only shows up on an index that uses more
        // than one of them at once.
        if (_includedColumns.Any())
        {
            builder.Append(" INCLUDE (");
            builder.Append(_includedColumns.Select(SchemaUtils.QuoteName).Join(", "));
            builder.Append(')');
        }

        if (Predicate.IsNotEmpty())
        {
            builder.Append(" WHERE ");
            builder.Append($"({Predicate})");
        }

        if (FillFactor.HasValue && FillFactor > 0)
        {
            builder.Append($" WITH (fillfactor={FillFactor})");
        }

        builder.Append(";");


        return builder.ToString();
    }

    private string correctedExpression(bool usePerColumnDirection = true)
    {
        if (!Columns.Any())
        {
            throw new InvalidOperationException("IndexDefinition requires at least one field");
        }

        // Quoted: a column named "Table" (or any other reserved word) is legal in SQL Server and
        // turns up in real schemas. QuoteName leaves ordinary identifiers untouched.
        var quoted = Columns.Select(SchemaUtils.QuoteName).ToArray();

        if (DescendingColumns.Any())
        {
            // A name here that is not a key column is always a mistake -- a typo, or a column that
            // was renamed and left behind. Silently ignoring it would emit an index whose direction
            // is not what the model says. Compared unbracketed on both sides, since QuoteName passes
            // through an entry the caller already bracketed.
            var keys = Columns.Select(SchemaUtils.Unbracket).ToArray();
            var stray = DescendingColumns
                .Where(x => !keys.Contains(SchemaUtils.Unbracket(x), StringComparer.OrdinalIgnoreCase))
                .ToArray();
            if (stray.Any())
            {
                throw new InvalidOperationException(
                    $"Index {Name} marks {stray.Join(", ")} descending, but {(stray.Length == 1 ? "it is not a key column" : "they are not key columns")} of the index. Key columns are: {Columns.Join(", ")}.");
            }
        }

        if (usePerColumnDirection && DescendingColumns.Any())
        {
            var descending = DescendingColumns
                .Select(SchemaUtils.Unbracket).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var perColumn = Columns
                .Select((c, i) => descending.Contains(SchemaUtils.Unbracket(c)) ? $"{quoted[i]} DESC" : quoted[i])
                .Join(", ");

            return $"({perColumn})";
        }

        var expression = quoted.Join(", ");

        if (SortOrder != SortOrder.Asc)
        {
            // A whole-index SortOrder appends one DESC, which attaches to the last column.
            // Deliberately not re-interpreted as all-columns-descending: that would silently
            // redefine every existing multi-column index using this property.
            expression += " DESC";
        }

        return $"({expression})";
    }

    /// <summary>
    ///     Does <paramref name="actual" /> satisfy this index, using the receiver's
    ///     <see cref="CompareColumnDirection" /> setting?
    /// </summary>
    /// <remarks>
    ///     <b>Directional</b>: the receiver is the declared index and the argument is what the
    ///     database holds. Because the setting comes from the receiver this is not an equivalence
    ///     relation, so do not use it for set membership; pass the setting to the overload instead.
    /// </remarks>
    public bool Matches(IndexDefinition actual, Table parent)
        => Matches(actual, parent, CompareColumnDirection);

    /// <summary>
    ///     Does <paramref name="actual" /> satisfy this index, comparing per-column direction only
    ///     when <paramref name="compareColumnDirection" /> says to? Symmetric for a given setting.
    /// </summary>
    public bool Matches(IndexDefinition actual, Table parent, bool compareColumnDirection)
        => CanonicizeDdl(this, parent, compareColumnDirection)
           == CanonicizeDdl(actual, parent, compareColumnDirection);

    public void AssertMatches(IndexDefinition actual, Table parent)
    {
        var expectedSql = CanonicizeDdl(this, parent, CompareColumnDirection);
        var actualSql = CanonicizeDdl(actual, parent, CompareColumnDirection);

        if (expectedSql != actualSql)
        {
            // Comparing leniently is a policy decision; reporting falsely is not, so the message
            // always shows the direction the database actually holds.
            throw new Exception(
                $"Index did not match, expected{Environment.NewLine}{expectedSql}{Environment.NewLine}but got:{Environment.NewLine}{CanonicizeDdl(actual, parent, true)}");
        }
    }

    /// <summary>
    ///     Normalize an index's DDL for comparison against the same index read back out of
    ///     <c>sys.indexes</c>.
    /// </summary>
    /// <remarks>
    ///     SQL Server rewrites a filtered index's predicate when it stores it: <c>quantity > 0</c>
    ///     comes back as <c>([quantity]&gt;(0))</c>. Brackets and the spacing around comparison
    ///     operators therefore have to be normalized away, or every filtered index reports drift on
    ///     every check — the disease weasel#445 and weasel#446 were about, found here by the shared
    ///     index scenario matrix (weasel#449).
    /// </remarks>
    public static string CanonicizeDdl(IndexDefinition index, Table parent)
        => CanonicizeDdl(index, parent, true);

    public static string CanonicizeDdl(IndexDefinition index, Table parent, bool compareColumnDirection)
    {
        var sql = index.ToDDL(parent, compareColumnDirection)
            .Replace("\"\"", "\"")
            .Replace("  ", " ")
            .Replace("(", "")
            .Replace(")", "")
            .Replace("[", "")
            .Replace("]", "")
            .Replace("INDEX CONCURRENTLY", "INDEX")
            .Replace("::text", "")
            .Replace(" ->> ", "->>")
            .Replace("->", "->")
            .TrimEnd(';');

        // Collapse the whitespace SQL Server adds or removes around operators and separators.
        return Regex.Replace(sql, @"\s*([<>=!+\-*/,]+)\s*", "$1");
    }

    public void AddColumn(string columnName)
    {
        _columns.Add(SchemaUtils.Unbracket(columnName));
    }

    public void AddIncludedColumn(string columnName)
    {
        _includedColumns.Add(SchemaUtils.Unbracket(columnName));
    }
}
