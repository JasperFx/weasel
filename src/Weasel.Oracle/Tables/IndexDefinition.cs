using System.Text;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Oracle.Tables;

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

    /// <summary>
    ///     The whole index's direction. <see cref="SortOrder.Desc" /> appends a single trailing
    ///     <c>DESC</c>, which Oracle attaches to the last key column only; use
    ///     <see cref="DescendingColumns" /> to say which columns sort descending.
    /// </summary>
    /// <remarks>
    ///     Kept as it has always rendered: reading it as "every column descending" would redefine every
    ///     existing multi-column index declared with it, and the next migration would rebuild them all.
    ///     Ignored when <see cref="DescendingColumns" /> names any column.
    /// </remarks>
    public SortOrder SortOrder { get; set; } = SortOrder.Asc;

    /// <summary>
    ///     Key columns that sort descending, for an index that mixes directions --
    ///     <c>(a, b DESC, c)</c>, which <see cref="SortOrder" /> cannot say.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         An index read back from the database carries its direction here, one entry per column
    ///         <c>ALL_IND_COLUMNS</c> reports as <c>DESC</c>. Oracle stores a descending key as a hidden
    ///         <c>SYS_NC…$</c> column over the real one, so the name is recovered from
    ///         <c>ALL_IND_EXPRESSIONS</c>. Both sides render per column, so direction is compared.
    ///     </para>
    ///     <para>
    ///         An entry that is not a key column throws when the index is rendered, as does any entry on
    ///         an index built from <see cref="FunctionExpression" />, which carries its own direction.
    ///     </para>
    /// </remarks>
    public ISet<string> DescendingColumns { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

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
    ///     Always <c>null</c>. Oracle has no partial indexes, so the setter throws rather than
    ///     accepting a predicate it will not honour.
    /// </summary>
    /// <remarks>
    ///     This property used to be settable and did nothing at all — never emitted into DDL, never
    ///     read during delta detection. A caller who set one got an index silently wider than the
    ///     one they asked for, with no error and no drift to notice (weasel#449). The supported way
    ///     to index a subset of rows on Oracle is a function-based index over a <c>CASE</c>
    ///     expression that returns NULL for the rows to exclude.
    /// </remarks>
    public string? Predicate
    {
        get => null;
        set
        {
            if (value != null)
            {
                throw new NotSupportedException(
                    "Oracle does not support partial indexes. Use a function-based index over a CASE "
                    + "expression that returns NULL for the excluded rows instead.");
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
                IndexType = OracleIndexType.BTree;
            }
            else if (Enum.TryParse<OracleIndexType>(value, ignoreCase: true, out var known))
            {
                IndexType = known;
            }
            else
            {
                throw new NotSupportedException($"Oracle does not support index method '{value}'");
            }
        }
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

    bool Weasel.Core.ITableIndex.HasProviderSpecificOptions
        => SortOrder != SortOrder.Asc
           || DescendingColumns.Count > 0
           || IndexType != OracleIndexType.BTree
           || FunctionExpression.IsNotEmpty()
           || Tablespace.IsNotEmpty();

    string Weasel.Core.ITableIndex.ToDDL(Weasel.Core.ITable parent) => ToDDL((Table)parent);

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
            if (DescendingColumns.Any())
            {
                throw new InvalidOperationException(
                    $"Index {Name} is built from a function expression, so DescendingColumns cannot apply to it. Write DESC into FunctionExpression instead.");
            }

            return $"({FunctionExpression})";
        }

        if (_columns == null || !_columns.Any())
        {
            throw new InvalidOperationException("IndexDefinition requires at least one field");
        }

        // Quoted: a column named "Order Date" or a reserved word is legal and now reaches here
        // unrewritten (weasel#458). QuoteName leaves a conventional Oracle identifier bare, so
        // the folded path is unchanged.
        var quoted = Columns.Select(SchemaUtils.QuoteName).ToArray();

        if (DescendingColumns.Any())
        {
            var descending = DescendingColumns.Select(SchemaUtils.Unquote).ToHashSet(StringComparer.OrdinalIgnoreCase);

            // A name here that is not a key column is always a mistake -- a typo, or a column that was
            // renamed and left behind. Ignoring it would emit an index whose direction is not what the
            // model says.
            var keys = Columns.Select(SchemaUtils.Unquote).ToArray();
            var stray = descending.Where(x => !keys.Contains(x, StringComparer.OrdinalIgnoreCase)).ToArray();
            if (stray.Any())
            {
                throw new InvalidOperationException(
                    $"Index {Name} marks {stray.Join(", ")} descending, but {(stray.Length == 1 ? "it is not a key column" : "they are not key columns")} of the index. Key columns are: {Columns.Join(", ")}.");
            }

            return $"({keys.Select((c, i) => descending.Contains(c) ? $"{quoted[i]} DESC" : quoted[i]).Join(", ")})";
        }

        var expression = quoted.Join(", ");

        if (SortOrder != SortOrder.Asc)
        {
            // One trailing DESC, which attaches to the last column. See SortOrder.
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
