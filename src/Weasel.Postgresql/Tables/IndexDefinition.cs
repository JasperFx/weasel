using System.Collections;
using System.Collections.Specialized;
using System.Text;
using JasperFx.Core;

namespace Weasel.Postgresql.Tables;

public class IndexDefinition: INamed
{
    private const string JsonbPathOps = "jsonb_path_ops";
    private const string Ascending = "ASC";
    private const string Descending = "DESC";
    private const string NullsFirst = "NULLS FIRST";
    private const string NullsLast = "NULLS LAST";
    private const string AscendingNullsFirst = "ASC NULLS FIRST";
    private const string AscendingNullsLast = "ASC NULLS LAST";
    private const string DescendingNullsFirst = "DESC NULLS FIRST";
    private const string DescendingNullsLast = "DESC NULLS LAST";
    private static readonly string[] _reserved_words = { "trim", "lower", "upper" };
    private string? _customIndexMethod;

    private string? _indexName;

    public IndexDefinition(string indexName)
    {
        _indexName = indexName;
    }

    protected IndexDefinition()
    {
    }

    /// <summary>
    ///     Set the index method using <see cref="IndexMethod" />
    /// </summary>
    public IndexMethod Method { get; set; } = IndexMethod.btree;

    /// <summary>
    ///     Set custom index method not defined in <see cref="IndexMethod" />
    /// </summary>
    public string? CustomMethod
    {
        get => Method == IndexMethod.custom ? _customIndexMethod ?? Method.ToString() : null;
        set
        {
            Method = IndexMethod.custom;
            _customIndexMethod = value;
        }
    }

    /// <summary>
    ///     Set sort order for a btree index column/expression
    /// </summary>
    public SortOrder SortOrder { get; set; } = SortOrder.Asc;

    /// <summary>
    ///     Set the null sort order for a btree index column/expression
    /// </summary>
    public NullsSortOrder NullsSortOrder { get; set; } = NullsSortOrder.None;

    /// <summary>
    ///     Option to create unique index
    /// </summary>
    public bool IsUnique { get; set; }

    /// <summary>
    ///     Option to build index without taking any locks that prevent concurrent inserts, updates or deletes in table
    /// </summary>
    public bool IsConcurrent { get; set; }

    // Define the columns part of the index definition
    public virtual string[]? Columns { get; set; }

    /// <summary>
    ///     Define the columns part of the include clause
    /// </summary>
    public virtual string[]? IncludeColumns { get; set; }

    /// <summary>
    ///     Pattern for surrounding the columns. Use a `?` character
    ///     for the location of the columns, like "? jsonb_path_ops"
    /// </summary>
    public string? Mask { get; set; }

    /// <summary>
    ///     The tablespace in which to create the index. If not specified, default_tablespace is consulted,
    /// </summary>
    public string? TableSpace { get; set; }

    /// <summary>
    ///     The constraint expression for a partial index.
    /// </summary>
    public string? Predicate { get; set; }

    /// <summary>
    ///     Set the collation to be used for the column/expression part of the index
    /// </summary>
    public string? Collation { get; set; }

    /// <summary>
    ///     Set a non-default fill factor on this index
    /// </summary>
    public int? FillFactor
    {
        get => StorageParameters["fillfactor"] as int?;
        set => StorageParameters["fillfactor"] = value;
    }

    /// <summary>
    ///     Method to define the index storage parameters
    /// </summary>
    public OrderedDictionary StorageParameters { get; set; } = new();


    /// <summary>
    ///     The index name used for the index definition
    /// </summary>
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
        Columns = columns;
        return this;
    }


    /// <summary>
    ///     Method to get the DDL statement for the index definition
    /// </summary>
    /// <param name="parent"></param>
    /// <returns></returns>
    public string ToDDL(Table parent)
    {
        var builder = new StringBuilder();

        builder.Append("CREATE ");

        if (IsUnique)
        {
            builder.Append("UNIQUE ");
        }

        builder.Append("INDEX ");

        if (IsConcurrent)
        {
            builder.Append("CONCURRENTLY ");
        }

        builder.Append(Name);

        builder.Append(" ON ");
        builder.Append(parent.Identifier);
        builder.Append(" USING ");
        builder.Append(Method == IndexMethod.custom ? CustomMethod : Method);
        builder.Append(" ");
        builder.Append(correctedExpression());

        if (TableSpace.IsNotEmpty())
        {
            builder.Append(" TABLESPACE ");
            builder.Append(TableSpace);
        }

        if (Predicate.IsNotEmpty())
        {
            builder.Append(" WHERE ");
            builder.Append($"({Predicate})");
        }

        if (StorageParameters.Count > 0)
        {
            builder.Append(" WITH (");

            foreach (DictionaryEntry entry in StorageParameters)
            {
                builder.Append(entry.Key);
                builder.Append('=');
                builder.Append("'");
                builder.Append(entry.Value);
                builder.Append("'");
                builder.Append(", ");
            }

            builder.Length -= 2;
            builder.Append(")");
        }

        if (IncludeColumns != null && IncludeColumns.Any())
        {
            builder.Append(" INCLUDE (");
            builder.Append(IncludeColumns.Join(", "));
            builder.Append(')');
        }

        builder.Append(";");


        return builder.ToString();
    }

    /// <summary>
    ///     Method to normalize a column definition for checking match/equivalene
    /// </summary>
    /// <param name="column"></param>
    /// <returns></returns>
    public static string CanonicizeCast(string column)
    {
        if (!column.Contains("::"))
        {
            return column;
        }

        var index = column.IndexOf("::");
        var type = column.Substring(index + 2);
        var expression = column.Substring(0, index).Trim().TrimStart('(').TrimEnd(')').Replace("  ", " ");

        return $"CAST({expression} as {type})";
    }

    private string correctedExpression()
    {
        if (Columns == null || !Columns.Any())
        {
            throw new InvalidOperationException("IndexDefinition requires at least one field");
        }

        var expression = Columns.Select(x =>
        {
            return _reserved_words.Contains(x) ? $"\"{x}\"" : x;
        }).Join(", ");
        if (Mask.IsNotEmpty())
        {
            expression = Mask.Replace("?", expression);
        }

        if (Collation != null)
        {
            expression += $" COLLATE \"{Collation}\"";
        }

        if (Method == IndexMethod.btree)
        {
            // ASC is default so ignore adding in expression
            // NULLS LAST is default for ASC so ignore adding in expression
            // NULLS FIRST is default for DESC so ignore adding in expression
            if (SortOrder == SortOrder.Asc && NullsSortOrder == NullsSortOrder.First)
            {
                expression += $" {NullsFirst}";
            }
            else if (SortOrder == SortOrder.Desc && NullsSortOrder is NullsSortOrder.None or NullsSortOrder.First)
            {
                expression += $" {Descending}";
            }
            else if (SortOrder == SortOrder.Desc && NullsSortOrder == NullsSortOrder.Last)
            {
                expression += $" {DescendingNullsLast}";
            }
        }

        return $"({expression})";
    }

    /// <summary>
    ///     Makes this index use the Gin method with the jsonb_path_ops operator
    /// </summary>
    public void ToGinWithJsonbPathOps()
    {
        Method = IndexMethod.gin;
        Mask = $"? {JsonbPathOps}";
    }

    public static IndexDefinition Parse(string definition)
    {
        var tokens = new Queue<string>(StringTokenizer.Tokenize(definition.TrimEnd(';')));

        IndexDefinition index = null!;

        var isUnique = false;
        var expression = "";
        var isFullTextIndex = false;

        while (tokens.Any())
        {
            var current = tokens.Dequeue();
            switch (current.ToUpper())
            {
                case "CREATE":
                case "CONCURRENTLY":
                    continue;

                case "INDEX":
                    var name = tokens.Dequeue();
                    index = new IndexDefinition(name) { Mask = string.Empty, IsUnique = isUnique };
                    break;

                case "ON":
                    // Skip the table name
                    tokens.Dequeue();

                    // USING clause is optional hence if next token isn't an USING clause then add it
                    if (!tokens.Peek().Contains("USING", StringComparison.InvariantCultureIgnoreCase))
                    {
                        // btree is default method
                        tokens = new Queue<string>(new[] { "USING", "btree" }.Concat(tokens.ToArray()));
                    }

                    break;

                case "UNIQUE":
                    isUnique = true;
                    break;

                case "USING":
                    var methodName = tokens.Dequeue();
                    if (Enum.TryParse<IndexMethod>(methodName, out var method))
                    {
                        index.Method = method;
                    }
                    else
                    {
                        index.CustomMethod = methodName;
                    }

                    expression = tokens.Dequeue();
                    (expression, index.SortOrder, index.NullsSortOrder) = removeSortOrderFromExpression(expression);

                    if (expression.Contains("COLLATE", StringComparison.OrdinalIgnoreCase))
                    {
                        // ensure to convert keyword to upper case
                        expression = expression.Replace("collate", "COLLATE");
                        var expressionsParts = expression.Split(new[] { " COLLATE " }, StringSplitOptions.None);
                        index.Collation = expressionsParts[1].TrimEnd(')').Trim('"');
                        expression = expressionsParts[0] + ")";
                    }

                    if (expression.EndsWith("jsonb_path_ops)"))
                    {
                        index.Mask = "? jsonb_path_ops";
                        expression = expression.Substring(0, expression.Length - index.Mask.Length) + ")";
                    }

                    if (expression.Contains("to_tsvector"))
                    {
                        isFullTextIndex = true;

                        // Note that full text index definition from db has some differences with the one generated by system
                        // DB ddl definition:
                        // has `::regconfig`
                        // appropriate number of brackets
                        // appropriate spacing between terms

                        // System generated:
                        // does not contain `::regconfig`
                        // more number of brackets and spacing between terms

                        // Overall, we are normalizing the expression here to deal with the above differences
                        // `CanonicizeDdl` method already deals with normalizing brackets so not dealing with it here
                        expression = expression
                            .Replace("::regconfig", "")
                            .Replace(" ", "");
                    }

                    break;

                case "WHERE":
                    var predicate = tokens.Dequeue();
                    index.Predicate = predicate;
                    break;

                case "WITH":
                    var storageParameters = getStorageParameters(tokens.Dequeue());

                    foreach (var parameter in storageParameters)
                    {
                        var parts = parameter.Split('=');

                        if (parts[0].Trim().EqualsIgnoreCase("fillfactor"))
                        {
                            index.FillFactor = int.Parse(parts[1].TrimStart('\'').TrimEnd('\'').Trim());
                        }
                        else
                        {
                            index.StorageParameters[parts[0]] = parts[1].TrimStart('\'').TrimEnd('\'').Trim();
                        }
                    }

                    break;

                case "INCLUDE":
                    index.IncludeColumns = getIncludeColumns(tokens.Dequeue()).ToArray();
                    break;

                case "TABLESPACE":
                    index.TableSpace = tokens.Dequeue();
                    break;

                default:
                    throw new NotImplementedException("NOT YET DEALING WITH " + current);
            }
        }

        if (isFullTextIndex)
        {
            index.Columns = new[] { expression };
        }
        else
        {
            index.Columns = expression.Split(',').Select(canonicizeColumn).ToArray();
        }

        return index;
    }

    private static IEnumerable<string> getStorageParameters(string rawInput)
    {
        rawInput = rawInput.TrimStart('(').TrimEnd(')');

        var builder = new StringBuilder(rawInput.Length);

        var inQuotes = false;

        for (var i = 0; i < rawInput.Length; i++)
        {
            var chr = rawInput[i];
            var nextChr = '\0';

            if (i + 1 < rawInput.Length)
            {
                nextChr = rawInput[i + 1];
            }

            switch (chr)
            {
                case '\'':
                    if (inQuotes)
                    {
                        if (nextChr == '\'')
                        {
                            builder.Append(chr);
                            i++;
                            continue;
                        }

                        if (nextChr != ',' && nextChr != '\0')
                        {
                            throw new ArgumentException(
                                $"Invalid storage parameters: {rawInput}",
                                nameof(rawInput));
                        }

                        inQuotes = false;
                        builder.Append(chr);
                        continue;
                    }

                    inQuotes = true;
                    builder.Append(chr);
                    continue;

                case ',':
                    if (inQuotes)
                    {
                        builder.Append(chr);
                        continue;
                    }

                    yield return builder.ToString();
                    builder.Clear();
                    i++;
                    continue;

                default:
                    builder.Append(chr);
                    break;
            }
        }

        if (builder.Length > 0)
        {
            yield return builder.ToString();
        }
    }

    private static IEnumerable<string> getIncludeColumns(string rawInput)
    {
        rawInput = rawInput.TrimStart('(').TrimEnd(')');
        return rawInput.Split(',').Select(x => x.Trim());
    }

    private static string canonicizeColumn(string expression)
    {
        expression = expression.Trim().Replace("::text", "");
        while (expression.StartsWith("(") && expression.EndsWith(")"))
        {
            expression = expression.Substring(1, expression.Length - 2);
        }

        if (expression.StartsWith('"') && expression.EndsWith('"'))
        {
            expression = expression.Trim('"');
        }

        return CanonicizeCast(expression);
    }

    private static (string expression, SortOrder order, NullsSortOrder nullsOrder) removeSortOrderFromExpression(
        string expression)
    {
        const int spaceAndEndParenthesis = 2;

        return expression switch
        {
            var expr when expr.EndsWith($"{Descending})", StringComparison.InvariantCultureIgnoreCase) =>
                (expr.Substring(0, expr.Length - Descending.Length - spaceAndEndParenthesis) + ")",
                    SortOrder.Desc, NullsSortOrder.None),
            var expr when expr.EndsWith($"{DescendingNullsFirst})", StringComparison.InvariantCultureIgnoreCase) =>
                (expr.Substring(0,
                        expr.Length - DescendingNullsFirst.Length - spaceAndEndParenthesis) + ")",
                    SortOrder.Desc, NullsSortOrder.First),
            var expr when expr.EndsWith($"{DescendingNullsLast})", StringComparison.InvariantCultureIgnoreCase) =>
                (expr.Substring(0,
                        expr.Length - DescendingNullsLast.Length - spaceAndEndParenthesis) + ")",
                    SortOrder.Desc, NullsSortOrder.Last),
            var expr when expr.EndsWith($"{Ascending})", StringComparison.InvariantCultureIgnoreCase) =>
                (expr.Substring(0, expr.Length - Ascending.Length - spaceAndEndParenthesis) + ")",
                    SortOrder.Asc, NullsSortOrder.None),
            var expr when expr.EndsWith($"{AscendingNullsLast})", StringComparison.InvariantCultureIgnoreCase) =>
                (expr.Substring(0, expr.Length - AscendingNullsLast.Length - spaceAndEndParenthesis) + ")",
                    SortOrder.Asc, NullsSortOrder.Last),
            var expr when expr.EndsWith($"{AscendingNullsFirst})", StringComparison.InvariantCultureIgnoreCase) =>
                (expr.Substring(0, expr.Length - AscendingNullsFirst.Length - spaceAndEndParenthesis) + ")",
                    SortOrder.Asc, NullsSortOrder.First),
            var expr when !expr.Contains(Ascending, StringComparison.InvariantCultureIgnoreCase) &&
                          !expr.Contains(Descending, StringComparison.InvariantCultureIgnoreCase) &&
                          expr.EndsWith($"{NullsFirst})", StringComparison.InvariantCultureIgnoreCase) =>
                (expr.Substring(0, expr.Length - NullsFirst.Length - spaceAndEndParenthesis) + ")",
                    SortOrder.Asc, NullsSortOrder.First),
            var expr when !expr.Contains(Ascending, StringComparison.InvariantCultureIgnoreCase) &&
                          !expr.Contains(Descending, StringComparison.InvariantCultureIgnoreCase) &&
                          expr.EndsWith($"{NullsLast})", StringComparison.InvariantCultureIgnoreCase) =>
                (expr.Substring(0, expr.Length - NullsLast.Length - spaceAndEndParenthesis) + ")",
                    SortOrder.Asc, NullsSortOrder.Last),
            _ => (expression.Trim(), SortOrder.Asc, NullsSortOrder.None)
        };
    }

    /// <summary>
    ///     Method to check if the index definition matches with a passed index definition
    /// </summary>
    /// <param name="actual"></param>
    /// <param name="parent"></param>
    /// <returns></returns>
    public bool Matches(IndexDefinition actual, Table parent)
    {
        var expectedExpression = correctedExpression();


        if (actual.Mask == expectedExpression)
        {
            (actual.Mask, _, _) = removeSortOrderFromExpression(expectedExpression);
        }

        var expectedSql = CanonicizeDdl(this, parent);

        var actualSql = CanonicizeDdl(actual, parent);

        return expectedSql == actualSql;
    }

    /// <summary>
    ///     Method to assert if the index definition matches with a passed index definition
    /// </summary>
    /// <param name="actual"></param>
    /// <param name="parent"></param>
    /// <exception cref="Exception"></exception>
    public void AssertMatches(IndexDefinition actual, Table parent)
    {
        var expectedExpression = correctedExpression();


        if (actual.Mask == expectedExpression)
        {
            (actual.Mask, _, _) = removeSortOrderFromExpression(expectedExpression);
        }

        var expectedSql = CanonicizeDdl(this, parent);

        var actualSql = CanonicizeDdl(actual, parent);

        if (expectedSql != actualSql)
        {
            throw new Exception(
                $"Index did not match, expected{Environment.NewLine}{expectedSql}{Environment.NewLine}but got:{Environment.NewLine}{actualSql}");
        }
    }

    /// <summary>
    ///     Method to normalize the index definition to use for checking match/equivalence
    /// </summary>
    /// <param name="index"></param>
    /// <param name="parent"></param>
    /// <returns></returns>
    public static string CanonicizeDdl(IndexDefinition index, Table parent)
    {
        return index.ToDDL(parent)
                .Replace("\"\"", "\"")
                .Replace("!=", "<>")
                .Replace("  ", " ")
                .Replace("(", "")
                .Replace(")", "")
                .Replace("IS NOT NULL", "is not null")
                .Replace("INDEX CONCURRENTLY", "INDEX")
                .Replace("::text", "")
                .Replace(" ->> ", "->>")
                .Replace(" -> ", "->").TrimEnd(new[] { ';' })
                .ToLowerInvariant()
            ;
    }
}
