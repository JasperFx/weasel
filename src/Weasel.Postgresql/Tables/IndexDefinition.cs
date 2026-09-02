using System.Collections;
using System.Collections.Specialized;
using System.Text;
using System.Text.RegularExpressions;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Postgresql.Tables;

public class IndexDefinition: ITableIndex
{
    public const string IndexCreationBeginComment = "--WEASEL_INDEX_CREATION_BEGIN";
    public const string IndexCreationEndComment = "--WEASEL_INDEX_CREATION_END";
    private const string JsonbPathOps = "jsonb_path_ops";
    private const string Ascending = "ASC";
    private const string Descending = "DESC";
    private const string NullsFirst = "NULLS FIRST";
    private const string NullsLast = "NULLS LAST";
    private const string AscendingNullsFirst = "ASC NULLS FIRST";
    private const string AscendingNullsLast = "ASC NULLS LAST";
    private const string DescendingNullsFirst = "DESC NULLS FIRST";
    private const string DescendingNullsLast = "DESC NULLS LAST";

    private string? _customIndexMethod;
    private string? _indexName;
    private bool _isUnique;

    public IndexDefinition(string indexName)
    {
        _indexName = SchemaUtils.Unquote(indexName);
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

    string? ITableIndex.Method
    {
        get => Method == IndexMethod.custom ? CustomMethod : Method.ToString();
        set
        {
            if (value == null)
            {
                Method = IndexMethod.btree;
            }
            else if (Enum.TryParse<IndexMethod>(value, ignoreCase: true, out var known) && known != IndexMethod.custom)
            {
                Method = known;
            }
            else
            {
                CustomMethod = value;
            }
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
    public bool IsUnique
    {
        get => _isUnique;
        set
        {
            _isUnique = value;

            if (_isUnique == false)
                NullsNotDistinct = false;
        }
    }

    /// <summary>
    ///     Should unique index consider nulls non distinct.
    /// </summary>
    /// <remarks>
    ///     Requires PostgreSQL version 15
    /// </remarks>
    public bool NullsNotDistinct { get; set; }

    /// <summary>
    ///     Option to build index without taking any locks that prevent concurrent inserts, updates or deletes in table
    /// </summary>
    /// <remarks>
    ///     From Postgresql 14, you cannot create indexes concurrently within a transaction.
    ///     Npgsql applies batches of statements automatically as implicit transactions.
    ///     Thus, concurrent indexes creation or update will only work if you apply them separately.
    ///     <br/><br/>
    ///     Read more in:<br/>
    ///     - https://github.com/npgsql/npgsql/issues/462#issuecomment-925054226<br/>
    ///     - https://www.migops.com/blog/important-postgresql-14-update-to-avoid-silent-corruption-of-indexes/
    /// </remarks>
    public bool IsConcurrent { get; set; }

    /// <summary>
    ///     False when the database holds this index but has marked it invalid. Only ever set while
    ///     reading an existing table; an index in a Weasel model is valid by construction.
    /// </summary>
    /// <remarks>
    ///     PostgreSQL leaves an index invalid when <c>CREATE INDEX CONCURRENTLY</c> fails partway, and
    ///     weasel#494 creates one deliberately -- a partitioned parent index is invalid until the last
    ///     partition's index is attached. Either way the planner ignores it, so an invalid index is a
    ///     schema that does not do what it says.
    /// </remarks>
    internal bool IsValidInDatabase { get; set; } = true;

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
        set => _indexName = SchemaUtils.Unquote(value);
    }

    public string QuotedName => SchemaUtils.QuoteName(Name);

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
    /// <remarks>
    /// Ordering the statement segments matters. Ref the Postgres create index docs here: https://www.postgresql.org/docs/current/sql-createindex.html
    /// CREATE [ UNIQUE ] INDEX [ CONCURRENTLY ] [ [ IF NOT EXISTS ] name ] ON [ ONLY ] table_name [ USING method ]
    ///     ( { column_name | ( expression ) } [ COLLATE collation ] [ opclass [ ( opclass_parameter = value [, ... ] ) ] ] [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] )
    ///     [ INCLUDE ( column_name [, ...] ) ]
    ///     [ NULLS [ NOT ] DISTINCT ]
    ///     [ WITH ( storage_parameter [= value] [, ... ] ) ]
    ///     [ TABLESPACE tablespace_name ]
    ///     [ WHERE predicate ]
    /// </remarks>
    /// <param name="parent"></param>
    /// <returns>Sql statement to create the index</returns>
    public string ToDDL(Table parent)
    {
        var builder = new StringBuilder();

        if (IsConcurrent)
        {
            builder.AppendLine(IndexCreationBeginComment);
        }

        builder.Append(createStatement(parent, QuotedName, parent.Identifier.ToString(), IsConcurrent, onlyParent: false));

        if (IsConcurrent)
        {
            builder.AppendLine();
            builder.Append(IndexCreationEndComment);
        }

        return builder.ToString();
    }

    /// <summary>
    ///     One <c>CREATE INDEX</c> statement, with the name, the target table, the concurrency and the
    ///     <c>ONLY</c> qualifier supplied rather than taken from this definition.
    /// </summary>
    /// <remarks>
    ///     The last three vary only for a concurrent index on a partitioned table, which is built as
    ///     three statements per partition rather than one — see <see cref="ToCreateSql" />.
    /// </remarks>
    private string createStatement(Table parent, string indexName, string table, bool concurrently, bool onlyParent)
    {
        var builder = new StringBuilder();

        builder.Append("CREATE ");

        if (IsUnique)
        {
            builder.Append("UNIQUE ");
        }

        builder.Append("INDEX ");

        if (concurrently)
        {
            builder.Append("CONCURRENTLY ");
        }

        builder.Append(indexName);

        builder.Append(" ON ");

        if (onlyParent)
        {
            builder.Append("ONLY ");
        }

        builder.Append(table);
        builder.Append(" USING ");
        builder.Append(Method == IndexMethod.custom ? CustomMethod : Method);
        builder.Append(" ");
        builder.Append(correctedExpression(parent));

        if (IncludeColumns != null && IncludeColumns.Any())
        {
            builder.Append(" INCLUDE (");
            builder.Append(IncludeColumns.Select(x => SchemaUtils.QuoteName(x)).Join(", "));
            builder.Append(')');
        }

        if (NullsNotDistinct)
        {
            if (!IsUnique)
            {
                throw new NotSupportedException("Cannot use NullsNotDistinct with non unique index");
            }

            builder.Append(" NULLS NOT DISTINCT ");
        }

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


        builder.Append(";");

        return builder.ToString();
    }

    /// <summary>
    ///     The DDL the migration path should execute to build this index. Usually one statement, and
    ///     identical to <see cref="ToDDL" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         A concurrent index on a partitioned table is the exception. PostgreSQL refuses
    ///         <c>CREATE INDEX CONCURRENTLY</c> on a partitioned parent outright — "cannot create index
    ///         on partitioned table ... concurrently" — so <see cref="IsConcurrent" /> could not be
    ///         honoured there at all, and adding an index to a partitioned table meant an
    ///         <c>ACCESS EXCLUSIVE</c> lock for the whole build: a write outage rather than a
    ///         migration (weasel#494).
    ///     </para>
    ///     <para>
    ///         The supported sequence is three steps. <c>CREATE INDEX ON ONLY parent</c> registers the
    ///         parent index as metadata and leaves it <em>invalid</em>; each partition then gets its own
    ///         index built concurrently; and each is attached with
    ///         <c>ALTER INDEX ... ATTACH PARTITION</c>. The parent flips to valid by itself once the
    ///         last child is attached, which is why a half-finished run is visible rather than silent.
    ///     </para>
    ///     <para>
    ///         Kept separate from <see cref="ToDDL" /> deliberately: that is also the canonical form the
    ///         delta compares against <c>pg_get_indexdef</c>, which returns a single statement. Folding
    ///         this sequence into it would make every such index report drift on every run.
    ///     </para>
    /// </remarks>
    public string ToCreateSql(Table parent)
        => IsConcurrent && parent.Partitioning != null
            ? toPartitionedConcurrentDDL(parent)
            : ToDDL(parent);

    private string toPartitionedConcurrentDDL(Table parent)
    {
        var builder = new StringBuilder();
        var schema = parent.Identifier.Schema;

        // Metadata only, and deliberately not concurrent: nothing is scanned, so nothing is blocked.
        builder.AppendLine(createStatement(parent, QuotedName, parent.Identifier.ToString(),
            concurrently: false, onlyParent: true));

        foreach (var partition in parent.Partitioning!.PartitionTableNames(parent))
        {
            var childName = ChildIndexName(parent, partition);
            var quotedChild = SchemaUtils.QuoteName(childName);

            // The markers put this statement in a command of its own. CREATE INDEX CONCURRENTLY
            // cannot run inside a transaction block, and PostgresqlMigrator.executeDelta splits the
            // script on exactly this boundary.
            builder.AppendLine(IndexCreationBeginComment);
            builder.AppendLine(createStatement(parent, quotedChild,
                $"{schema}.{SchemaUtils.QuoteName(partition)}", concurrently: true, onlyParent: false));
            builder.AppendLine(IndexCreationEndComment);

            builder.AppendLine($"ALTER INDEX {schema}.{QuotedName} ATTACH PARTITION {schema}.{quotedChild};");
        }

        return builder.ToString();
    }

    /// <summary>
    ///     The name of the index built on one partition, before it is attached to this one.
    /// </summary>
    /// <remarks>
    ///     Deterministic, so a run that died partway through names the same child index on the next
    ///     attempt rather than building a second one beside it. Deliberately not truncated when it runs
    ///     long: <c>AssertValidIdentifier</c> refuses an over-length identifier rather than silently
    ///     renaming the object, which is the rule weasel#468 settled.
    /// </remarks>
    internal string ChildIndexName(Table parent, string partitionTableName)
    {
        // Partition tables are named "{parent}_{suffix}", so trimming the parent off keeps the child
        // index at "{index}_{suffix}" rather than repeating the table name inside it. The fallback
        // covers a partition name that does not follow the convention.
        var prefix = parent.Identifier.Name.ToLowerInvariant() + "_";

        var suffix = partitionTableName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            ? partitionTableName.Substring(prefix.Length)
            : partitionTableName;

        return $"{Name}_{suffix}";
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

    private string correctedExpression(Table? parent = null)
    {
        if (Columns == null || !Columns.Any())
        {
            throw new InvalidOperationException("IndexDefinition requires at least one field");
        }

        var expression = Columns.Select(x =>
        {
            // A name the parent actually declares as a column is a column, whatever it looks
            // like. Without this check the heuristic below reads "Order Date" as an expression
            // and emits it bare, producing an index over a column that does not exist -- the
            // bug weasel#458 was opened for. Columns may legally carry spaces now that nothing
            // rewrites them into underscores.
            if (parent?.HasColumn(x) == true)
            {
                return SchemaUtils.QuoteName(x);
            }

            if (x.StartsWith('(') || x.Contains(' ') || x.Contains('-') || x.Contains('\''))
                return x;

            // Case-quoting is only correct when the parent table itself preserved
            // identifier case at creation (EF Core-mapped tables). On a case-folded
            // table the physical column is lowercase, and quoting the declared
            // casing would reference a column that does not exist (weasel#382)
            if (parent is { PreserveIdentifierCase: true })
            {
                return SchemaUtils.QuoteName(x);
            }

            return SchemaUtils.IsReservedKeyword(x) ? $"\"{x}\"" : x;
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

        // PostgreSQL requires unique indexes on partitioned tables to include all partitioning columns
        if (IsUnique && parent?.Partitioning != null)
        {
            var existingColumns = new HashSet<string>(Columns, StringComparer.OrdinalIgnoreCase);
            foreach (var partitionColumn in parent.Partitioning.Columns)
            {
                if (!existingColumns.Contains(partitionColumn))
                {
                    expression += $", {partitionColumn}";
                }
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
        var trimmedDefinition = definition
            .Replace(IndexCreationBeginComment, "")
            .Replace(IndexCreationEndComment, "")
            .Trim()
            .TrimEnd(';');
        var tokens = new Queue<string>(StringTokenizer.Tokenize(trimmedDefinition));

        IndexDefinition index = null!;

        var isUnique = false;
        var expression = "";
        var isFullTextIndex = false;

        while (tokens.Any())
        {
            var current = tokens.Dequeue();
            switch (current.ToUpperInvariant())
            {
                case "CREATE":
                case "CONCURRENTLY":
                case IndexCreationBeginComment:
                case IndexCreationEndComment:
                    continue;

                case "INDEX":
                    var name = tokens.Dequeue().Trim('"');
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
                            .Replace("::regconfig", "");
                        // Trim redundant spaces, but not those that come in the text.
                        // Index with multiple column can look like:
                        // to_tsvector('english',((data ->> 'FirstName') || ' ' || (data ->> 'LastName')))
                        expression = Regex.Replace(expression, @"('[^'\\]*(?:\\.[^'\\]*)*')|\s+", "$1");
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

                case "NULLS":
                    var nextToken = tokens.Dequeue();
                    if (nextToken.Equals("DISTINCT", StringComparison.InvariantCultureIgnoreCase))
                    {
                        // Not interested in NULLS DISTINCT
                        break;
                    }

                    if (!nextToken.Contains("NOT", StringComparison.InvariantCultureIgnoreCase))
                    {
                        throw new NotImplementedException($"Unsupported index definition. Received 'NULLS {nextToken}'");
                    }

                    if (!tokens.Peek().Contains("DISTINCT", StringComparison.InvariantCultureIgnoreCase))
                    {
                        throw new NotImplementedException($"Unsupported index definition. Received 'NULLS NOT {tokens.Peek()}'");
                    }

                    tokens.Dequeue();
                    index.NullsNotDistinct = true;
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
            if (expression.StartsWith('(') && expression.EndsWith(')'))
            {
                expression = expression.Substring(1, expression.Length - 2);
            }

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
        while (expression.StartsWith('(') && expression.EndsWith(')'))
        {
            expression = expression.Substring(1, expression.Length - 2);
        }

        // If Postgres keyword are used as a column name then those are enclosed in double quotes
        expression = expression.Trim('"');

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
        if (!actual.IsValidInDatabase)
        {
            // The index exists but PostgreSQL will not use it -- the planner ignores an invalid
            // index entirely. Reporting a match would leave it unusable forever while every check
            // said the schema was correct, so this is drift even though the definitions agree. It
            // routes to ItemDelta.Different, which drops before it creates; a bare CREATE INDEX
            // would fail with 42P07 against the index still sitting there (weasel#503).
            return false;
        }

        var expectedExpression = correctedExpression(parent);

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
        var expectedExpression = correctedExpression(parent);


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
        var canonicizedStr = index.ToDDL(parent);
        return CanonicizeDdl(canonicizedStr, parent.Identifier.Schema);
    }

    public static string CanonicizeDdl(string sql, string schema)
    {
        // This was caused by https://github.com/JasperFx/marten/issues/2983
        sql = sql.Replace("if not exists", "", StringComparison.OrdinalIgnoreCase);
        sql = sql.Replace("desc nulls first", "desc", StringComparison.OrdinalIgnoreCase);
        sql = sql.Replace("asc nulls first", "asc", StringComparison.OrdinalIgnoreCase);
        sql = sql.Replace("TABLESPACE pg_default", "", StringComparison.OrdinalIgnoreCase);
        sql = sql.Replace("public.", "", StringComparison.OrdinalIgnoreCase);
        sql = sql.Replace("public.", "");
        sql = sql.Replace($"{schema}.", "");

        // replace multiple spaces with single space
        sql = Regex.Replace(sql, @"\s+", " ");
        // replace open parenthesis followed by one or more spaces to just open parenthesis
        sql = Regex.Replace(sql, @"\(\s+", "(");
        // replace one or more spaces followed by closed parenthesis to closed parenthesis
        sql = Regex.Replace(sql, @"\s+\)", ")");
        return sql.Replace("\"\"", "\"")
            .Replace("!=", "<>")
            .Replace("(", "")
            .Replace(")", "")
            .Replace(" || ", "||")
            .Replace("IS NOT NULL", "is not null")
            .Replace("INDEX CONCURRENTLY", "INDEX")
            .Replace("::text", "")
            .Replace("::regconfig", "")
            // setweight's second argument is of type "char", and PostgreSQL renders the cast
            // explicitly when it gives an index expression back. Same class of automatic cast as
            // ::text and ::regconfig above: it is in the actual and never in the expected, so
            // without this a weighted full text index reads as changed on every migration and is
            // dropped and recreated every time (weasel#541).
            .Replace("::\"char\"", "")
            .Replace(" ->> ", "->>")
            .Replace(" -> ", "->")
            .Replace(IndexCreationBeginComment, "")
            .Replace(IndexCreationEndComment, "")
            .Replace("as decimal", "as numeric")
            .Replace("character varying", "varchar")
            .Replace(", ", ",")
            .Trim()
            .TrimEnd(new[] { ';' })
            .ToLowerInvariant();
    }
}
