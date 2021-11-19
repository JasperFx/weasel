using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Baseline;

namespace Weasel.Postgresql.Tables
{
    public class IndexDefinition : INamed
    {
        private const string JsonbPathOps = "jsonb_path_ops";
        private static readonly string[] _reserved_words = new string[] {"trim", "lower", "upper"};

        private string? _indexName;
        private string? _customIndexMethod;

        public IndexDefinition(string indexName)
        {
            _indexName = indexName;
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
            set
            {
                _indexName = value;
            }
        }

        protected virtual string deriveIndexName()
        {
            throw new NotSupportedException();
        }

        public IndexMethod Method { get; set; } = IndexMethod.btree;

        public string? CustomMethod
        {
            get => Method == IndexMethod.custom ? _customIndexMethod ?? Method.ToString() : null;
            set
            {
                Method = IndexMethod.custom;
                _customIndexMethod = value;
            }
        }

        public SortOrder SortOrder { get; set; } = SortOrder.Asc;

        public bool IsUnique { get; set; }

        public bool IsConcurrent { get; set; }

        public virtual string[]? Columns { get; set; }

        public virtual string[]? IncludeColumns { get; set; }

        /// <summary>
        /// Pattern for surrounding the columns. Use a `?` character
        /// for the location of the columns, like "? jsonb_path_ops"
        /// </summary>
        public string? Mask { get; set; }

        /// <summary>
        /// Set the Index expression against the supplied columns
        /// </summary>
        /// <param name="columns"></param>
        /// <returns></returns>
        public IndexDefinition AgainstColumns(params string[] columns)
        {
            Columns = columns;
            return this;
        }

        /// <summary>
        /// The tablespace in which to create the index. If not specified, default_tablespace is consulted,
        /// </summary>
        public string? TableSpace { get; set; }

        /// <summary>
        /// The constraint expression for a partial index.
        /// </summary>
        public string? Predicate { get; set; }


        public string ToDDL(Table parent)
        {
            var builder = new StringBuilder();

            builder.Append("CREATE ");

            if (IsUnique) builder.Append("UNIQUE ");
            builder.Append("INDEX ");

            if (IsConcurrent) builder.Append("CONCURRENTLY ");

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

            if (IncludeColumns != null &&  IncludeColumns.Any())
            {
                builder.Append(" INCLUDE (");
                builder.Append(IncludeColumns.Join(", "));
                builder.Append(')');
            }

            builder.Append(";");


            return builder.ToString();
        }

        public static string CanonicizeCast(string column)
        {
            if (!column.Contains("::")) return column;

            var index = column.IndexOf("::");
            var type = column.Substring(index + 2);
            var expression = column.Substring(0, index).Trim().TrimStart('(').TrimEnd(')').Replace("  ", " ");

            return $"CAST({expression} as {type})";

        }

        /// <summary>
        /// Set a non-default fill factor on this index
        /// </summary>
        public int? FillFactor
        {
            get => StorageParameters["fillfactor"] as int?;
            set => StorageParameters["fillfactor"] = value;
        }

        public OrderedDictionary StorageParameters { get; set; } = new();

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

            if (Method == IndexMethod.btree && SortOrder != SortOrder.Asc)
            {
                expression += " DESC";
            }

            return $"({expression})";
        }

        /// <summary>
        /// Makes this index use the Gin method with the jsonb_path_ops operator
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

            bool isUnique = false;
            string expression = "";
            bool isFullTextIndex = false;

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
                        index = new IndexDefinition(name){Mask = String.Empty, IsUnique = isUnique};
                        break;

                    case "ON":
                        // Skip the table name
                        tokens.Dequeue();
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
                        expression = removeSortOrderFromExpression(expression, out var order);

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

                        index.SortOrder = order;

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

                    default:
                        throw new NotImplementedException("NOT YET DEALING WITH " + current);
                }


            }

            if (isFullTextIndex)
            {
                index.Columns = new string[] { expression };
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

            bool inQuotes = false;

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

            return CanonicizeCast(expression);
        }

        private static string removeSortOrderFromExpression(string expression, out SortOrder order)
        {
            if (expression.EndsWith("DESC)"))
            {
                order = SortOrder.Desc;
                return expression.Substring(0, expression.Length - 6) + ")";
            }
            else if (expression.EndsWith("ASC)"))
            {
                order = SortOrder.Asc;
                return  expression.Substring(0, expression.Length - 5) + ")";
            }

            order = SortOrder.Asc;
            return expression.Trim();
        }

        public bool Matches(IndexDefinition actual, Table parent)
        {
            var expectedExpression = correctedExpression();


            if (actual.Mask == expectedExpression)
            {
                actual.Mask = removeSortOrderFromExpression(expectedExpression, out var order);
            }

            var expectedSql = CanonicizeDdl(this, parent);

            var actualSql = CanonicizeDdl(actual, parent);

            return expectedSql == actualSql;
        }

        public void AssertMatches(IndexDefinition actual, Table parent)
        {
            var expectedExpression = correctedExpression();


            if (actual.Mask == expectedExpression)
            {
                actual.Mask = removeSortOrderFromExpression(expectedExpression, out var order);
            }

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
                    .Replace("IS NOT NULL", "is not null")
                    .Replace("INDEX CONCURRENTLY", "INDEX")
                    .Replace("::text", "")
                    .Replace(" ->> ", "->>")
                    .Replace(" -> ", "->").TrimEnd(new[] {';'})
                    .ToLowerInvariant()
                ;
        }
    }
}
