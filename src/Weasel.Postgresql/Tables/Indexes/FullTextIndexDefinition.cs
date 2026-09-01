using JasperFx.Core;

namespace Weasel.Postgresql.Tables.Indexes;

public class FullTextIndexDefinition: IndexDefinition
{
    public const string DefaultRegConfig = "english";
    public const string DataDocumentConfig = "data";

    private readonly PostgresqlObjectName table;
    private readonly string? indexName;
    private readonly string? indexPrefix;

    private string regConfig;
    private string? tsVectorExpression;

    public FullTextIndexDefinition(
        PostgresqlObjectName tableName,
        string documentConfig,
        string? regConfig = null,
        string? indexName = null,
        string? indexPrefix = null)
    {
        table = tableName;
        this.regConfig = regConfig ?? DefaultRegConfig;
        DocumentConfig = documentConfig;
        this.indexName = indexName;
        this.indexPrefix = indexPrefix;

        Method = IndexMethod.gin;
    }

    /// <summary>
    ///     An index over an expression that is <em>already</em> a <c>tsvector</c>, rather than over
    ///     text for this definition to convert.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The case this exists for is per-member weighting. PostgreSQL's <c>setweight</c> labels
    ///         a vector, so weighting works by converting each member separately and concatenating
    ///         the <em>vectors</em> — not by concatenating the text and converting once:
    ///     </para>
    ///     <code>
    ///     setweight(to_tsvector('english', coalesce(data ->> 'Title', '')), 'A') ||
    ///     setweight(to_tsvector('english', coalesce(data ->> 'Body', '')), 'B')
    ///     </code>
    ///     <para>
    ///         That expression is a <c>tsvector</c> at the top level. Handing it to
    ///         <see cref="DocumentConfig" /> would wrap it in another <c>to_tsvector</c>, which is a
    ///         type error rather than a weighted index — so weighting was unreachable through this
    ///         type (weasel#541, for JasperFx/marten#5298).
    ///     </para>
    ///     <para>
    ///         When this is set it wins outright: <see cref="DocumentConfig" /> and
    ///         <see cref="RegConfig" /> take no part in the DDL, because a pre-built vector already
    ///         carries its own text search configuration inside the expression. Leave it unset — the
    ///         default — and this definition behaves exactly as it always has, down to the byte. That
    ///         matters: a changed index expression makes Weasel drop and recreate the index, which on
    ///         a large table is an outage rather than a migration.
    ///     </para>
    ///     <para>
    ///         Whitespace-only is treated as unset. Read <see cref="IndexedTsVector" /> rather than
    ///         this property to find out what is actually indexed.
    ///     </para>
    /// </remarks>
    public string? TsVectorExpression
    {
        get => tsVectorExpression;
        set => tsVectorExpression = string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }

    /// <summary>
    ///     The <c>tsvector</c> expression this index is actually built over, whichever way it was
    ///     configured.
    /// </summary>
    /// <remarks>
    ///     The DDL is generated from this and nothing else, so a consumer building the query-side
    ///     filter should read it from here too. Marten's <c>FullTextWhereFragment</c> reads the
    ///     expression back off the index definition for exactly this reason, and ranking makes the
    ///     coupling stricter still: a <c>ts_rank</c> computed over a different vector than the one
    ///     <c>@@</c> filtered on is silently wrong rather than merely slow.
    /// </remarks>
    public string IndexedTsVector =>
        tsVectorExpression == null
            ? $"to_tsvector('{regConfig}',{DocumentConfig.Trim()})"
            : parenthesize(tsVectorExpression);

    /// <summary>
    ///     Wrap the expression unless it already carries its own outermost pair.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Not cosmetic. PostgreSQL lets an index expression go bare only when it is a function
    ///         call or a simple column reference; anything built with an operator needs parentheses
    ///         of its own. The weighting case is built with <c>||</c>, and
    ///         <c>IndexDefinition.correctedExpression</c> supplies exactly one pair — which the
    ///         <c>USING gin (…)</c> argument list consumes — so an unwrapped
    ///         <c>setweight(…) || setweight(…)</c> reaches the server as a syntax error.
    ///     </para>
    ///     <para>
    ///         Delta detection is unaffected either way: <see cref="CanonicizeDdl(string, string)" />
    ///         strips every parenthesis before comparing.
    ///     </para>
    /// </remarks>
    private static string parenthesize(string expression)
    {
        if (!expression.StartsWith('(') || !expression.EndsWith(')'))
        {
            return $"({expression})";
        }

        // The leading '(' must be the one the trailing ')' closes, or the expression is something
        // like "(a) || (b)" -- already balanced, but not wrapped.
        var depth = 0;
        for (var i = 0; i < expression.Length; i++)
        {
            if (expression[i] == '(')
            {
                depth++;
            }
            else if (expression[i] == ')')
            {
                depth--;

                if (depth == 0)
                {
                    return i == expression.Length - 1 ? expression : $"({expression})";
                }
            }
        }

        return $"({expression})";
    }

    /// <summary>
    ///     An index over an expression that is already a <c>tsvector</c>. See
    ///     <see cref="TsVectorExpression" /> for why this is not just a different
    ///     <see cref="DocumentConfig" />.
    /// </summary>
    /// <remarks>
    ///     A factory rather than a constructor overload: the existing constructor already takes a
    ///     <c>string</c> in that position followed by three optional <c>string?</c>s, so an overload
    ///     would be ambiguous at every call site that omits the optional arguments — and would
    ///     silently pick the wrong one where it was not.
    /// </remarks>
    public static FullTextIndexDefinition ForTsVector(
        PostgresqlObjectName tableName,
        string tsVectorExpression,
        string? indexName = null,
        string? indexPrefix = null)
    {
        if (string.IsNullOrWhiteSpace(tsVectorExpression))
        {
            throw new ArgumentOutOfRangeException(
                nameof(tsVectorExpression),
                "A tsvector expression is required. To index text for Weasel to convert, use the constructor and DocumentConfig instead.");
        }

        return new FullTextIndexDefinition(tableName, DataDocumentConfig, indexName: indexName,
            indexPrefix: indexPrefix)
        {
            TsVectorExpression = tsVectorExpression
        };
    }

    public string? RegConfig
    {
        get => regConfig;
        set => regConfig = value ?? DefaultRegConfig;
    }

    public string DocumentConfig { get; set; }

    [Obsolete("Use DocumentConfig instead")]
    public string? DataConfig
    {
        get => DocumentConfig;
        set => DocumentConfig = value ?? DataDocumentConfig;
    }

    public override string[] Columns
    {
        get => new[] { IndexedTsVector };
        set
        {
            // nothing
        }
    }

    protected override string deriveIndexName()
    {
        var lowerValue = indexName?.ToLowerInvariant();

        if (lowerValue?.IsNotEmpty() == true)
        {
            return indexPrefix?.IsNotEmpty() == true && !lowerValue.StartsWith(indexPrefix)
                ? indexPrefix + lowerValue
                : lowerValue;
        }

        if (regConfig != DefaultRegConfig)
        {
            return $"{table.Name}_{regConfig}_idx_fts";
        }

        return $"{table.Name}_idx_fts";
    }
}
