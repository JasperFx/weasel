using System.Text;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.SqlServer.Tables;

/// <summary>
///     A SQL Server 2025 <c>CREATE JSON INDEX</c> over a native <c>json</c> column. One index covers
///     many JSON paths and accelerates <c>JSON_VALUE</c>, <c>JSON_PATH_EXISTS</c> and
///     <c>JSON_CONTAINS</c> predicates without a computed column per path.
/// </summary>
/// <remarks>
///     <para>
///         <b>Modelled as an <see cref="IndexDefinition" /> subclass rather than a parallel
///         collection</b>, so it flows through everything a table already does with its indexes —
///         <see cref="Table.AllNames" />, the <see cref="ItemDelta{T}" /> comparison, the drop/create
///         emission, and the migration script. A separate collection would have to be threaded
///         through each of those by hand, and the one that got missed would be the one that silently
///         disagreed with the database.
///     </para>
///     <para>
///         <b>A JSON index really is in <c>sys.indexes</c></b>, with <c>type_desc = 'JSON'</c> — but
///         its <c>sys.index_columns</c> row carries <c>key_ordinal = 0</c>, which the ordinary column
///         read filters out. Read through the ordinary path it therefore arrives as an index with NO
///         columns, matches nothing declared, and is dropped as an extra. That is why
///         <see cref="Table.FetchExisting" /> excludes <c>type_desc = 'JSON'</c> from the ordinary
///         query and reads these through <c>sys.json_indexes</c> instead.
///     </para>
///     <para>
///         The grammar differs enough from a regular index that almost nothing is inherited: no
///         UNIQUE, no CLUSTERED, no INCLUDE, no WHERE filter, no sort order, and the key is always the
///         single <c>json</c> column with the indexed paths in a <c>FOR</c> clause. Those members are
///         refused rather than ignored — see <see cref="AssertShapeIsSupported" />.
///     </para>
/// </remarks>
public class JsonIndexDefinition: IndexDefinition
{
    private string[] _jsonPaths = [];

    public JsonIndexDefinition(string indexName, string columnName): base(indexName)
    {
        AgainstColumns(columnName);
    }

    /// <summary>
    ///     The JSON paths the index covers, e.g. <c>$.name</c>, <c>$.address.city</c>. Empty means the
    ///     whole document is indexed and the <c>FOR</c> clause is omitted, which is SQL Server's own
    ///     default rather than a Weasel convention.
    /// </summary>
    public string[] JsonPaths
    {
        get => _jsonPaths;
        set => _jsonPaths = value ?? [];
    }

    /// <summary>
    ///     Maps to <c>WITH (OPTIMIZE_FOR_ARRAY_SEARCH = ON)</c>.
    /// </summary>
    public bool OptimizeForArraySearch { get; set; }

    /// <summary>
    ///     The single <c>json</c> column this index is built on.
    /// </summary>
    public string ColumnName => Columns.Single();

    /// <summary>
    ///     Refuse the regular-index options a JSON index has no grammar for, rather than rendering DDL
    ///     SQL Server will reject — or, worse, quietly dropping the option and building a different
    ///     index than the model describes.
    /// </summary>
    internal void AssertShapeIsSupported()
    {
        var unsupported = new List<string>();
        if (IsUnique) unsupported.Add(nameof(IsUnique));
        if (IsClustered) unsupported.Add(nameof(IsClustered));
        if (Predicate.IsNotEmpty()) unsupported.Add(nameof(Predicate));
        if (IncludedColumns is { Length: > 0 }) unsupported.Add(nameof(IncludedColumns));
        if (SortOrder != SortOrder.Asc) unsupported.Add(nameof(SortOrder));
        if (DescendingColumns.Count > 0) unsupported.Add(nameof(DescendingColumns));

        if (unsupported.Count > 0)
        {
            throw new InvalidOperationException(
                $"JSON index '{Name}' sets {unsupported.Join(", ")}, which CREATE JSON INDEX does not support. "
                + "Use a computed-column index for a unique, clustered, filtered, covering or descending index.");
        }

        if (Columns.Length != 1)
        {
            throw new InvalidOperationException(
                $"JSON index '{Name}' must be declared against exactly one json column, but has "
                + $"{Columns.Length}.");
        }
    }

    internal override string ToDDL(Table parent, bool usePerColumnDirection)
    {
        AssertShapeIsSupported();

        var builder = new StringBuilder();
        builder.Append("CREATE JSON INDEX ");
        builder.Append(SchemaUtils.QuoteName(Name));
        builder.Append(" ON ");
        builder.Append(parent.Identifier);
        builder.Append(" (");
        builder.Append(SchemaUtils.QuoteName(ColumnName));
        builder.Append(')');

        if (JsonPaths.Length > 0)
        {
            builder.Append(" FOR (");
            builder.Append(JsonPaths.Select(p => $"'{SchemaUtils.EscapeLiteral(p)}'").Join(", "));
            builder.Append(')');
        }

        var options = new List<string>();
        if (OptimizeForArraySearch) options.Add("OPTIMIZE_FOR_ARRAY_SEARCH = ON");
        if (FillFactor.HasValue) options.Add($"FILLFACTOR = {FillFactor.Value}");

        if (options.Count > 0)
        {
            builder.Append(" WITH (");
            builder.Append(options.Join(", "));
            builder.Append(')');
        }

        return builder.ToString();
    }

    /// <summary>
    ///     <c>CREATE JSON INDEX</c> requires <c>QUOTED_IDENTIFIER ON</c>, and a rendered migration
    ///     script cannot assume the session it will be run in has it. The existence guard also has to
    ///     key on the TABLE rather than the index name: SQL Server permits only one JSON index per
    ///     json column, so a differently-named one already present is a conflict rather than an
    ///     unrelated object, and <c>IF NOT EXISTS (… WHERE name = …)</c> would step straight into
    ///     "a JSON index already exists on this column".
    /// </summary>
    public override void WriteCreateStatement(Table parent, TextWriter writer)
    {
        var table = SchemaUtils.EscapeLiteral(parent.Identifier.QualifiedName);

        writer.WriteLine("SET QUOTED_IDENTIFIER ON;");
        writer.WriteLine(
            $"IF NOT EXISTS (SELECT 1 FROM sys.json_indexes WHERE object_id = OBJECT_ID(N'{table}'))");
        writer.WriteLine($"    {ToDDL(parent)};");
    }
}
