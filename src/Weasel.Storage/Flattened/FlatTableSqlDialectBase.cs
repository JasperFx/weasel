using System.Globalization;
using Weasel.Core;

namespace Weasel.Storage.Flattened;

/// <summary>
///     Base class for a flat-table dialect, carrying the quoting, table-naming, parameter-naming and
///     literal rules that are the same shape on every provider even where the tokens differ.
/// </summary>
/// <remarks>
///     A dialect whose upsert is a single inline statement — a <c>MERGE</c> or an
///     <c>INSERT … ON CONFLICT DO UPDATE</c> — should derive from
///     <see cref="InlineFlatTableSqlDialect" /> instead, which additionally composes the clauses.
///     This class is what is left for a dialect whose upsert is not one statement at all, such as one
///     that generates an upsert function per event type and calls it.
/// </remarks>
public abstract class FlatTableSqlDialectBase: IFlatTableSqlDialect
{
    private readonly IDdlSyntaxStrategy? _syntax;

    /// <param name="syntax">
    ///     The provider's DDL syntax strategy, when it has one — its
    ///     <see cref="IDdlSyntaxStrategy.QuoteIdentifier" /> then becomes this dialect's quoting rule,
    ///     so the two cannot drift. Pass <see langword="null" /> and override
    ///     <see cref="QuoteIdentifier" /> for a provider that does not ship one.
    /// </param>
    protected FlatTableSqlDialectBase(IDdlSyntaxStrategy? syntax = null)
    {
        _syntax = syntax;
    }

    /// <inheritdoc />
    public virtual string QuoteIdentifier(string name)
        => _syntax?.QuoteIdentifier(name)
           ?? throw new InvalidOperationException(
               $"{GetType().FullName} was constructed without an {nameof(IDdlSyntaxStrategy)}, so it has to "
               + $"override {nameof(QuoteIdentifier)} itself.");

    /// <inheritdoc />
    /// <remarks>Schema-qualified and quoted; override for a provider without schemas.</remarks>
    public virtual string TableReference(DbObjectName table)
        => $"{QuoteIdentifier(table.Schema)}.{QuoteIdentifier(table.Name)}";

    /// <inheritdoc />
    public virtual string ParameterName(int index) => "@p" + index.ToString(CultureInfo.InvariantCulture);

    /// <inheritdoc />
    /// <remarks>
    ///     Doubling the single quote is the rule on every provider Weasel supports. Override where a
    ///     provider needs more — a backslash-escaping mode, say.
    /// </remarks>
    public virtual string StringLiteral(string value)
        => string.Concat("'", value.Replace("'", "''"), "'");

    /// <inheritdoc />
    public abstract string ExistingRowReference(DbObjectName table, string columnName);

    /// <inheritdoc />
    public abstract FlatTableUpsert BuildUpsert(FlatTableUpsertRequest request);

    /// <inheritdoc />
    public virtual string BuildDelete(DbObjectName table, string primaryKeyColumn)
        => $"delete from {TableReference(table)} where {QuoteIdentifier(primaryKeyColumn)} = {ParameterName(0)};";
}

/// <summary>
///     Base class for a flat-table dialect whose upsert is a single inline statement.
/// </summary>
/// <remarks>
///     The parameter numbering, the walk over the column maps and the insert / update clause
///     composition are identical between a <c>MERGE</c> and an <c>INSERT … ON CONFLICT DO UPDATE</c>,
///     so a concrete dialect is left with <see cref="FlatTableSqlDialectBase.ExistingRowReference" />
///     and <see cref="WriteUpsert" /> — plus whatever quoting it does not get for free from an
///     <see cref="IDdlSyntaxStrategy" />.
/// </remarks>
public abstract class InlineFlatTableSqlDialect: FlatTableSqlDialectBase
{
    protected InlineFlatTableSqlDialect(IDdlSyntaxStrategy? syntax = null): base(syntax)
    {
    }

    /// <inheritdoc />
    public override FlatTableUpsert BuildUpsert(FlatTableUpsertRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        // An event type mapped to no columns renders an update branch with nothing on the left of it,
        // which is a syntax error rather than a no-op. Say so here, where the projection can still be
        // named, instead of at the first event.
        if (request.Columns.Count == 0)
        {
            throw new InvalidOperationException(
                $"No columns are mapped for the flat table '{request.Identifier}'. A Project<T>(...) "
                + "registration has to map at least one column.");
        }

        var context = new FlatTableColumnContext(this, request.Identifier);

        var quotedKey = QuoteIdentifier(request.PrimaryKeyColumn);
        var keyParameter = ParameterName(0);

        var insertColumns = new List<string>(request.Columns.Count + 1) { quotedKey };
        var insertValues = new List<string>(request.Columns.Count + 1) { keyParameter };
        var updateAssignments = new List<string>(request.Columns.Count);

        // Parameter 0 is the key; the maps that read from the event take 1..N in declaration order,
        // which is the order the caller's parameter setters are built in.
        var nextParameter = 1;

        foreach (var map in request.Columns)
        {
            var parameterName = string.Empty;

            if (map.RequiresInput)
            {
                parameterName = ParameterName(nextParameter++);
            }

            insertColumns.Add(QuoteIdentifier(map.ColumnName));
            insertValues.Add(map.InsertExpression(context, parameterName));
            updateAssignments.Add(map.UpdateExpression(context, parameterName));
        }

        var clauses = new FlatTableUpsertClauses(quotedKey, keyParameter, insertColumns, insertValues,
            updateAssignments);

        return WriteUpsert(request, clauses);
    }

    /// <summary>
    ///     String the composed clauses together into this dialect's upsert statement.
    /// </summary>
    protected abstract FlatTableUpsert WriteUpsert(FlatTableUpsertRequest request, FlatTableUpsertClauses clauses);
}
