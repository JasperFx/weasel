using Weasel.Core;

namespace Weasel.Storage.Flattened;

/// <summary>
///     How one column of a flat table participates in the upsert for a single event type.
/// </summary>
/// <remarks>
///     <para>
///         Both branches are described here, and they are deliberately not derived from each other:
///         an <c>Increment</c> <em>inserts</em> a starting value but <em>updates</em> by adding to
///         what is already there, and that asymmetry is the entire point of the model.
///     </para>
///     <para>
///         Nothing here renders a dialect's SQL directly — every fragment goes through the
///         <see cref="FlatTableColumnContext" /> it is handed, which is what lets one set of maps
///         serve a <c>MERGE</c>, an <c>INSERT … ON CONFLICT</c> and a generated upsert function.
///     </para>
/// </remarks>
public interface IColumnMap
{
    /// <summary>The column this mapping writes.</summary>
    string ColumnName { get; }

    /// <summary>
    ///     The CLR type the column holds, used to declare the column when the table does not already
    ///     have one by that name.
    /// </summary>
    Type ColumnType { get; }

    /// <summary>
    ///     True when the column's value comes from the event and therefore needs a bound parameter.
    ///     False for <c>Increment(name)</c>, <c>Decrement(name)</c> and <c>SetValue</c>, which are
    ///     entirely determined at configuration time.
    /// </summary>
    bool RequiresInput { get; }

    /// <summary>
    ///     The assignment for the update branch — the <c>SET</c> clause of a <c>MERGE</c>'s
    ///     <c>WHEN MATCHED</c>, or of an <c>ON CONFLICT … DO UPDATE</c>.
    /// </summary>
    /// <param name="context">The dialect and table this fragment is being rendered against.</param>
    /// <param name="parameterName">
    ///     The placeholder bound to this column's value, or an empty string when
    ///     <see cref="RequiresInput" /> is false.
    /// </param>
    string UpdateExpression(in FlatTableColumnContext context, string parameterName);

    /// <summary>The value expression for the insert branch.</summary>
    /// <param name="context">The dialect and table this fragment is being rendered against.</param>
    /// <param name="parameterName">
    ///     The placeholder bound to this column's value, or an empty string when
    ///     <see cref="RequiresInput" /> is false.
    /// </param>
    string InsertExpression(in FlatTableColumnContext context, string parameterName);
}

/// <summary>
///     What a column map is rendered against: the dialect, and the table whose row is being written.
/// </summary>
/// <remarks>
///     The table travels with the dialect because the pre-update row reference needs it —
///     PostgreSQL's <c>ON CONFLICT DO UPDATE</c> names the target row by the table's own name, where
///     a <c>MERGE</c> names it by the alias the statement gave it.
/// </remarks>
public readonly struct FlatTableColumnContext
{
    public FlatTableColumnContext(IFlatTableSqlDialect dialect, DbObjectName table)
    {
        Dialect = dialect;
        Table = table;
    }

    /// <summary>The dialect supplying the SQL fragments.</summary>
    public IFlatTableSqlDialect Dialect { get; }

    /// <summary>The table being written.</summary>
    public DbObjectName Table { get; }

    /// <summary>The column, quoted for an assignment target or an insert column list.</summary>
    public string Quote(string columnName) => Dialect.QuoteIdentifier(columnName);

    /// <summary>The column's value <em>before</em> this statement runs.</summary>
    public string Existing(string columnName) => Dialect.ExistingRowReference(Table, columnName);

    /// <summary>A configured string baked into the statement as a literal.</summary>
    public string Literal(string value) => Dialect.StringLiteral(value);
}
