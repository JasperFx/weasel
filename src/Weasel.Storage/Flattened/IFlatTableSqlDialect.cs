using Weasel.Core;

namespace Weasel.Storage.Flattened;

/// <summary>
///     The dialect seam for flat-table projections — the three provider-specific decisions a
///     flat-table upsert needs, and nothing else.
/// </summary>
/// <remarks>
///     <para>
///         The flat-table DSL (<c>Project&lt;T&gt;(map =&gt; map.Map(x =&gt; x.Prop))</c>,
///         <c>Increment</c>, <c>Decrement</c>, <c>SetValue</c>, <c>Delete&lt;T&gt;()</c>) is the same
///         mapping model on every store. What differs is only the SQL it renders to:
///     </para>
///     <list type="number">
///         <item>
///             <description>
///                 <b>Identifier quoting</b> — <c>[c]</c> on SQL Server, <c>"c"</c> on SQLite and
///                 PostgreSQL. See <see cref="QuoteIdentifier" />; a provider that ships an
///                 <see cref="IDdlSyntaxStrategy" /> can simply hand it to
///                 <see cref="FlatTableSqlDialectBase" /> rather than reimplementing the rule.
///             </description>
///         </item>
///         <item>
///             <description>
///                 <b>The upsert form</b> — a <c>MERGE</c>, an <c>INSERT … ON CONFLICT DO UPDATE</c>,
///                 or a generated upsert <em>function</em> the statement then calls. See
///                 <see cref="BuildUpsert" />, whose result carries both the runtime SQL and any
///                 schema objects the form needs created, so the function-based shape fits without
///                 the caller knowing which shape it got.
///             </description>
///         </item>
///         <item>
///             <description>
///                 <b>The "pre-update row" reference</b> — how the update branch names the value a
///                 column held <em>before</em> this statement. <c>target.[c]</c> inside a
///                 <c>MERGE</c>, a bare <c>"c"</c> inside SQLite's <c>DO UPDATE</c> (where
///                 <c>excluded."c"</c> would be the would-be-inserted value instead), and
///                 <c>tbl.c</c> inside PostgreSQL's. See <see cref="ExistingRowReference" />; this is
///                 what makes <c>Increment</c> an increment rather than a self-assignment, so getting
///                 it wrong is silent.
///             </description>
///         </item>
///     </list>
///     <para>
///         This is deliberately <em>not</em> folded onto <see cref="IStorageDialect" />. That
///         interface is about materializing provider <see cref="System.Data.Common.DbCommand" /> /
///         <see cref="System.Data.Common.DbParameter" /> objects for the closed-shape document
///         runtime; everything here is pure SQL-text composition with no ADO.NET types in sight, and
///         a store may want one without the other. The precedent is
///         <see cref="IEventStoreSqlDialect" />, which was given its own seam for the same reason.
///     </para>
/// </remarks>
public interface IFlatTableSqlDialect
{
    /// <summary>
    ///     Quote a column or table identifier per the provider's rules, escaping whatever character
    ///     would otherwise terminate the quoting early.
    /// </summary>
    string QuoteIdentifier(string name);

    /// <summary>
    ///     How the table is named in a DML statement — schema-qualified and quoted where the provider
    ///     has schemas, the bare quoted name where it does not (SQLite folds its logical schema into
    ///     the table name instead).
    /// </summary>
    string TableReference(DbObjectName table);

    /// <summary>
    ///     The placeholder for the parameter at <paramref name="index" />. Index 0 is always the
    ///     primary key; the column maps that read from the event take 1..N in declaration order.
    /// </summary>
    string ParameterName(int index);

    /// <summary>
    ///     <paramref name="value" /> as a complete, quoted SQL string literal with the provider's
    ///     escaping applied. Used by <c>SetValue(column, "literal")</c>, which is fixed at
    ///     configuration time and therefore baked into the statement rather than parameterized.
    /// </summary>
    string StringLiteral(string value);

    /// <summary>
    ///     How the update branch refers to <paramref name="columnName" />'s <em>pre-update</em> value.
    /// </summary>
    string ExistingRowReference(DbObjectName table, string columnName);

    /// <summary>
    ///     Build the upsert for one event type's column mappings.
    /// </summary>
    FlatTableUpsert BuildUpsert(FlatTableUpsertRequest request);

    /// <summary>
    ///     Build the statement that removes a flat table's row, keyed on parameter 0.
    /// </summary>
    string BuildDelete(DbObjectName table, string primaryKeyColumn);
}
