using Weasel.Core;

namespace Weasel.Storage.Flattened;

/// <summary>
///     The dialect-neutral half of a flat-table projection: resolving the columns a set of mappings
///     names, and handing them to a dialect to be rendered.
/// </summary>
/// <remarks>
///     Everything here works against <see cref="ITable" /> rather than a provider's concrete
///     <c>Table</c>, which is what lets one implementation serve every store. A store's own
///     <c>StatementMap</c> keeps only what genuinely cannot be lifted: returning its provider's
///     fluent <c>ColumnExpression</c>, building its parameter binding, and queueing its
///     <c>IStorageOperation</c>.
/// </remarks>
public static class FlatTableStatementBuilder
{
    /// <summary>
    ///     The single column a flat table's rows are keyed on.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///     The table declares no primary key, which is a configuration mistake worth naming at
    ///     compile time rather than meeting as a duplicate-row bug later.
    /// </exception>
    public static string PrimaryKeyColumn(ITable table)
    {
        ArgumentNullException.ThrowIfNull(table);

        var primaryKey = table.PrimaryKeyColumns.FirstOrDefault();

        return primaryKey ?? throw new InvalidOperationException(
            $"The flat table '{table.Identifier}' has no primary key column. Declare one in the "
            + "projection's constructor with Table.AddColumn(...).AsPrimaryKey() before mapping events.");
    }

    /// <summary>
    ///     Build the upsert for one event type's mappings.
    /// </summary>
    public static FlatTableUpsert Upsert(IFlatTableSqlDialect dialect, ITable table, Type eventType,
        IReadOnlyList<IColumnMap> columns)
    {
        ArgumentNullException.ThrowIfNull(dialect);

        return dialect.BuildUpsert(
            new FlatTableUpsertRequest(table, eventType, PrimaryKeyColumn(table), columns));
    }

    /// <summary>
    ///     Build the statement that removes a flat table's row, keyed on parameter 0.
    /// </summary>
    public static string Delete(IFlatTableSqlDialect dialect, ITable table)
    {
        ArgumentNullException.ThrowIfNull(dialect);

        return dialect.BuildDelete(table.Identifier, PrimaryKeyColumn(table));
    }

    /// <summary>
    ///     Find or add the column a mapping writes.
    /// </summary>
    /// <param name="table">The flat table being shaped.</param>
    /// <param name="map">The mapping naming the column.</param>
    /// <param name="columnTypeFor">
    ///     An optional CLR-type-to-column-type rule. Supply one where a store has an opinion the
    ///     provider's own type map does not carry — a strong-typed id stored as its inner primitive,
    ///     for instance. Omit it to take the provider's mapping.
    /// </param>
    /// <remarks>
    ///     Matched case-insensitively on every provider, including the ones that compare
    ///     <em>values</em> case-sensitively: identifiers are the exception, so two mappings naming
    ///     <c>Amount</c> and <c>amount</c> mean one column and adding both would emit DDL the database
    ///     rejects as a duplicate (polecat#45).
    /// </remarks>
    public static ITableColumn ResolveColumn(ITable table, IColumnMap map, Func<Type, string>? columnTypeFor = null)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(map);

        foreach (var column in table.Columns)
        {
            if (string.Equals(column.Name, map.ColumnName, StringComparison.OrdinalIgnoreCase))
            {
                return column;
            }
        }

        return columnTypeFor is null
            ? table.AddColumn(map.ColumnName, map.ColumnType)
            : table.AddColumn(map.ColumnName, columnTypeFor(map.ColumnType));
    }
}
