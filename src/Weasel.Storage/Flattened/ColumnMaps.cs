using System.Globalization;

namespace Weasel.Storage.Flattened;

/// <summary>Writes an event member straight onto the column.</summary>
public sealed class MemberMap: IColumnMap
{
    public MemberMap(string columnName, Type columnType)
    {
        ColumnName = columnName;
        ColumnType = columnType;
    }

    /// <inheritdoc />
    public string ColumnName { get; }

    /// <inheritdoc />
    public Type ColumnType { get; }

    /// <inheritdoc />
    public bool RequiresInput => true;

    /// <inheritdoc />
    public string UpdateExpression(in FlatTableColumnContext context, string parameterName)
        => $"{context.Quote(ColumnName)} = {parameterName}";

    /// <inheritdoc />
    public string InsertExpression(in FlatTableColumnContext context, string parameterName)
        => parameterName;
}

/// <summary>Adds an event member's value to the column.</summary>
public sealed class IncrementMemberMap: IColumnMap
{
    public IncrementMemberMap(string columnName, Type columnType)
    {
        ColumnName = columnName;
        ColumnType = columnType;
    }

    /// <inheritdoc />
    public string ColumnName { get; }

    /// <inheritdoc />
    public Type ColumnType { get; }

    /// <inheritdoc />
    public bool RequiresInput => true;

    /// <inheritdoc />
    public string UpdateExpression(in FlatTableColumnContext context, string parameterName)
        => $"{context.Quote(ColumnName)} = {context.Existing(ColumnName)} + {parameterName}";

    /// <summary>
    ///     A row that does not exist yet starts at the increment itself, not at zero-plus-it.
    /// </summary>
    public string InsertExpression(in FlatTableColumnContext context, string parameterName)
        => parameterName;
}

/// <summary>Subtracts an event member's value from the column.</summary>
public sealed class DecrementMemberMap: IColumnMap
{
    public DecrementMemberMap(string columnName, Type columnType)
    {
        ColumnName = columnName;
        ColumnType = columnType;
    }

    /// <inheritdoc />
    public string ColumnName { get; }

    /// <inheritdoc />
    public Type ColumnType { get; }

    /// <inheritdoc />
    public bool RequiresInput => true;

    /// <inheritdoc />
    public string UpdateExpression(in FlatTableColumnContext context, string parameterName)
        => $"{context.Quote(ColumnName)} = {context.Existing(ColumnName)} - {parameterName}";

    /// <summary>
    ///     Polecat's and Fisher's shape: a first sighting inserts the value as given. Marten's
    ///     equivalent inserts <c>-value</c> instead — see the type's remarks in the lift's PR; the
    ///     divergence is preserved here as the two-store majority rather than reconciled silently.
    /// </summary>
    public string InsertExpression(in FlatTableColumnContext context, string parameterName)
        => parameterName;
}

/// <summary>Adds one to the column, with nothing read from the event.</summary>
public sealed class IncrementMap: IColumnMap
{
    public IncrementMap(string columnName)
    {
        ColumnName = columnName;
    }

    /// <inheritdoc />
    public string ColumnName { get; }

    /// <inheritdoc />
    public Type ColumnType => typeof(int);

    /// <inheritdoc />
    public bool RequiresInput => false;

    /// <inheritdoc />
    public string UpdateExpression(in FlatTableColumnContext context, string parameterName)
        => $"{context.Quote(ColumnName)} = {context.Existing(ColumnName)} + 1";

    /// <summary>
    ///     A first sighting counts once, so the row starts at 1. Marten's equivalent inserts 0 — see
    ///     the lift's PR; the divergence is preserved here as the two-store majority.
    /// </summary>
    public string InsertExpression(in FlatTableColumnContext context, string parameterName) => "1";
}

/// <summary>Subtracts one from the column, with nothing read from the event.</summary>
public sealed class DecrementMap: IColumnMap
{
    public DecrementMap(string columnName)
    {
        ColumnName = columnName;
    }

    /// <inheritdoc />
    public string ColumnName { get; }

    /// <inheritdoc />
    public Type ColumnType => typeof(int);

    /// <inheritdoc />
    public bool RequiresInput => false;

    /// <inheritdoc />
    public string UpdateExpression(in FlatTableColumnContext context, string parameterName)
        => $"{context.Quote(ColumnName)} = {context.Existing(ColumnName)} - 1";

    /// <summary>All three stores insert 0 here; it is not the mirror of <see cref="IncrementMap" />.</summary>
    public string InsertExpression(in FlatTableColumnContext context, string parameterName) => "0";
}

/// <summary>Sets the column to a configured string.</summary>
/// <remarks>
///     The value lands in a SQL string literal rather than a parameter, because it is fixed at
///     configuration time and baking it in keeps the parameter list aligned with the maps that
///     genuinely read from the event. Escaping is therefore the dialect's job and not optional — an
///     embedded quote that is not doubled terminates the literal and the remainder is parsed as SQL
///     (polecat#390).
/// </remarks>
public sealed class SetStringValueMap: IColumnMap
{
    private readonly string _value;

    public SetStringValueMap(string columnName, string value)
    {
        ColumnName = columnName;
        _value = value;
    }

    /// <inheritdoc />
    public string ColumnName { get; }

    /// <inheritdoc />
    public Type ColumnType => typeof(string);

    /// <inheritdoc />
    public bool RequiresInput => false;

    /// <inheritdoc />
    public string UpdateExpression(in FlatTableColumnContext context, string parameterName)
        => $"{context.Quote(ColumnName)} = {context.Literal(_value)}";

    /// <inheritdoc />
    public string InsertExpression(in FlatTableColumnContext context, string parameterName)
        => context.Literal(_value);
}

/// <summary>Sets the column to a configured integer.</summary>
public sealed class SetIntValueMap: IColumnMap
{
    private readonly string _literal;

    public SetIntValueMap(string columnName, int value)
    {
        ColumnName = columnName;

        // Invariant, so a column literal never picks up a locale's digit or sign formatting.
        _literal = value.ToString(CultureInfo.InvariantCulture);
    }

    /// <inheritdoc />
    public string ColumnName { get; }

    /// <inheritdoc />
    public Type ColumnType => typeof(int);

    /// <inheritdoc />
    public bool RequiresInput => false;

    /// <inheritdoc />
    public string UpdateExpression(in FlatTableColumnContext context, string parameterName)
        => $"{context.Quote(ColumnName)} = {_literal}";

    /// <inheritdoc />
    public string InsertExpression(in FlatTableColumnContext context, string parameterName) => _literal;
}
