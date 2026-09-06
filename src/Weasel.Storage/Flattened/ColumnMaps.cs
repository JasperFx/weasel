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
    ///     A row that does not exist yet is decremented <em>from zero</em>, so a first sighting of
    ///     <c>5</c> lands the column at <c>-5</c> — the parameter negated, not the parameter as given.
    /// </summary>
    /// <remarks>
    ///     This is Marten's shape, and it was the minority one at the lift: Polecat, Fisher and the
    ///     first cut of these maps all inserted the value unchanged, which made a <em>decrement</em>
    ///     event raise the column on a fresh row. jasperfx#773 ruled Marten correct, so the negation
    ///     is now the shared behaviour and the other stores are the ones that change. The asymmetry
    ///     with <see cref="IncrementMemberMap" />, whose insert is the bare parameter, is intended:
    ///     read both as "apply this event to an implicit zero row" and they agree.
    ///     <para>
    ///         The negation lives here rather than behind a dialect hook because a leading unary minus
    ///         on a placeholder is valid in every position the insert expression is rendered into — a
    ///         <c>MERGE</c>'s <c>WHEN NOT MATCHED … VALUES</c>, an <c>INSERT … ON CONFLICT</c>'s
    ///         <c>VALUES</c>, and the <c>VALUES</c> of a generated upsert function's body, which is
    ///         where Marten has been shipping it. A provider that ever needs <c>(0 - @p)</c> instead
    ///         can supply its own <see cref="IColumnMap" /> rather than every dialect carrying a hook
    ///         no dialect uses.
    ///     </para>
    ///     <para>
    ///         Note that the parameterless <see cref="DecrementMap" /> still inserts <c>0</c> on all
    ///         four stores. jasperfx#773 asked about both and only the member-valued form was ruled
    ///         on; that one is unchanged and still agrees everywhere.
    ///     </para>
    /// </remarks>
    public string InsertExpression(in FlatTableColumnContext context, string parameterName)
        => "-" + parameterName;
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
    ///     A first sighting counts once, so the row starts at 1 rather than at 0 plus a later event.
    /// </summary>
    /// <remarks>
    ///     No longer a divergence: marten#5341 ruled 1 correct and marten#5342 brought Marten's
    ///     <c>IncrementMap</c> in line with Polecat, Fisher and these maps, so all four now agree.
    /// </remarks>
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
