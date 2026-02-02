using System.Globalization;

namespace Weasel.Core.Tables;

/// <summary>
/// Abstract base class for fluent column configuration expressions.
/// Provides common methods for configuring column properties.
/// </summary>
/// <typeparam name="TExpression">The concrete expression type (for fluent chaining)</typeparam>
/// <typeparam name="TColumn">The column type</typeparam>
/// <typeparam name="TColumnCheck">The column check constraint type</typeparam>
public abstract class ColumnExpressionBase<TExpression, TColumn, TColumnCheck>
    where TExpression : ColumnExpressionBase<TExpression, TColumn, TColumnCheck>
    where TColumn : TableColumnBase<TColumnCheck>
    where TColumnCheck : ColumnCheckBase
{
    protected ColumnExpressionBase(TColumn column)
    {
        Column = column;
    }

    /// <summary>
    /// The column being configured
    /// </summary>
    internal TColumn Column { get; }

    /// <summary>
    /// Marks this column as part of the primary key
    /// </summary>
    public virtual TExpression AsPrimaryKey()
    {
        Column.IsPrimaryKey = true;
        Column.AllowNulls = false;
        return (TExpression)this;
    }

    /// <summary>
    /// Allows NULL values for this column
    /// </summary>
    public TExpression AllowNulls()
    {
        Column.AllowNulls = true;
        return (TExpression)this;
    }

    /// <summary>
    /// Prevents NULL values for this column
    /// </summary>
    public TExpression NotNull()
    {
        Column.AllowNulls = false;
        return (TExpression)this;
    }

    /// <summary>
    /// Sets the default value as a quoted string
    /// </summary>
    public TExpression DefaultValueByString(string value)
    {
        return DefaultValueByExpression($"'{value}'");
    }

    /// <summary>
    /// Sets the default value as an integer
    /// </summary>
    public TExpression DefaultValue(int value)
    {
        return DefaultValueByExpression(value.ToString());
    }

    /// <summary>
    /// Sets the default value as a long
    /// </summary>
    public TExpression DefaultValue(long value)
    {
        return DefaultValueByExpression(value.ToString());
    }

    /// <summary>
    /// Sets the default value as a double
    /// </summary>
    public TExpression DefaultValue(double value)
    {
        return DefaultValueByExpression(value.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Sets the default value using a raw SQL expression
    /// </summary>
    public TExpression DefaultValueByExpression(string expression)
    {
        Column.DefaultExpression = expression;
        return (TExpression)this;
    }

    /// <summary>
    /// Sets the default value from a sequence
    /// </summary>
    public abstract TExpression DefaultValueFromSequence(DbObjectName sequenceName);
}
