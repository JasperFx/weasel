using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     Reads the variables a query captures at the moment it is queued, so that the batch SQL and EF
///     Core's own execution of the query later see the same values. EF Core otherwise reads captured
///     variables each time it runs a query, and a value that changed in between (or a shared loop
///     variable) would change the parameters, or even the SQL, EF Core expects.
/// </summary>
internal sealed class CapturedValues : ExpressionVisitor
{
    private static readonly CapturedValues Instance = new();

    public static IQueryable<T> Freeze<T>(IQueryable<T> queryable) =>
        queryable.Provider.CreateQuery<T>(Instance.Visit(queryable.Expression));

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Builds StrongBox<T> for a captured variable's type, like EF Core's own query compilation; EF Core upstream is not AOT-ready (dotnet/efcore#29761). weasel#263.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "StrongBox<T>.Value is a public field of a framework type; EF Core upstream is not trim-safe (dotnet/efcore#29761). weasel#263.")]
    protected override Expression VisitMember(MemberExpression node)
    {
        if (isQueryOrContext(node.Type) || !tryEvaluate(node, out var value) || value is IQueryable)
        {
            return base.VisitMember(node);
        }

        // A field of a constant, like a captured variable, so EF Core still sends it as a parameter
        var boxType = typeof(StrongBox<>).MakeGenericType(node.Type);
        var box = Activator.CreateInstance(boxType, value);
        return Expression.Field(Expression.Constant(box, boxType), nameof(StrongBox<object>.Value));
    }

    private static bool isQueryOrContext(Type type) =>
        typeof(IQueryable).IsAssignableFrom(type) || typeof(DbContext).IsAssignableFrom(type);

    // Evaluates a chain of fields and properties that starts at a constant (a closure) or a static member.
    // Anything that depends on the query's own lambda parameters is left for EF Core to translate.
    private static bool tryEvaluate(MemberExpression node, out object? value)
    {
        value = null;
        object? instance = null;

        switch (node.Expression)
        {
            case null:
                break;
            case ConstantExpression constant:
                instance = constant.Value;
                break;
            case MemberExpression member when !isQueryOrContext(member.Type):
                if (!tryEvaluate(member, out instance)) return false;
                break;
            default:
                return false;
        }

        switch (node.Member)
        {
            case FieldInfo field when instance != null || field.IsStatic:
                value = field.GetValue(instance);
                return true;
            case PropertyInfo property when instance != null || property.GetMethod?.IsStatic == true:
                value = property.GetValue(instance);
                return true;
            default:
                return false;
        }
    }
}
