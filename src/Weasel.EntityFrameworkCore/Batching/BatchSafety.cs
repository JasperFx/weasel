using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     Decides whether <see cref="EntityMaterializer" /> can produce the same result EF Core would.
///     Anything it can't (owned/complex/JSON members, navigations, inheritance, tracking) is run
///     through EF Core's normal pipeline instead of being silently materialized incompletely.
/// </summary>
internal static class BatchSafety
{
    /// <summary>False when the query must go through EF Core; <paramref name="tracking" /> says whether
    ///     EF Core would track the results.</summary>
    public static bool CanMaterialize(DbContext context, IEntityType entityType, IQueryable queryable, out bool tracking)
    {
        tracking = false;
        if (entityType.GetComplexProperties().Any()) return false;
        if (entityType.GetNavigations().Any(n => n.TargetEntityType.IsOwned() || n.IsEagerLoaded)) return false;
        if (entityType.GetSkipNavigations().Any(n => n.IsEagerLoaded)) return false;
        if (entityType.GetDerivedTypes().Any()) return false;
        if (entityType.GetProperties().Any(p => !p.IsShadowProperty() && p.PropertyInfo?.SetMethod == null)) return false;

        var visitor = new QueryShapeVisitor();
        visitor.Visit(queryable.Expression);
        if (visitor.HasInclude) return false;

        tracking = entityType.FindPrimaryKey() != null &&
                   (visitor.Tracking ?? context.ChangeTracker.QueryTrackingBehavior == QueryTrackingBehavior.TrackAll);
        return true;
    }

    // A split query runs several commands, which a single result set can't answer
    public static bool IsSplitQuery(DbContext context, IQueryable queryable)
    {
        var visitor = new QueryShapeVisitor();
        visitor.Visit(queryable.Expression);
        if (visitor.Splitting != null) return visitor.Splitting.Value;

        return context.GetService<Microsoft.EntityFrameworkCore.Infrastructure.IDbContextOptions>().Extensions
            .OfType<Microsoft.EntityFrameworkCore.Infrastructure.RelationalOptionsExtension>()
            .Any(x => x.QuerySplittingBehavior == QuerySplittingBehavior.SplitQuery);
    }

    private sealed class QueryShapeVisitor : ExpressionVisitor
    {
        public bool HasInclude { get; private set; }
        public bool? Tracking { get; private set; }
        public bool? Splitting { get; private set; }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions) ||
                node.Method.DeclaringType?.Name == "RelationalQueryableExtensions")
            {
                switch (node.Method.Name)
                {
                    case nameof(EntityFrameworkQueryableExtensions.Include):
                    case nameof(EntityFrameworkQueryableExtensions.ThenInclude):
                        HasInclude = true;
                        break;
                    case "AsSplitQuery":
                        Splitting ??= true;
                        break;
                    case "AsSingleQuery":
                        Splitting ??= false;
                        break;
                    case nameof(EntityFrameworkQueryableExtensions.AsTracking):
                        Tracking ??= true;
                        break;
                    case nameof(EntityFrameworkQueryableExtensions.AsNoTracking):
                    case nameof(EntityFrameworkQueryableExtensions.AsNoTrackingWithIdentityResolution):
                        Tracking ??= false;
                        break;
                }
            }

            return base.VisitMethodCall(node);
        }
    }
}
