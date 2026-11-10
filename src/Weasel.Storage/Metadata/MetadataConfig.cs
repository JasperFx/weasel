#nullable enable
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Weasel.Storage.Metadata;

/// <summary>
/// weasel#745: the object a store hands to its per-document metadata DSL — Marten's
/// <c>Schema.For&lt;T&gt;().Metadata(m =&gt; ...)</c> and its Polecat and Fisher counterparts. Each
/// metadata column is a <see cref="MetadataColumnExpression{T}"/> over a shared
/// <see cref="DocumentMetadataColumns"/>, so the DSL writes straight into the set the
/// <see cref="DocumentBinderPlan{TDoc}"/> is built from.
/// </summary>
/// <typeparam name="T">The document type, which <see cref="MetadataColumnExpression{T}.MapTo"/> resolves members against.</typeparam>
public class MetadataConfig<T>
{
    public MetadataConfig(DocumentMetadataColumns columns)
    {
        Columns = columns ?? throw new ArgumentNullException(nameof(columns));
    }

    /// <summary>
    /// The column set this DSL writes into.
    /// </summary>
    public DocumentMetadataColumns Columns { get; }

    /// <summary>The Guid optimistic-concurrency version.</summary>
    public MetadataColumnExpression<T> Version => new(Columns.Version);

    /// <summary>The monotonic numeric revision.</summary>
    public MetadataColumnExpression<T> Revision => new(Columns.Revision);

    /// <summary>Timestamp of the last write.</summary>
    public MetadataColumnExpression<T> LastModified => new(Columns.LastModified);

    /// <summary>Timestamp of the first write.</summary>
    public MetadataColumnExpression<T> CreatedAt => new(Columns.CreatedAt);

    /// <summary>The stored tenant id.</summary>
    public MetadataColumnExpression<T> TenantId => new(Columns.TenantId);

    /// <summary>The soft-delete flag.</summary>
    public MetadataColumnExpression<T> IsSoftDeleted => new(Columns.IsSoftDeleted);

    /// <summary>The soft-delete timestamp.</summary>
    public MetadataColumnExpression<T> SoftDeletedAt => new(Columns.SoftDeletedAt);

    /// <summary>The hierarchy discriminator.</summary>
    public MetadataColumnExpression<T> DocumentType => new(Columns.DocumentType);

    /// <summary>The full name of the persisted .NET type.</summary>
    public MetadataColumnExpression<T> DotNetType => new(Columns.DotNetType);

    /// <summary>The session's correlation id.</summary>
    public MetadataColumnExpression<T> CorrelationId => new(Columns.CorrelationId);

    /// <summary>The session's causation id.</summary>
    public MetadataColumnExpression<T> CausationId => new(Columns.CausationId);

    /// <summary>The session's user or process name.</summary>
    public MetadataColumnExpression<T> LastModifiedBy => new(Columns.LastModifiedBy);

    /// <summary>The session's custom headers.</summary>
    public MetadataColumnExpression<T> Headers => new(Columns.Headers);
}

/// <summary>
/// weasel#745: one metadata column inside <see cref="MetadataConfig{T}"/>. Turn the column on or off
/// with <see cref="Enabled"/>, or <see cref="MapTo"/> a member of the document, which also enables
/// it.
/// </summary>
public class MetadataColumnExpression<T>
{
    public MetadataColumnExpression(DocumentMetadataColumn column)
    {
        Column = column ?? throw new ArgumentNullException(nameof(column));
    }

    /// <summary>
    /// The column this expression configures.
    /// </summary>
    public DocumentMetadataColumn Column { get; }

    /// <summary>
    /// Whether the column is persisted for <typeparamref name="T"/>.
    /// </summary>
    public bool Enabled
    {
        get => Column.Enabled;
        set => Column.Enabled = value;
    }

    /// <summary>
    /// Map the stored value onto a member of the document, and enable the column. The member must be
    /// a single field or property of the document (<c>x =&gt; x.Member</c>).
    /// </summary>
    public MetadataColumnExpression<T> MapTo(Expression<Func<T, object?>> member)
    {
        Column.Member = MetadataMemberResolver.Resolve(member);
        Column.Enabled = true;
        return this;
    }
}

internal static class MetadataMemberResolver
{
    public static MemberInfo Resolve<T>(Expression<Func<T, object?>> expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var body = expression.Body;

        // A value-type member arrives boxed: x => (object)x.Version.
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            body = unary.Operand;
        }

        if (body is MemberExpression { Member: PropertyInfo or FieldInfo } member
            && member.Expression is ParameterExpression)
        {
            return member.Member;
        }

        throw new ArgumentException(
            $"Expression '{expression}' is not a supported metadata member mapping. Use a single field or property of the document (x => x.Member).",
            nameof(expression));
    }
}
