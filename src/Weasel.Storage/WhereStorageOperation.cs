#nullable enable
using System.Data.Common;
using Weasel.Core;
using Weasel.Core.SqlGeneration;

namespace Weasel.Storage;

/// <summary>
///     One statement against every document matching a predicate — a store's <c>DeleteWhere</c>,
///     <c>HardDeleteWhere</c> and <c>UndoDeleteWhere</c>, which differ only in the SQL head (the
///     <see cref="IOperationFragment" />) and the guard they carry.
/// </summary>
/// <remarks>
///     <para>
///         Renders <c>{head} where [tenant_id = ? and] [{guard} and] {predicate}</c>. The order is the
///         point: the tenant scope and the guard are the storage's own terms and the predicate is the
///         caller's, so keeping the caller's last means an <c>or</c> inside it cannot swallow them. A
///         compound predicate is expected to arrive parenthesized.
///     </para>
///     <para>
///         The guard is what keeps a soft <c>DeleteWhere</c> from re-stamping rows that are already
///         deleted (<c>is_deleted = 0</c> or its equivalent), and an <c>UndoDeleteWhere</c> to rows that are
///         (<c>is_deleted = 1</c>). A hard delete passes none.
///     </para>
///     <para>
///         ⚠️ The tenant id is bound by <see cref="AppendTenantId" />, which by default appends an untyped
///         parameter. On SQL Server that goes out as <c>nvarchar</c>, and an <c>nvarchar</c> parameter
///         against a <c>varchar</c> <c>tenant_id</c> converts the column and scans; a store with a
///         <c>varchar</c> tenant column overrides it (for instance setting
///         <see cref="System.Data.DbType.AnsiString" />).
///     </para>
///     <para>
///         No trailing <c>;</c>: the session separates statements, so this belongs in a batch whose builder
///         really splits commands — see <see cref="OperationBatchExecutor.Configure" />.
///     </para>
/// </remarks>
public class WhereStorageOperation: IStorageOperation
{
    private readonly IOperationFragment _fragment;
    private readonly ISqlFragment _where;
    private readonly OperationRole _role;

    /// <param name="fragment">The statement's head, e.g. <c>delete from …</c> or a soft-delete <c>update</c>.</param>
    /// <param name="documentType">The document type the statement targets.</param>
    /// <param name="where">The caller's predicate.</param>
    /// <param name="tenantId">The tenant to scope to, or <see langword="null" /> for a single-tenant table.</param>
    /// <param name="guard">The storage's own extra condition, or <see langword="null" />.</param>
    /// <param name="role">What the statement does, for unit-of-work ordering and change sets.</param>
    public WhereStorageOperation(IOperationFragment fragment, Type documentType, ISqlFragment where,
        string? tenantId, string? guard, OperationRole role)
    {
        _fragment = fragment ?? throw new ArgumentNullException(nameof(fragment));
        _where = where ?? throw new ArgumentNullException(nameof(where));
        DocumentType = documentType ?? throw new ArgumentNullException(nameof(documentType));
        TenantId = tenantId;
        Guard = guard;
        _role = role;
    }

    /// <inheritdoc />
    public Type DocumentType { get; }

    /// <summary>The tenant the statement is scoped to, or <see langword="null" />.</summary>
    public string? TenantId { get; }

    /// <summary>The storage's own extra condition, or <see langword="null" />.</summary>
    public string? Guard { get; }

    /// <summary>The tenant column's name; <c>tenant_id</c> unless set.</summary>
    public string TenantIdColumn { get; init; } = "tenant_id";

    /// <inheritdoc />
    public OperationRole Role() => _role;

    /// <inheritdoc />
    public void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        _fragment.Apply(builder);
        builder.Append(" where ");

        if (TenantId is not null)
        {
            builder.Append(TenantIdColumn);
            builder.Append(" = ");
            AppendTenantId(builder, TenantId);
            builder.Append(" and ");
        }

        if (Guard is not null)
        {
            builder.Append(Guard);
            builder.Append(" and ");
        }

        _where.Apply(builder);
    }

    /// <summary>
    ///     Append the tenant id's parameter. The default appends it untyped; see the remarks on the class
    ///     for when a store should override this.
    /// </summary>
    protected virtual DbParameter AppendTenantId(ICommandBuilder builder, string tenantId)
        => builder.AppendParameter(tenantId);

    /// <inheritdoc />
    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
