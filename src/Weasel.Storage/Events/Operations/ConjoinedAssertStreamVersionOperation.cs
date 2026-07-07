#nullable enable
using System.Data;
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Conjoined-tenant closed-shape assert-stream-version operation. Emits
/// <c>select version from {schema}.mt_streams where id = $1 and tenant_id = $2</c>.
/// Selected by <see cref="EventStorage{TId}.AssertStreamVersion"/> when the
/// events table is conjoined-tenant. The trailing tenant predicate restores the
/// <c>(tenant_id, id)</c> primary-key point seek and scopes the version check to
/// the fetching tenant.
/// </summary>
internal sealed class ConjoinedAssertStreamVersionOperation<TId>: AssertStreamVersionOperation<TId>
    where TId : notnull
{
    // Matches JasperFx.StorageConstants.TenantIdColumn — the metadata tenant
    // column name is the same across the Postgres and SQL Server dialects.
    private const string TenantIdColumnName = "tenant_id";

    public ConjoinedAssertStreamVersionOperation(string selectVersionByIdPrefix, StreamAction stream)
        : base(selectVersionByIdPrefix, stream)
    {
    }

    public override void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        builder.Append(SelectVersionByIdPrefix);
        var idParam = builder.AppendParameter(StreamIdentity);
        idParam.DbType = IdDbType;

        builder.Append(" and ");
        builder.Append(TenantIdColumnName);
        builder.Append(" = ");
        var tenantParam = builder.AppendParameter((object)Stream.TenantId);
        tenantParam.DbType = DbType.String;
    }
}
