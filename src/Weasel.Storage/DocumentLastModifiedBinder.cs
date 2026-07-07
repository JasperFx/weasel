#nullable enable
using System;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Reflection;

namespace Weasel.Storage;

/// <summary>
/// W3 spike (M1): <see cref="IDocumentMetadataBinder{TDoc}"/> for the
/// <c>mt_last_modified</c> column. Server-side value — declares
/// <see cref="ValueSql"/> as <c>transaction_timestamp()</c> and skips
/// <see cref="BindParameter"/>. The descriptor's SQL prefix bakes the
/// literal into the VALUES list so no parameter slot is reserved.
/// </summary>
public sealed class DocumentLastModifiedBinder<TDoc>: IDocumentMetadataBinder<TDoc>
    where TDoc : notnull
{
    private readonly Action<TDoc, DateTimeOffset>? _setter;

    public DocumentLastModifiedBinder(string columnName, MemberInfo? lastModifiedMember)
        : this(columnName, lastModifiedMember, "transaction_timestamp()")
    {
    }

    /// <summary>
    ///     Overload for non-Postgres dialects: <paramref name="serverTimestampSql" /> is the
    ///     engine's transaction/statement timestamp function (e.g. <c>SYSDATETIMEOFFSET()</c>
    ///     on SQL Server) baked into the SQL as the column's server-side value.
    /// </summary>
    public DocumentLastModifiedBinder(string columnName, MemberInfo? lastModifiedMember, string serverTimestampSql)
    {
        ColumnName = columnName;
        ValueSql = serverTimestampSql;
        if (lastModifiedMember is not null)
        {
            _setter = LambdaBuilder.Setter<TDoc, DateTimeOffset>(lastModifiedMember);
        }
    }

    public string ColumnName { get; }

    public string ValueSql { get; }

    public void BindParameter(DbParameter parameter, TDoc document, IStorageSession session)
    {
        // No-op — IsServerSide is true; the operation skips this binder
        // in its write loop. The SQL literal in ValueSql does the work.
    }

    public void Apply(DbDataReader reader, int columnOrdinal, TDoc document, IStorageSession session)
    {
        if (_setter is null) return;
        if (reader.IsDBNull(columnOrdinal)) return;

        var ts = reader.GetFieldValue<DateTimeOffset>(columnOrdinal);
        _setter(document, ts);
    }

    public BulkColumnValue GetBulkValue(TDoc document)
    {
        // COPY can't run transaction_timestamp() — compute client-side.
        // Slight skew vs. SQL writes that get the transaction's
        // commit time; acceptable for the bulk path.
        var now = DateTimeOffset.UtcNow;
        _setter?.Invoke(document, now);
        return new BulkColumnValue(now, StorageColumnType.Timestamp);
    }
}
