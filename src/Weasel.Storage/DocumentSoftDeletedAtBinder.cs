#nullable enable
using System;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Reflection;

namespace Weasel.Storage;

/// <summary>
/// W3 spike (M9): <see cref="IDocumentMetadataBinder{TDoc}"/> for the
/// <c>mt_deleted_at</c> column on a soft-delete mapping. Each write
/// binds <see cref="DBNull"/> — only the soft-delete operation (issued
/// via the inherited <c>DeleteFragment</c>) writes a concrete timestamp.
/// Saving a previously soft-deleted document clears the timestamp,
/// matching <c>UpsertFunction</c> codegen.
/// </summary>
public sealed class DocumentSoftDeletedAtBinder<TDoc>: IDocumentMetadataBinder<TDoc>
    where TDoc : notnull
{
    private readonly Action<TDoc, DateTimeOffset?>? _setter;

    private readonly IStorageDialect _dialect;

    public DocumentSoftDeletedAtBinder(string columnName, IStorageDialect dialect, MemberInfo? member)
    {
        ColumnName = columnName;
        _dialect = dialect;
        if (member is not null)
        {
            _setter = LambdaBuilder.Setter<TDoc, DateTimeOffset?>(member);
        }
    }

    public string ColumnName { get; }

    public string ValueSql => "?";

    public void BindParameter(DbParameter parameter, TDoc document, IStorageSession session)
    {
        parameter.Value = DBNull.Value;
        _dialect.SetParameterType(parameter, StorageColumnType.Timestamp);
    }

    public void Apply(DbDataReader reader, int columnOrdinal, TDoc document, IStorageSession session)
    {
        if (_setter is null) return;
        if (reader.IsDBNull(columnOrdinal))
        {
            _setter(document, null);
            return;
        }

        var value = reader.GetFieldValue<DateTimeOffset>(columnOrdinal);
        _setter(document, value);
    }

    public BulkColumnValue GetBulkValue(TDoc document) => BulkColumnValue.Null;
}
