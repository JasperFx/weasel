#nullable enable
using System;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Reflection;

namespace Weasel.Storage;

/// <summary>
/// <see cref="IDocumentMetadataBinder{TDoc}"/> for the
/// <c>correlation_id</c> column. The value comes from
/// <see cref="IStorageSession.CorrelationId"/> on every write — not from
/// the document — mirroring the codegen path's
/// <c>setStringParameter(_, session.CorrelationId)</c> emit. On read the
/// stored value is projected back onto the document's
/// <c>[CorrelationId]</c>-annotated member when one exists.
/// </summary>
public sealed class DocumentCorrelationIdBinder<TDoc>: IDocumentMetadataBinder<TDoc>
    where TDoc : notnull
{
    private readonly Action<TDoc, string?>? _setter;

    private readonly IStorageDialect _dialect;

    public DocumentCorrelationIdBinder(string columnName, IStorageDialect dialect, MemberInfo? correlationIdMember)
    {
        ColumnName = columnName;
        _dialect = dialect;
        if (correlationIdMember is not null)
        {
            _setter = LambdaBuilder.Setter<TDoc, string?>(correlationIdMember);
        }
    }

    public string ColumnName { get; }

    public string ValueSql => "?";

    public void BindParameter(DbParameter parameter, TDoc document, IStorageSession session)
    {
        _dialect.SetParameterType(parameter, StorageColumnType.String);
        parameter.Value = (object?)session.CorrelationId ?? DBNull.Value;
    }

    public void ApplyToDocument(TDoc document, IStorageSession session)
        => _setter?.Invoke(document, session.CorrelationId);

    public void Apply(DbDataReader reader, int columnOrdinal, TDoc document, IStorageSession session)
    {
        if (_setter is null) return;
        if (reader.IsDBNull(columnOrdinal)) return;

        var value = reader.GetFieldValue<string>(columnOrdinal);
        _setter(document, value);
    }

    public BulkColumnValue GetBulkValue(TDoc document)
        // Bulk loader has no session — no source for a correlation id. Write null;
        // mirrors the codegen path's bulk emit (the column was never session-aware on COPY).
        => BulkColumnValue.Null;
}
