#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Reflection;

namespace Weasel.Storage;

/// <summary>
/// <see cref="IDocumentMetadataBinder{TDoc}"/> for the <c>headers</c>
/// (jsonb) column. Write path pulls the headers off the session — under
/// a store session with a per-batch UTF-8 byte[] cache the serialization is
/// reused across the batch so N storage ops serialize the dictionary
/// once. Read path projects the deserialized dictionary onto the
/// document's <c>[Headers]</c>-annotated member when one exists.
/// </summary>
public sealed class DocumentHeadersBinder<TDoc>: IDocumentMetadataBinder<TDoc>
    where TDoc : notnull
{
    private readonly Action<TDoc, Dictionary<string, object>?>? _setter;

    private readonly IStorageDialect _dialect;

    public DocumentHeadersBinder(string columnName, IStorageDialect dialect, MemberInfo? headersMember)
    {
        ColumnName = columnName;
        _dialect = dialect;
        if (headersMember is not null)
        {
            _setter = LambdaBuilder.Setter<TDoc, Dictionary<string, object>?>(headersMember);
        }
    }

    public string ColumnName { get; }

    public string ValueSql => "?";

    public void ApplyToDocument(TDoc document, IStorageSession session)
        => _setter?.Invoke(document, session.Headers);

    public void BindParameter(DbParameter parameter, TDoc document, IStorageSession session)
    {
        // Use the session's cached UTF-8 bytes when the owning store caches the serialized
        // headers per batch (the cache returns null when there are no headers), otherwise fall
        // back to direct serialization via the storage serializer.
        var cachedBytes = session.TryGetCachedSerializedHeaders();
        if (cachedBytes is not null)
        {
            _dialect.SetParameterType(parameter, StorageColumnType.Json);
            parameter.Value = cachedBytes;
            return;
        }

        if (session.Headers is null)
        {
            _dialect.SetParameterType(parameter, StorageColumnType.Json);
            parameter.Value = DBNull.Value;
        }
        else
        {
            session.Serializer.WriteToParameter(parameter, session.Headers);
        }
    }

    public void Apply(DbDataReader reader, int columnOrdinal, TDoc document, IStorageSession session)
    {
        if (_setter is null) return;
        if (reader.IsDBNull(columnOrdinal)) return;

        var headers = session.Serializer.FromJson<Dictionary<string, object>>(reader, columnOrdinal);
        _setter(document, headers);
    }

    public BulkColumnValue GetBulkValue(TDoc document)
        // Headers aren't carried on the COPY path — write a typed JSONB null, as before.
        => BulkColumnValue.TypedNull(StorageColumnType.Json);
}
