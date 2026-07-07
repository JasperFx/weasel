#nullable enable
using System;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Reflection;

namespace Weasel.Storage;

/// <summary>
/// <see cref="IDocumentMetadataBinder{TDoc}"/> for the <c>causation_id</c>
/// column. Value comes from <see cref="IStorageSession.CausationId"/> on
/// every write; read path projects the stored value onto the document's
/// <c>[CausationId]</c>-annotated member when one exists.
/// </summary>
public sealed class DocumentCausationIdBinder<TDoc>: IDocumentMetadataBinder<TDoc>
    where TDoc : notnull
{
    private readonly Action<TDoc, string?>? _setter;

    private readonly IStorageDialect _dialect;

    public DocumentCausationIdBinder(string columnName, IStorageDialect dialect, MemberInfo? causationIdMember)
    {
        ColumnName = columnName;
        _dialect = dialect;
        if (causationIdMember is not null)
        {
            _setter = LambdaBuilder.Setter<TDoc, string?>(causationIdMember);
        }
    }

    public string ColumnName { get; }

    public string ValueSql => "?";

    public void BindParameter(DbParameter parameter, TDoc document, IStorageSession session)
    {
        _dialect.SetParameterType(parameter, StorageColumnType.String);
        parameter.Value = (object?)session.CausationId ?? DBNull.Value;
    }

    public void ApplyToDocument(TDoc document, IStorageSession session)
        => _setter?.Invoke(document, session.CausationId);

    public void Apply(DbDataReader reader, int columnOrdinal, TDoc document, IStorageSession session)
    {
        if (_setter is null) return;
        if (reader.IsDBNull(columnOrdinal)) return;

        var value = reader.GetFieldValue<string>(columnOrdinal);
        _setter(document, value);
    }

    public BulkColumnValue GetBulkValue(TDoc document) => BulkColumnValue.Null;
}
