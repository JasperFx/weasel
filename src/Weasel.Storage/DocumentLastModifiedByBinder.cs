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
/// <c>last_modified_by</c> column. Value comes from
/// <see cref="IStorageSession.LastModifiedBy"/> (alias of
/// <c>CurrentUserName</c>) on every write; read path projects the stored
/// value onto the document's <c>[LastModifiedBy]</c>-annotated member
/// when one exists.
/// </summary>
public sealed class DocumentLastModifiedByBinder<TDoc>: IDocumentMetadataBinder<TDoc>
    where TDoc : notnull
{
    private readonly Action<TDoc, string?>? _setter;

    private readonly IStorageDialect _dialect;

    public DocumentLastModifiedByBinder(string columnName, IStorageDialect dialect, MemberInfo? lastModifiedByMember)
    {
        ColumnName = columnName;
        _dialect = dialect;
        if (lastModifiedByMember is not null)
        {
            _setter = LambdaBuilder.Setter<TDoc, string?>(lastModifiedByMember);
        }
    }

    public string ColumnName { get; }

    public string ValueSql => "?";

    public void BindParameter(DbParameter parameter, TDoc document, IStorageSession session)
    {
        _dialect.SetParameterType(parameter, StorageColumnType.String);
        // IMetadataContext.LastModifiedBy is the alias of CurrentUserName;
        // prefer CurrentUserName since the former is marked [Obsolete].
        parameter.Value = (object?)session.CurrentUserName ?? DBNull.Value;
    }

    public void ApplyToDocument(TDoc document, IStorageSession session)
        => _setter?.Invoke(document, session.CurrentUserName);

    public void Apply(DbDataReader reader, int columnOrdinal, TDoc document, IStorageSession session)
    {
        if (_setter is null) return;
        if (reader.IsDBNull(columnOrdinal)) return;

        var value = reader.GetFieldValue<string>(columnOrdinal);
        _setter(document, value);
    }

    public BulkColumnValue GetBulkValue(TDoc document) => BulkColumnValue.Null;
}
