#nullable enable
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Weasel.Storage;

/// <summary>
/// <see cref="IDocumentMetadataBinder{TDoc}"/> for the
/// <c>mt_dotnet_type</c> column. Stores the full .NET type name of the
/// concrete instance — uses <c>document.GetType().FullName</c> so
/// hierarchical mappings record the subclass name, not the base type
/// <typeparamref name="TDoc"/>.
/// </summary>
public sealed class DocumentDotNetTypeBinder<TDoc>: IDocumentMetadataBinder<TDoc>
    where TDoc : notnull
{
    private readonly IStorageDialect _dialect;

    public DocumentDotNetTypeBinder(string columnName, IStorageDialect dialect)
    {
        ColumnName = columnName;
        _dialect = dialect;
    }

    // Fast path for non-hierarchical mappings: cached because
    // document.GetType() == typeof(TDoc) for every row.
    private static readonly string _baseTypeName = typeof(TDoc).FullName!;

    public string ColumnName { get; }

    public string ValueSql => "?";

    public void BindParameter(DbParameter parameter, TDoc document, IStorageSession session)
    {
        parameter.Value = ResolveTypeName(document);
        _dialect.SetParameterType(parameter, StorageColumnType.String);
    }

    public void Apply(DbDataReader reader, int columnOrdinal, TDoc document, IStorageSession session)
    {
        // No-op — dotnet_type isn't projected back onto the document.
    }

    public BulkColumnValue GetBulkValue(TDoc document)
        => new(ResolveTypeName(document), StorageColumnType.String);

    private static string ResolveTypeName(TDoc document)
    {
        var actual = document.GetType();
        return actual == typeof(TDoc) ? _baseTypeName : actual.FullName!;
    }
}
