using JasperFx.Core;

namespace Weasel.Postgresql.Tables.Indexes;

public class FullTextIndexDefinition: IndexDefinition
{
    public const string DefaultRegConfig = "english";
    public const string DataDocumentConfig = "data";

    private readonly PostgresqlObjectName table;
    private readonly string? indexName;
    private readonly string? indexPrefix;

    private string regConfig;

    public FullTextIndexDefinition(
        PostgresqlObjectName tableName,
        string documentConfig,
        string? regConfig = null,
        string? indexName = null,
        string? indexPrefix = null)
    {
        table = tableName;
        this.regConfig = regConfig ?? DefaultRegConfig;
        DocumentConfig = documentConfig;
        this.indexName = indexName;
        this.indexPrefix = indexPrefix;

        Method = IndexMethod.gin;
    }

    public string? RegConfig
    {
        get => regConfig;
        set => regConfig = value ?? DefaultRegConfig;
    }

    public string DocumentConfig { get; set; }

    [Obsolete("Use DocumentConfig instead")]
    public string? DataConfig
    {
        get => DocumentConfig;
        set => DocumentConfig = value ?? DataDocumentConfig;
    }

    public override string[] Columns
    {
        get => new[] { $"to_tsvector('{regConfig}',{DocumentConfig.Trim()})" };
        set
        {
            // nothing
        }
    }

    protected override string DeriveIndexName()
    {
        var lowerValue = indexName?.ToLowerInvariant();

        if (lowerValue?.IsNotEmpty() == true)
        {
            return indexPrefix?.IsNotEmpty() == true && !lowerValue.StartsWith(indexPrefix)
                ? indexPrefix + lowerValue
                : lowerValue;
        }

        if (regConfig != DefaultRegConfig)
        {
            return $"{table.Name}_{regConfig}_idx_fts";
        }

        return $"{table.Name}_idx_fts";
    }
}
