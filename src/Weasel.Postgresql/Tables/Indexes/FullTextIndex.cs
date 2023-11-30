using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Postgresql.Tables.Indexes;

public class FullTextIndex: IndexDefinition
{
    public const string DefaultRegConfig = "english";
    public const string DefaultDataConfig = "data";

    private readonly PostgresqlObjectName table;
    private string dataConfig;
    private readonly string? indexName;
    private readonly string? indexPrefix;

    private string _regConfig;

    public FullTextIndex(
        PostgresqlObjectName tableName,
        string? regConfig = null,
        string? dataConfig = null,
        string? indexName = null,
        string? indexPrefix = null)
    {
        table = tableName;
        RegConfig = regConfig;
        DataConfig = dataConfig;
        this.indexName = indexName;
        this.indexPrefix = indexPrefix;

        Method = IndexMethod.gin;
    }

    public string? RegConfig
    {
        get => _regConfig;
        set => _regConfig = value ?? DefaultRegConfig;
    }

    public string? DataConfig
    {
        get => dataConfig;
        set => dataConfig = value ?? DefaultDataConfig;
    }

    public override string[] Columns
    {
        get => new[] { $"to_tsvector('{_regConfig}',{dataConfig.Trim()})" };
        set
        {
            // nothing
        }
    }

    protected override string deriveIndexName()
    {
        var lowerValue = indexName?.ToLowerInvariant();
        if (indexPrefix != null && lowerValue?.StartsWith(indexPrefix) == true)
        {
            return lowerValue.ToLowerInvariant();
        }

        if (indexPrefix != null && lowerValue?.IsNotEmpty() == true)
        {
            return indexPrefix + lowerValue.ToLowerInvariant();
        }

        if (_regConfig != DefaultRegConfig)
        {
            return $"{table.Name}_{_regConfig}_idx_fts";
        }

        return $"{table.Name}_idx_fts";
    }
}
