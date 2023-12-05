using Shouldly;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Indexes;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.Indexes;

public class FullTextIndexTests
{
    private const string TablePrefix = "mt_";
    private const string DataDocumentConfig = "data";
    private static readonly PostgresqlObjectName TableName = new("public", "mt_doc_target");
    private readonly Table parent = new(TableName);

    [Fact]
    public void creating_a_full_text_index_should_create_the_index_on_the_table()
    {
        var index = new FullTextIndexDefinition(TableName, DataDocumentConfig, indexPrefix: TablePrefix);

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX {TableName.Name}_idx_fts ON {TableName.QualifiedName} USING gin (to_tsvector('english',data));"
        );
    }

    [Fact]
    public void
        creating_a_full_text_index_with_custom_indexName_without_tableprefix_should_create_the_index_on_the_table()
    {
        var indexName = "custom_index_name";
        var index = new FullTextIndexDefinition(
            TableName,
            DataDocumentConfig,
            indexName: indexName,
            indexPrefix: TablePrefix
        );

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX {TablePrefix}{indexName} ON {TableName.QualifiedName} USING gin (to_tsvector('english',data));"
        );
    }

    [Fact]
    public void
        creating_a_full_text_index_with_custom_indexName_without_prefix_should_create_the_index_on_the_table()
    {
        var indexName = "custom_index_name";
        var index = new FullTextIndexDefinition(
            TableName,
            DataDocumentConfig,
            indexName: indexName
        );

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX {indexName} ON {TableName.QualifiedName} USING gin (to_tsvector('english',data));"
        );
    }

    [Fact]
    public void
        creating_a_full_text_index_with_custom_document_configuration_should_create_the_index_without_regConfig_in_indexname_custom_document_configuration()
    {
        const string documentConfig = "(data ->> 'AnotherString' || ' ' || 'test')";

        var index = new FullTextIndexDefinition(TableName, documentConfig);

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX {TableName.Name}_idx_fts ON {TableName.QualifiedName} USING gin (to_tsvector('english',{documentConfig}));"
        );
    }

    [Fact]
    public void
        creating_a_full_text_index_with_custom_document_configuration_and_custom_regConfig_should_create_the_index_with_custom_regConfig_in_indexname_custom_document_configuration()
    {
        const string documentConfig = "(data ->> 'AnotherString' || ' ' || 'test')";
        const string regConfig = "french";

        var index = new FullTextIndexDefinition(
            TableName,
            documentConfig,
            regConfig: regConfig, indexPrefix: TablePrefix);

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX mt_doc_target_{regConfig}_idx_fts ON {TableName.QualifiedName} USING gin (to_tsvector('{regConfig}',{documentConfig}));"
        );
    }

    [Fact]
    public void
        creating_a_full_text_index_with_single_member_should_create_the_index_without_regConfig_in_indexname_and_member_selectors()
    {
        const string documentConfig = "(data ->> 'SomeProperty')";

        var index = new FullTextIndexDefinition(
            TableName,
            documentConfig,
            indexPrefix: TablePrefix);

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX mt_doc_target_idx_fts ON {TableName.QualifiedName} USING gin (to_tsvector('english',{documentConfig}));"
        );
    }

    [Fact]
    public void
        creating_a_full_text_index_with_multiple_members_should_create_the_index_without_regConfig_in_indexname_and_members_selectors()
    {
        const string documentConfig = "((data ->> 'SomeProperty') || ' ' || (data ->> 'AnotherProperty'))";

        var index = new FullTextIndexDefinition(
            TableName,
            documentConfig,
            indexPrefix: TablePrefix);

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX mt_doc_target_idx_fts ON {TableName.QualifiedName} USING gin (to_tsvector('english',{documentConfig}));"
        );
    }

    [Fact]
    public void
        creating_a_full_text_index_with_multiple_members_and_custom_configuration_should_create_the_index_with_custom_configuration_and_members_selectors()
    {
        const string indexName = "custom_index_name";
        const string regConfig = "french";

        const string documentConfig = "((data ->> 'SomeProperty') || ' ' || (data ->> 'AnotherProperty'))";

        var index = new FullTextIndexDefinition(
            TableName,
            documentConfig,
            regConfig,
            indexName,
            TablePrefix
        );

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX {TablePrefix}{indexName} ON {TableName.QualifiedName} USING gin (to_tsvector('{regConfig}',{documentConfig}));"
        );
    }

    [Fact]
    public void
        creating_multiple_full_text_index_with_different_regConfigs_and_custom_data_config_should_create_the_indexes_with_different_recConfigs()
    {
        // Given
        const string frenchRegConfig = "french";
        const string frenchdocumentConfig = "(data ->> 'SomeProperty')";

        const string italianRegConfig = "italian";
        const string italiandocumentConfig = "(data ->> 'AnotherProperty')";

        var column = parent.AddColumn(new TableColumn("data", "jsonb"));

        // When
        column.AddFullTextIndex(frenchRegConfig, frenchdocumentConfig);
        column.AddFullTextIndex(italianRegConfig, italiandocumentConfig);

        // Then
        parent.Indexes.Count.ShouldBe(2);

        var frenchIndex = parent.Indexes.First();
        var italianIndex = parent.Indexes[1];

        frenchIndex.ToDDL(parent).ShouldBe(
            $"CREATE INDEX mt_doc_target_{frenchRegConfig}_idx_fts ON {TableName.QualifiedName} USING gin (to_tsvector('{frenchRegConfig}',{frenchdocumentConfig}));"
        );
        italianIndex.ToDDL(parent).ShouldBe(
            $"CREATE INDEX mt_doc_target_{italianRegConfig}_idx_fts ON {TableName.QualifiedName} USING gin (to_tsvector('{italianRegConfig}',{italiandocumentConfig}));"
        );
    }
}
