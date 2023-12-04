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
        creating_a_full_text_index_with_custom_data_configuration_should_create_the_index_without_regConfig_in_indexname_custom_data_configuration()
    {
        const string documentConfig = "(data ->> 'AnotherString' || ' ' || 'test')";

        var index = new FullTextIndexDefinition(TableName, documentConfig);

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX {TableName.Name}_idx_fts ON {TableName.QualifiedName} USING gin (to_tsvector('english',{documentConfig}));"
        );
    }

    [Fact]
    public void
        creating_a_full_text_index_with_custom_data_configuration_and_custom_regConfig_should_create_the_index_with_custom_regConfig_in_indexname_custom_data_configuration()
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

//
//
// #region sample_using_a_full_text_index_through_attribute_on_class_with_default
//
// [FullTextIndex]
// public class Book
// {
//     public Guid Id { get; set; }
//
//     public string Title { get; set; }
//
//     public string Author { get; set; }
//
//     public string Information { get; set; }
// }
//
// #endregion
//
// #region sample_using_a_single_property_full_text_index_through_attribute_with_default
//
// public class UserProfile
// {
//     public Guid Id { get; set; }
//
//     [FullTextIndex] public string Information { get; set; }
// }
//
// #endregion
//
// #region sample_using_a_single_property_full_text_index_through_attribute_with_custom_settings
//
// public class UserDetails
// {
//     private const string FullTextIndexName = "mt_custom_user_details_fts_idx";
//
//     public Guid Id { get; set; }
//
//     [FullTextIndex(IndexName = FullTextIndexName, RegConfig = "italian")]
//     public string Details { get; set; }
// }
//
// #endregion
//
// #region sample_using_multiple_properties_full_text_index_through_attribute_with_default
//
// public class Article
// {
//     public Guid Id { get; set; }
//
//     [FullTextIndex] public string Heading { get; set; }
//
//     [FullTextIndex] public string Text { get; set; }
// }
//
// #endregion
//
// #region sample_using_multiple_properties_full_text_index_through_attribute_with_custom_settings
//
// public class BlogPost
// {
//     public Guid Id { get; set; }
//
//     public string Category { get; set; }
//
//     [FullTextIndex] public string EnglishText { get; set; }
//
//     [FullTextIndex(RegConfig = "italian")] public string ItalianText { get; set; }
//
//     [FullTextIndex(RegConfig = "french")] public string FrenchText { get; set; }
// }
//
// #endregion
//
// public class full_text_index: OneOffConfigurationsContext
// {
//
//
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public void
//         creating_a_full_text_index_with_custom_data_configuration_and_custom_regConfig_custom_indexName_should_create_the_index_with_custom_indexname_custom_data_configuration()
//     {
//         const string documentConfig = "(data ->> 'AnotherString' || ' ' || 'test')";
//         const string RegConfig = "french";
//         const string IndexName = "custom_index_name";
//
//         StoreOptions(_ => _.Schema.For<Target>().FullTextIndex(
//             index =>
//             {
//                 index.documentConfig = documentConfig;
//                 index.RegConfig = RegConfig;
//                 index.Name = IndexName;
//             }));
//
//         var data = Target.GenerateRandomData(100).ToArray();
//         theStore.BulkInsert(data);
//
//         theStore.StorageFeatures
//             .ShouldContainIndexDefinitionFor<Target>(
//                 indexName: IndexName,
//                 regConfig: RegConfig,
//                 documentConfig: documentConfig
//             );
//     }
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public async Task wholedoc_fts_index_comparison_works()
//     {
//         StoreOptions(_ =>
//         {
//             _.Schema.For<User>().FullTextIndex();
//         });
//
//         // Apply changes
//         await theStore.Storage.ApplyAllConfiguredChangesToDatabaseAsync();
//
//         // Look at updates after that
//         var patch = await theStore.Storage.Database.CreateMigrationAsync();
//
//         var patchSql = patch.UpdateSql();
//
//         Assert.DoesNotContain("drop index if exists full_text_index.mt_doc_user_idx_fts", patchSql);
//         Assert.DoesNotContain("drop index full_text_index.mt_doc_user_idx_fts", patchSql);
//     }
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public async Task fts_index_comparison_must_take_into_account_automatic_cast()
//     {
//         StoreOptions(_ =>
//         {
//             _.Schema.For<Company>()
//                 .FullTextIndex(x => x.Name);
//         });
//
//         // Apply changes
//         await theStore.Storage.ApplyAllConfiguredChangesToDatabaseAsync();
//
//         // Look at updates after that
//         var patch = await theStore.Storage.Database.CreateMigrationAsync();
//
//         var patchSql = patch.UpdateSql();
//
//         Assert.DoesNotContain("drop index if exists full_text_index.mt_doc_user_idx_fts", patchSql);
//         Assert.DoesNotContain("drop index full_text_index.mt_doc_user_idx_fts", patchSql);
//     }
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public async Task multifield_fts_index_comparison_must_take_into_account_automatic_cast()
//     {
//         StoreOptions(_ =>
//         {
//             _.Schema.For<User>()
//                 .FullTextIndex(x => x.FirstName, x => x.LastName);
//         });
//
//         // Apply changes
//         await theStore.Storage.ApplyAllConfiguredChangesToDatabaseAsync();
//
//         // Look at updates after that
//         var patch = await theStore.Storage.Database.CreateMigrationAsync();
//
//         var patchSql = patch.UpdateSql();
//
//         Assert.DoesNotContain("drop index if exists full_text_index.mt_doc_user_idx_fts", patchSql);
//         Assert.DoesNotContain("drop index full_text_index.mt_doc_user_idx_fts", patchSql);
//     }
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public async Task modified_fts_index_comparison_must_generate_drop()
//     {
//         StoreOptions(_ =>
//         {
//             _.Schema.For<User>()
//                 .FullTextIndex(x => x.FirstName);
//         });
//
//         // Apply changes
//         await theStore.Storage.ApplyAllConfiguredChangesToDatabaseAsync();
//
//         // Change indexed fields
//         StoreOptions(_ =>
//         {
//             _.Schema.For<User>()
//                 .FullTextIndex(x => x.FirstName, x => x.LastName);
//         }, false);
//
//         // Look at updates after that
//         var patch = await theStore.Storage.Database.CreateMigrationAsync();
//
//         Assert.Contains("drop index if exists full_text_index.mt_doc_user_idx_fts", patch.UpdateSql());
//     }
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public async Task migration_from_v3_to_v4_should_not_result_in_schema_difference()
//     {
//         // setup/simulate a full text index as in v3
//         StoreOptions(_ =>
//         {
//             _.Schema.For<User>().FullTextIndex();
//         });
//
//         await theStore.Storage.ApplyAllConfiguredChangesToDatabaseAsync();
//
//         // drop and recreate index with a sql statement not containing `::regconfig`
//         await using (var conn = new NpgsqlConnection(ConnectionSource.ConnectionString))
//         {
//             await conn.OpenAsync();
//             await conn.CreateCommand("DROP INDEX if exists full_text_index.mt_doc_user_idx_fts")
//                 .ExecuteNonQueryAsync();
//             await conn.CreateCommand(
//                     "CREATE INDEX mt_doc_user_idx_fts ON full_text_index.mt_doc_user USING gin (( to_tsvector('english', data) ))")
//                 .ExecuteNonQueryAsync();
//         }
//
//         // create another store and check if there is no schema difference
//         var store2 = DocumentStore.For(_ =>
//         {
//             _.Connection(ConnectionSource.ConnectionString);
//             _.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;
//             _.DatabaseSchemaName = "fulltext";
//
//             _.Schema.For<User>().FullTextIndex();
//         });
//         await Should.NotThrowAsync(async () => await store2.Storage.Database.AssertDatabaseMatchesConfigurationAsync());
//     }
// }
//
// public static class FullTextIndexTestsExtension
// {
//     public static void ShouldContainIndexDefinitionFor<TDocument>(
//         this StorageFeatures storage,
//         string tableName = "full_text_index.mt_doc_target",
//         string indexName = "mt_doc_target_idx_fts",
//         string regConfig = "english",
//         string documentConfig = null)
//     {
//         var documentMapping = storage.MappingFor(typeof(TDocument));
//         var table = new DocumentTable(documentMapping);
//         var ddl = documentMapping.Indexes
//             .Where(x => x.Name == indexName)
//             .Select(x => x.ToDDL(table))
//             .FirstOrDefault();
//
//         ddl.ShouldNotBeNull();
//
//         SpecificationExtensions.ShouldContain(ddl, $"CREATE INDEX {indexName}");
//         SpecificationExtensions.ShouldContain(ddl, $"ON {tableName}");
//         SpecificationExtensions.ShouldContain(ddl, $"to_tsvector('{regConfig}',{documentConfig})");
//
//         if (regConfig != null)
//         {
//             SpecificationExtensions.ShouldContain(ddl, regConfig);
//         }
//
//         if (documentConfig != null)
//         {
//             SpecificationExtensions.ShouldContain(ddl, documentConfig);
//         }
//     }
// }
