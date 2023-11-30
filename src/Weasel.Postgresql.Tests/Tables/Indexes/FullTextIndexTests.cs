using Shouldly;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Indexes;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.Indexes;

public class FullTextIndexTests
{
    private const string TablePrefix = "mt_";
    private static readonly PostgresqlObjectName TableName = new("public", "mt_doc_target");
    private readonly Table parent = new(TableName);

    [Fact]
    public void creating_a_full_text_index_should_create_the_index_on_the_table()
    {
        var index = new FullTextIndex(TableName, indexPrefix: TablePrefix);

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX {TableName.Name}_idx_fts ON {TableName.QualifiedName} USING gin (to_tsvector('english',data));"
        );
    }

    [Fact]
    public void
        creating_a_full_text_index_with_custom_data_configuration_should_create_the_index_without_regConfig_in_indexname_custom_data_configuration()
    {
        const string dataConfig = "(data ->> 'AnotherString' || ' ' || 'test')";

        var index = new FullTextIndex(TableName, dataConfig: dataConfig);

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX {TableName.Name}_idx_fts ON {TableName.QualifiedName} USING gin (to_tsvector('english',{dataConfig}));"
        );
    }

    [Fact]
    public void
        creating_a_full_text_index_with_custom_data_configuration_and_custom_regConfig_should_create_the_index_with_custom_regConfig_in_indexname_custom_data_configuration()
    {
        const string dataConfig = "(data ->> 'AnotherString' || ' ' || 'test')";
        const string regConfig = "french";

        var index = new FullTextIndex(
            TableName,
            regConfig,
            dataConfig,
            indexPrefix: TablePrefix);

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX mt_doc_target_{regConfig}_idx_fts ON {TableName.QualifiedName} USING gin (to_tsvector('{regConfig}',{dataConfig}));"
        );
    }

    [Fact]
    public void
        creating_a_full_text_index_with_single_member_should_create_the_index_without_regConfig_in_indexname_and_member_selectors()
    {
        const string dataConfig = "(data ->> 'SomeProperty')";

        var index = new FullTextIndex(
            TableName,
            dataConfig: dataConfig,
            indexPrefix: TablePrefix);

        index.ToDDL(parent).ShouldBe(
            $"CREATE INDEX mt_doc_target_idx_fts ON {TableName.QualifiedName} USING gin (to_tsvector('english',{dataConfig}));"
        );
    }

//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public void
//         creating_a_full_text_index_with_multiple_members_should_create_the_index_without_regConfig_in_indexname_and_members_selectors()
//     {
//         StoreOptions(_ => _.Schema.For<Target>().FullTextIndex(d => d.String, d => d.AnotherString));
//
//         var data = Target.GenerateRandomData(100).ToArray();
//         theStore.BulkInsert(data);
//
//         theStore.StorageFeatures
//             .ShouldContainIndexDefinitionFor<Target>(
//                 indexName: $"mt_doc_target_idx_fts",
//                 dataConfig:
//                 $"((data ->> '{nameof(Target.String)}') || ' ' || (data ->> '{nameof(Target.AnotherString)}'))"
//             );
//     }
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public void
//         creating_a_full_text_index_with_multiple_members_and_custom_configuration_should_create_the_index_with_custom_configuration_and_members_selectors()
//     {
//         const string IndexName = "custom_index_name";
//         const string RegConfig = "french";
//
//         StoreOptions(_ => _.Schema.For<Target>().FullTextIndex(
//             index =>
//             {
//                 index.Name = IndexName;
//                 index.RegConfig = RegConfig;
//             },
//             d => d.AnotherString));
//
//         var data = Target.GenerateRandomData(100).ToArray();
//         theStore.BulkInsert(data);
//
//         theStore.StorageFeatures
//             .ShouldContainIndexDefinitionFor<Target>(
//                 indexName: IndexName,
//                 regConfig: RegConfig,
//                 dataConfig: $"((data ->> '{nameof(Target.AnotherString)}'))"
//             );
//     }
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public void
//         creating_multiple_full_text_index_with_different_regConfigs_and_custom_data_config_should_create_the_indexes_with_different_recConfigs()
//     {
//         const string frenchRegConfig = "french";
//         const string italianRegConfig = "italian";
//
//         StoreOptions(_ => _.Schema.For<Target>()
//             .FullTextIndex(frenchRegConfig, d => d.String)
//             .FullTextIndex(italianRegConfig, d => d.AnotherString));
//
//         var data = Target.GenerateRandomData(100).ToArray();
//         theStore.BulkInsert(data);
//
//         theStore.StorageFeatures
//             .ShouldContainIndexDefinitionFor<Target>(
//                 indexName: $"mt_doc_target_{frenchRegConfig}_idx_fts",
//                 regConfig: frenchRegConfig,
//                 dataConfig: $"((data ->> '{nameof(Target.String)}'))"
//             );
//
//         theStore.StorageFeatures
//             .ShouldContainIndexDefinitionFor<Target>(
//                 indexName: $"mt_doc_target_{italianRegConfig}_idx_fts",
//                 regConfig: italianRegConfig,
//                 dataConfig: $"((data ->> '{nameof(Target.AnotherString)}'))"
//             );
//     }
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public void using_a_full_text_index_through_attribute_on_class_with_default()
//     {
//         StoreOptions(_ => _.RegisterDocumentType<Book>());
//
//         theStore.BulkInsert(new[]
//         {
//             new Book { Id = Guid.NewGuid(), Author = "test", Information = "test", Title = "test" }
//         });
//
//         theStore.StorageFeatures
//             .ShouldContainIndexDefinitionFor<Book>(
//                 tableName: "full_text_index.mt_doc_book",
//                 indexName: $"mt_doc_book_idx_fts",
//                 regConfig: FullTextIndex.DefaultRegConfig,
//                 dataConfig: $"data"
//             );
//     }
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public void using_a_single_property_full_text_index_through_attribute_with_default()
//     {
//         StoreOptions(_ => _.RegisterDocumentType<UserProfile>());
//
//         theStore.BulkInsert(new[] { new UserProfile { Id = Guid.NewGuid(), Information = "test" } });
//
//         theStore.StorageFeatures
//             .ShouldContainIndexDefinitionFor<UserProfile>(
//                 tableName: "full_text_index.mt_doc_userprofile",
//                 indexName: $"mt_doc_userprofile_idx_fts",
//                 regConfig: FullTextIndex.DefaultRegConfig,
//                 dataConfig: $"((data ->> '{nameof(UserProfile.Information)}'))"
//             );
//     }
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public void using_a_single_property_full_text_index_through_attribute_with_custom_settings()
//     {
//         StoreOptions(_ => _.RegisterDocumentType<UserDetails>());
//
//         theStore.BulkInsert(new[] { new UserDetails { Id = Guid.NewGuid(), Details = "test" } });
//
//         theStore.StorageFeatures
//             .ShouldContainIndexDefinitionFor<UserDetails>(
//                 tableName: "full_text_index.mt_doc_userdetails",
//                 indexName: "mt_custom_user_details_fts_idx",
//                 regConfig: "italian",
//                 dataConfig: $"((data ->> '{nameof(UserDetails.Details)}'))"
//             );
//     }
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public void using_multiple_properties_full_text_index_through_attribute_with_default()
//     {
//         StoreOptions(_ => _.RegisterDocumentType<Article>());
//
//         theStore.BulkInsert(new[] { new Article { Id = Guid.NewGuid(), Heading = "test", Text = "test" } });
//
//         theStore.StorageFeatures
//             .ShouldContainIndexDefinitionFor<Article>(
//                 tableName: "full_text_index.mt_doc_article",
//                 indexName: $"mt_doc_article_idx_fts",
//                 regConfig: FullTextIndex.DefaultRegConfig,
//                 dataConfig: $"((data ->> '{nameof(Article.Heading)}') || ' ' || (data ->> '{nameof(Article.Text)}'))"
//             );
//     }
//
//     [PgVersionTargetedFact(MinimumVersion = "10.0")]
//     public void using_multiple_properties_full_text_index_through_attribute_with_custom_settings()
//     {
//         const string frenchRegConfig = "french";
//         const string italianRegConfig = "italian";
//
//         StoreOptions(_ => _.RegisterDocumentType<BlogPost>());
//
//         theStore.BulkInsert(new[]
//         {
//             new BlogPost
//             {
//                 Id = Guid.NewGuid(),
//                 Category = "test",
//                 EnglishText = "test",
//                 FrenchText = "test",
//                 ItalianText = "test"
//             }
//         });
//
//         theStore.StorageFeatures
//             .ShouldContainIndexDefinitionFor<BlogPost>(
//                 tableName: "full_text_index.mt_doc_blogpost",
//                 indexName: $"mt_doc_blogpost_idx_fts",
//                 regConfig: FullTextIndex.DefaultRegConfig,
//                 dataConfig: $"((data ->> '{nameof(BlogPost.EnglishText)}'))"
//             );
//
//         theStore.StorageFeatures
//             .ShouldContainIndexDefinitionFor<BlogPost>(
//                 tableName: "full_text_index.mt_doc_blogpost",
//                 indexName: $"mt_doc_blogpost_{frenchRegConfig}_idx_fts",
//                 regConfig: frenchRegConfig,
//                 dataConfig: $"((data ->> '{nameof(BlogPost.FrenchText)}'))"
//             );
//
//         theStore.StorageFeatures
//             .ShouldContainIndexDefinitionFor<BlogPost>(
//                 tableName: "full_text_index.mt_doc_blogpost",
//                 indexName: $"mt_doc_blogpost_{italianRegConfig}_idx_fts",
//                 regConfig: italianRegConfig,
//                 dataConfig: $"((data ->> '{nameof(BlogPost.ItalianText)}'))"
//             );
//     }
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
//         const string DataConfig = "(data ->> 'AnotherString' || ' ' || 'test')";
//         const string RegConfig = "french";
//         const string IndexName = "custom_index_name";
//
//         StoreOptions(_ => _.Schema.For<Target>().FullTextIndex(
//             index =>
//             {
//                 index.DataConfig = DataConfig;
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
//                 dataConfig: DataConfig
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
//         string dataConfig = null)
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
//         SpecificationExtensions.ShouldContain(ddl, $"to_tsvector('{regConfig}',{dataConfig})");
//
//         if (regConfig != null)
//         {
//             SpecificationExtensions.ShouldContain(ddl, regConfig);
//         }
//
//         if (dataConfig != null)
//         {
//             SpecificationExtensions.ShouldContain(ddl, dataConfig);
//         }
//     }
// }
