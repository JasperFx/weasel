using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

using static PostgresqlProvider;

public class IndexDefinitionTests
{
    private IndexDefinition theIndex = new IndexDefinition("idx_1")
        .AgainstColumns("column1");

    private Table parent = new Table("people");


    [InlineData(IndexMethod.btree, true)]
    [InlineData(IndexMethod.gin, false)]
    [InlineData(IndexMethod.brin, false)]
    [InlineData(IndexMethod.gist, false)]
    [InlineData(IndexMethod.hash, false)]
    [Theory]
    public void sort_order_only_applied_for_btree_index(IndexMethod method, bool shouldUseSort)
    {
        theIndex.Method = method;
        theIndex.SortOrder = SortOrder.Desc;

        var ddl = theIndex.ToDDL(parent);

        if (shouldUseSort)
        {
            ddl.ShouldEndWith(" DESC);");
        }
        else
        {
            ddl.ShouldNotEndWith(" DESC);");
        }
    }

    [Fact]
    public void default_sort_order_is_asc()
    {
        theIndex.SortOrder.ShouldBe(SortOrder.Asc);
    }

    [Fact]
    public void default_method_is_btree()
    {
        theIndex.Method.ShouldBe(IndexMethod.btree);
    }

    [Fact]
    public void is_not_unique_by_default()
    {
        theIndex.IsUnique.ShouldBeFalse();
    }

    [Fact]
    public void is_not_concurrent_by_default()
    {
        theIndex.IsConcurrent.ShouldBeFalse();
    }


    [Fact]
    public void write_basic_index()
    {
        theIndex.ToDDL(parent)
            .ShouldBe($"CREATE INDEX idx_1 ON {Instance.Parse("public.people")} USING btree (column1);");
    }

    [Fact]
    public void write_unique_index()
    {
        theIndex.IsUnique = true;

        theIndex.ToDDL(parent)
            .ShouldBe($"CREATE UNIQUE INDEX idx_1 ON {Instance.Parse("public.people")} USING btree (column1);");
    }

    [Fact]
    public void write_concurrent_index()
    {
        theIndex.IsConcurrent = true;

        theIndex.ToDDL(parent)
            .ShouldBe($"--WEASEL_INDEX_CREATION_BEGIN{Environment.NewLine}" +
                      $"CREATE INDEX CONCURRENTLY idx_1 ON {Instance.Parse("public.people")} USING btree (column1);{Environment.NewLine}" +
                      "--WEASEL_INDEX_CREATION_END");
    }

    [Fact]
    public void write_unique_and_concurrent_index()
    {
        theIndex.IsUnique = true;
        theIndex.IsConcurrent = true;

        theIndex.ToDDL(parent)
            .ShouldBe($"--WEASEL_INDEX_CREATION_BEGIN{Environment.NewLine}" +
                      $"CREATE UNIQUE INDEX CONCURRENTLY idx_1 ON {Instance.Parse("public.people")} USING btree (column1);{Environment.NewLine}" +
                      "--WEASEL_INDEX_CREATION_END");
    }

    [Fact]
    public void write_unique_and_concurrent_index_with_distinct_nulls()
    {
        theIndex.IsUnique = true;
        theIndex.NullsNotDistinct = true;
        theIndex.IsConcurrent = true;

        theIndex.ToDDL(parent)
            .ShouldBe($"--WEASEL_INDEX_CREATION_BEGIN{Environment.NewLine}" +
                      $"CREATE UNIQUE INDEX CONCURRENTLY idx_1 ON {Instance.Parse("public.people")} USING btree (column1) NULLS NOT DISTINCT ;{Environment.NewLine}" +
                      "--WEASEL_INDEX_CREATION_END");
    }

    [Fact]
    public void write_desc()
    {
        theIndex.SortOrder = SortOrder.Desc;

        theIndex.ToDDL(parent)
            .ShouldBe($"CREATE INDEX idx_1 ON {Instance.Parse("public.people")} USING btree (column1 DESC);");
    }

    [Fact]
    public void write_gin()
    {
        theIndex.Method = IndexMethod.gin;

        theIndex.ToDDL(parent)
            .ShouldBe($"CREATE INDEX idx_1 ON {Instance.Parse("public.people")} USING gin (column1);");
    }

    [Fact]
    public void write_with_namespace()
    {
        theIndex.TableSpace = "green";
        theIndex.Method = IndexMethod.gin;

        theIndex.ToDDL(parent)
            .ShouldBe($"CREATE INDEX idx_1 ON {Instance.Parse("public.people")} USING gin (column1) TABLESPACE green;");
    }

    [Fact]
    public void with_a_predicate()
    {
        theIndex.TableSpace = "green";
        theIndex.Method = IndexMethod.gin;
        theIndex.Predicate = "foo > 1";

        theIndex.ToDDL(parent)
            .ShouldBe($"CREATE INDEX idx_1 ON {Instance.Parse("public.people")} USING gin (column1) TABLESPACE green WHERE (foo > 1);");
    }

    [Fact]
    public void with_a_non_default_fill_factor()
    {
        theIndex.TableSpace = "green";
        theIndex.Method = IndexMethod.gin;
        theIndex.Predicate = "foo > 1";
        theIndex.FillFactor = 70;

        theIndex.ToDDL(parent)
            .ShouldBe(
                $"CREATE INDEX idx_1 ON {Instance.Parse("public.people")} USING gin (column1) TABLESPACE green WHERE (foo > 1) WITH (fillfactor='70');");
    }

    [Fact]
    public void generate_ddl_for_descending_sort_order()
    {
        theIndex.SortOrder = SortOrder.Desc;

        theIndex.ToDDL(parent)
            .ShouldBe($"CREATE INDEX idx_1 ON {Instance.Parse("public.people")} USING btree (column1 DESC);");
    }


    public static IEnumerable<object[]> Indexes()
    {
        yield return new[] { new IndexDefinition("idx_1").AgainstColumns("name") };
        yield return new[] { new IndexDefinition("idx_1").AgainstColumns("name", "age") };
        yield return new[] { new IndexDefinition("idx_1") { IsUnique = true }.AgainstColumns("name", "age") };
        yield return new[] { new IndexDefinition("idx_1") { IsUnique = true, NullsNotDistinct = true}.AgainstColumns("name", "age") };

        yield return new[] { new IndexDefinition("idx_1") { SortOrder = SortOrder.Desc }.AgainstColumns("name") };
    }

    [Theory]
    [MemberData(nameof(Indexes))]
    public void IndexParsing(IndexDefinition expected)
    {
        var table = new Table("people");
        var ddl = expected.ToDDL(table);

        var actual = IndexDefinition.Parse(ddl);

        expected.AssertMatches(actual, table);
    }

    [Fact]
    public void Can_roundtrip_from_instance_to_ddl_and_back()
    {
        // (data->'RoleIds' jsonb_path_ops)

        var expected = new IndexDefinition("idx_1").AgainstColumns("data->'RoleIds'");
        expected.ToGinWithJsonbPathOps();

        var table = new Table("test");
        var ddl = expected.ToDDL(table);
        var parsed = IndexDefinition.Parse(ddl);

        expected.AssertMatches(parsed, table);

        parsed.Columns.Single().ShouldBe(expected.Columns.Single());
        parsed.Mask.ShouldBe(expected.Mask);
    }

    [Theory]
    [InlineData("column1 <> column2", "column1 != column2")]
    [InlineData("column1 != column2", "column1 <> column2")]
    [InlineData("column1 <> column2", "column1 <> column2")]
    [InlineData("column1 != column2", "column1 != column2")]
    public void canonicalize_not_equals(string actual, string expected)
    {
        actual.CanonicizeSql().ShouldBe(expected.CanonicizeSql());
    }

    [Theory]
    [InlineData("column1", "column1")]
    [InlineData("column1::uuid", "CAST(column1 as uuid)")]
    [InlineData("CAST(data ->> 'SomeGuid' as uuid)", "CAST(data ->> 'SomeGuid' as uuid)")]
    [InlineData("((data ->> 'SomeGuid'))::uuid", "CAST(data ->> 'SomeGuid' as uuid)")]
    [InlineData("((data  ->> 'SomeGuid'))::uuid", "CAST(data ->> 'SomeGuid' as uuid)")]
    [InlineData("(data  ->> 'SomeGuid')::uuid", "CAST(data ->> 'SomeGuid' as uuid)")]
    public void canonicize_cast(string actual, string expected)
    {
        IndexDefinition.CanonicizeCast(actual).ShouldBe(expected);
    }

    [Fact]
    public void Bug30()
    {
        var table = new Table("mt_doc_user");
        var index1 = IndexDefinition.Parse(
            "CREATE INDEX idx_1 ON public.mt_doc_user USING gin ((data->'RoleIds') jsonb_path_ops)");
        var index2 = IndexDefinition.Parse(
            "CREATE INDEX idx_1 ON public.mt_doc_user USING gin ((data -> 'RoleIds') jsonb_path_ops)");

        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void ensure_proper_delta_check_for_full_text_index_definition_between_db_and_instance()
    {
        var table = new Table("mt_doc_user");
        // full text index as fetched from db schema
        // note that the value from db has some differences
        // 1) it has `::regconfig`
        // 2) right number of brackets
        // 3) right spacing between terms
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX mt_doc_user_idx_fts ON fulltext.mt_doc_user USING gin (to_tsvector('english'::regconfig, data))");

        // full text index created by our system
        // system generated has the following
        // does not contain ::regconfig
        // has more brackets and additional spacing between terms
        var index2 =
            IndexDefinition.Parse(
                "CREATE INDEX mt_doc_user_idx_fts ON fulltext.mt_doc_user USING gin (( to_tsvector('english', data) ))");

        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }


    [Fact]
    public void ensure_proper_delta_check_for_full_text_index_definition_with_multiple_columns_between_db_and_instance()
    {
        var table = new Table("mt_doc_user");
        // full text index as fetched from db schema
        // note that the value from db has some differences
        // 1) it has `::regconfig`
        // 2) right number of brackets
        // 3) right spacing between terms
        // 4) has casting to text for columns
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX mt_doc_user_idx_fts ON full_text_index.mt_doc_user USING gin (to_tsvector('english'::regconfig, (((data ->> 'FirstName'::text) || ' '::text) || (data ->> 'LastName'::text))))");

        // full text index created by our system
        // system generated has the following
        // does not contain ::regconfig
        // has more brackets and additional spacing between terms
        var index2 =
            IndexDefinition.Parse(
                "CREATE INDEX mt_doc_user_idx_fts ON full_text_index.mt_doc_user USING gin (to_tsvector('english',((data ->> 'FirstName') || ' ' || (data ->> 'LastName'))))");

        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void Bug36_custom_storage_parameters_and_custom_method()
    {
        var table = new Table("mt_doc_user");

        var ddl =
            "CREATE INDEX idx_1 ON public.mt_doc_user USING pgroonga ((data->'RoleIds') pgroonga_text_full_text_search_ops_v2) WITH (normalizers='NormalizerNFKC100(\"unify_kana\", true)', tokenizer='MeCab', test = 1)";
        var index = IndexDefinition.Parse(ddl);
        var expectedCanonicalizedDdl = IndexDefinition.CanonicizeDdl(index, table);

        index.Method.ShouldBe(IndexMethod.custom);
        index.CustomMethod.ShouldBe("pgroonga");
        index.StorageParameters.Count.ShouldBe(3);
        index.StorageParameters["normalizers"].ShouldBe("NormalizerNFKC100(\"unify_kana\", true)");

        var canonicalizedDdl = IndexDefinition.CanonicizeDdl(index, table);
        canonicalizedDdl.ShouldBe(expectedCanonicalizedDdl);
    }

    [Fact]
    public void ensure_include_columns_are_handled_properly()
    {
        var table = new Table("mt_doc_user");
        var index1 = IndexDefinition.Parse(
            "CREATE INDEX idx_1 ON public.mt_doc_user USING gin ((data->'RoleIds') jsonb_path_ops INCLUDE (column2))");

        var index2 = new IndexDefinition("idx_1");
        index2.Columns = new[] { "data->'RoleIds'" };
        index2.ToGinWithJsonbPathOps();
        index2.IncludeColumns = new[] { "column2" };

        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void ensure_tablespace_is_handled_properly()
    {
        var table = new Table("mt_doc_user");
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX idx_1 ON public.mt_doc_user USING gin ((data->'RoleIds') jsonb_path_ops) TABLESPACE pg_default;");

        var index2 = new IndexDefinition("idx_1") { Columns = new[] { "data->'RoleIds'" } };
        index2.ToGinWithJsonbPathOps();
        index2.TableSpace = "pg_default";

        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void ensure_asc_sort_order_nulls_default_is_handled_properly()
    {
        var table = new Table("mt_doc_user");
        // NULLS LAST is explicitly added for test even though it is default for test
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX idx_1 ON mt_doc_user USING btree (column1 ASC NULLS LAST);");

        // NULLS LAST not added here
        var index2 = new IndexDefinition("idx_1") { Columns = new[] { "column1" } };
        index2.Method = IndexMethod.btree;

        index1.ToDDL(parent).ShouldNotContain("ASC");
        index2.ToDDL(parent).ShouldNotContain("ASC");
        index1.ToDDL(parent).ShouldNotContain("ASC NULLS LAST");
        index2.ToDDL(parent).ShouldNotContain("ASC NULLS LAST");
        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void ensure_desc_sort_order_nulls_default_is_handled_properly()
    {
        var table = new Table("mt_doc_user");
        // NULLS FIRST is explicitly added even though it is default
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX idx_1 ON mt_doc_user USING btree (mt_immutable_timestamp(data ->> 'ArrivalDate'::text) DESC NULLS FIRST);");

        // NULLS FIRST not added here
        var index2 = new IndexDefinition("idx_1")
        {
            Columns = new[] { "mt_immutable_timestamp(data ->> 'ArrivalDate'::text)" }
        };
        index2.Method = IndexMethod.btree;
        index2.SortOrder = SortOrder.Desc;

        index1.ToDDL(parent).ShouldContain("DESC");
        index2.ToDDL(parent).ShouldContain("DESC");
        index1.ToDDL(parent).ShouldNotContain("DESC NULLS FIRST");
        index2.ToDDL(parent).ShouldNotContain("DESC NULLS FIRST");
        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void ensure_asc_sort_order_nulls_is_handled_properly()
    {
        var table = new Table("mt_doc_user");
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX idx_1 ON mt_doc_user USING btree (column1 ASC NULLS FIRST);");

        var index2 = new IndexDefinition("idx_1") { Columns = new[] { "column1" } };
        index2.Method = IndexMethod.btree;
        index2.NullsSortOrder = NullsSortOrder.First;

        index1.ToDDL(parent).ShouldNotContain("ASC");
        index2.ToDDL(parent).ShouldNotContain("ASC");
        index1.ToDDL(parent).ShouldContain("NULLS FIRST");
        index2.ToDDL(parent).ShouldContain("NULLS FIRST");
        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void ensure_sort_order_nulls_first_without_asc_is_handled_properly()
    {
        var table = new Table("mt_doc_user");
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX idx_1 ON mt_doc_user USING btree (column1 NULLS FIRST);");

        var index2 = new IndexDefinition("idx_1") { Columns = new[] { "column1" } };
        index2.Method = IndexMethod.btree;
        index2.NullsSortOrder = NullsSortOrder.First;

        index1.ToDDL(parent).ShouldNotContain("ASC");
        index2.ToDDL(parent).ShouldNotContain("ASC");
        index1.ToDDL(parent).ShouldContain("NULLS FIRST");
        index2.ToDDL(parent).ShouldContain("NULLS FIRST");
        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void ensure_sort_order_nulls_last_without_asc_is_handled_properly()
    {
        var table = new Table("mt_doc_user");
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX idx_1 ON mt_doc_user USING btree (column1 NULLS LAST);");

        var index2 = new IndexDefinition("idx_1") { Columns = new[] { "column1" } };
        index2.Method = IndexMethod.btree;
        index2.NullsSortOrder = NullsSortOrder.Last;

        index1.ToDDL(parent).ShouldNotContain("ASC");
        index2.ToDDL(parent).ShouldNotContain("ASC");
        index1.ToDDL(parent).ShouldNotContain("ASC NULLS LAST");
        index2.ToDDL(parent).ShouldNotContain("ASC NULLS LAST");
        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void ensure_desc_sort_order_nulls_is_handled_properly()
    {
        var table = new Table("mt_doc_user");
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX idx_1 ON mt_doc_user USING btree (column1 DESC NULLS LAST);");

        var index2 = new IndexDefinition("idx_1") { Columns = new[] { "column1" } };
        index2.Method = IndexMethod.btree;
        index2.SortOrder = SortOrder.Desc;
        index2.NullsSortOrder = NullsSortOrder.Last;

        index1.ToDDL(parent).ShouldContain("DESC NULLS LAST");
        index2.ToDDL(parent).ShouldContain("DESC NULLS LAST");
        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void gin_index_with_fastupdate_on()
    {
        var table = new Table("mt_doc_user");
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX idx_1 ON public.mt_doc_user USING gin (column1) WITH (fastupdate=on);");

        var index2 = new IndexDefinition("idx_1") { Columns = new[] { "column1" } };
        index2.Method = IndexMethod.gin;
        index2.StorageParameters.Add("FASTUPDATE", "ON");

        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void ensure_collation_is_handled_properly()
    {
        var table = new Table("mt_doc_user");
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX idx_1 ON public.mt_doc_user USING btree (column1 COLLATE \"de_DE\");");

        var index2 = new IndexDefinition("idx_1") { Columns = new[] { "column1" } };
        index2.Method = IndexMethod.btree;
        index2.Collation = "de_DE";

        index1.ToDDL(parent).ShouldContain("COLLATE \"de_DE\"");
        index2.ToDDL(parent).ShouldContain("COLLATE \"de_DE\"");

        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void ensure_handling_of_index_def_without_using_clause()
    {
        var table = new Table("mt_doc_user");
        // index does not contain using clause
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX idx_1 ON public.mt_doc_user (column1);");

        var index2 = new IndexDefinition("idx_1") { Columns = new[] { "column1" } };
        index2.Method = IndexMethod.btree;

        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void ensure_handling_of_index_def_with_column_name_using_pg_keyword()
    {
        var table = new Table("mt_doc_user");
        // index does not contain using clause
        var index1 =
            IndexDefinition.Parse(
                "CREATE INDEX idx_1 ON public.mt_doc_user using btree (\"time\" desc);");

        var index2 = new IndexDefinition("idx_1")
        {
            Columns = new[] { "time" }, Method = IndexMethod.btree, SortOrder = SortOrder.Desc
        };

        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Theory]
    [InlineData("((  data->'RoleIds') jsonb_path_ops)", "((data->'RoleIds') jsonb_path_ops)")]
    [InlineData("((data->'RoleIds'  ) jsonb_path_ops)", "((data->'RoleIds') jsonb_path_ops)")]
    [InlineData("( (  data->'RoleIds'  ) jsonb_path_ops  )", "((data->'RoleIds') jsonb_path_ops)")]
    public void ensure_proper_delta_check_for_parentheses_with_spaces(string exprParenthesesWithSpaces, string exprParenthesesWithoutSpaces)
    {
        var table = new Table("mt_doc_user");
        var index1 = IndexDefinition.Parse(
            $"CREATE INDEX idx_1 ON public.mt_doc_user USING gin {exprParenthesesWithSpaces}");
        var index2 = IndexDefinition.Parse(
            $"CREATE INDEX idx_1 ON public.mt_doc_user USING gin {exprParenthesesWithoutSpaces}");

        IndexDefinition.CanonicizeDdl(index1, table).ShouldBe(IndexDefinition.CanonicizeDdl(index2, table));
    }

    [Fact]
    public void nulls_not_distinct_should_generate_correct_index()
    {
        var index = new IndexDefinition("index")
        {
            Columns = new[]
            {
                "column1",
                "column2"
            },
            IsUnique = true,
            NullsNotDistinct = true
        };

        var ddl = index.ToDDL(new Table("table"));
        ddl.ShouldBe($"CREATE UNIQUE INDEX index ON {Instance.Parse("public.table")} USING btree (column1, column2) NULLS NOT DISTINCT ;");
    }

    [Fact]
    public void should_be_able_to_parse_index_with_nulls_not_distinct()
    {
        var index = IndexDefinition.Parse($"CREATE UNIQUE INDEX index ON {Instance.Parse("public.table")} USING btree (column1, column2) NULLS NOT DISTINCT;");
        index.IsUnique.ShouldBeTrue();
        index.NullsNotDistinct.ShouldBeTrue();

        index = IndexDefinition.Parse("CREATE UNIQUE INDEX index ON public.table USING btree (column1, column2) NULLS DISTINCT;");
        index.IsUnique.ShouldBeTrue();
        index.NullsNotDistinct.ShouldBeFalse();
    }
}
