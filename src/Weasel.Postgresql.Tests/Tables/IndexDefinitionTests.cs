using System.Collections.Generic;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables
{
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
                .ShouldBe("CREATE INDEX idx_1 ON public.people USING btree (column1);");
        }
        
        [Fact]
        public void write_unique_index()
        {
            theIndex.IsUnique = true;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE UNIQUE INDEX idx_1 ON public.people USING btree (column1);");
        }
        
        [Fact]
        public void write_concurrent_index()
        {
            theIndex.IsConcurrent = true;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE INDEX CONCURRENTLY idx_1 ON public.people USING btree (column1);");
        }

        [Fact]
        public void write_unique_and_concurrent_index()
        {
            theIndex.IsUnique = true;
            theIndex.IsConcurrent = true;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE UNIQUE INDEX CONCURRENTLY idx_1 ON public.people USING btree (column1);");
        }
        
        [Fact]
        public void write_desc()
        {
            theIndex.SortOrder = SortOrder.Desc;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE INDEX idx_1 ON public.people USING btree (column1 DESC);");
        }
        
        [Fact]
        public void write_gin()
        {
            theIndex.Method = IndexMethod.gin;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE INDEX idx_1 ON public.people USING gin (column1);");
        }

        [Fact]
        public void write_with_namespace()
        {
            theIndex.TableSpace = "green";
            theIndex.Method = IndexMethod.gin;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE INDEX idx_1 ON public.people USING gin (column1) TABLESPACE green;");
        }

        [Fact]
        public void with_a_predicate()
        {
            theIndex.TableSpace = "green";
            theIndex.Method = IndexMethod.gin;
            theIndex.Predicate = "foo > 1";
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE INDEX idx_1 ON public.people USING gin (column1) TABLESPACE green WHERE (foo > 1);");
        }

        [Fact]
        public void with_a_non_default_fill_factor()
        {
            theIndex.TableSpace = "green";
            theIndex.Method = IndexMethod.gin;
            theIndex.Predicate = "foo > 1";
            theIndex.FillFactor = 70;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE INDEX idx_1 ON public.people USING gin (column1) TABLESPACE green WHERE (foo > 1) WITH (fillfactor='70');");
        }
        
        [Fact]
        public void generate_ddl_for_descending_sort_order()
        {
            theIndex.SortOrder = SortOrder.Desc;

            theIndex.ToDDL(parent)
                .ShouldBe("CREATE INDEX idx_1 ON public.people USING btree (column1 DESC);");
        }


        public static IEnumerable<object[]> Indexes()
        {
            yield return new[]{new IndexDefinition("idx_1").AgainstColumns("name")};
            yield return new[]{new IndexDefinition("idx_1").AgainstColumns("name", "age")};
            yield return new[]{new IndexDefinition("idx_1")
            {
                IsUnique = true
            }.AgainstColumns("name", "age")};
            
            yield return new[]{new IndexDefinition("idx_1"){SortOrder = SortOrder.Desc}.AgainstColumns("name")};
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

        
    }
}