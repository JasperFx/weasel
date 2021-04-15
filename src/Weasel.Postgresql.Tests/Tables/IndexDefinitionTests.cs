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
                .ShouldBe("CREATE INDEX idx_1 ON public.people USING btree (column1) ASC");
        }
        
        [Fact]
        public void write_unique_index()
        {
            theIndex.IsUnique = true;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE UNIQUE INDEX idx_1 ON public.people USING btree (column1) ASC");
        }
        
        [Fact]
        public void write_concurrent_index()
        {
            theIndex.IsConcurrent = true;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE INDEX CONCURRENTLY idx_1 ON public.people USING btree (column1) ASC");
        }

        [Fact]
        public void write_unique_and_concurrent_index()
        {
            theIndex.IsUnique = true;
            theIndex.IsConcurrent = true;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE UNIQUE INDEX CONCURRENTLY idx_1 ON public.people USING btree (column1) ASC");
        }
        
        [Fact]
        public void write_desc()
        {
            theIndex.SortOrder = SortOrder.Desc;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE INDEX idx_1 ON public.people USING btree (column1) DESC");
        }
        
        [Fact]
        public void write_gin()
        {
            theIndex.Method = IndexMethod.gin;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE INDEX idx_1 ON public.people USING gin (column1) ASC");
        }

        [Fact]
        public void write_with_namespace()
        {
            theIndex.TableSpace = "green";
            theIndex.Method = IndexMethod.gin;
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE INDEX idx_1 ON public.people USING gin (column1) ASC TABLESPACE green");
        }

        [Fact]
        public void with_a_predicate()
        {
            theIndex.TableSpace = "green";
            theIndex.Method = IndexMethod.gin;
            theIndex.Predicate = "foo > 1";
            
            theIndex.ToDDL(parent)
                .ShouldBe("CREATE INDEX idx_1 ON public.people USING gin (column1) ASC TABLESPACE green WHERE foo > 1");
        }

        
    }
}