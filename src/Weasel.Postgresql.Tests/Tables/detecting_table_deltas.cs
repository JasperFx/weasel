using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Baseline;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables
{
    [Collection("deltas")]
    public class detecting_table_deltas : IntegrationContext
    {
        private Table theTable;
        
        /*
         * TODO
         * 1. Column constraints, to find deltas
         * 5. Table constraints?
         * 6. Partitions?
         *
         *
         * 
         */
        
        public detecting_table_deltas() : base("deltas")
        {
            theTable = new Table("deltas.people");
            theTable.AddColumn<int>("id").AsPrimaryKey();
            theTable.AddColumn<string>("first_name");
            theTable.AddColumn<string>("last_name");
            theTable.AddColumn<string>("user_name");
            theTable.AddColumn("data", "jsonb");
            
            
            ResetSchema().GetAwaiter().GetResult();
        }
        
        protected async Task AssertNoDeltasAfterPatching(Table table = null)
        {
            table ??= theTable;
            await table.ApplyChanges(theConnection);

            var delta = await table.FindDelta(theConnection);
            
            delta.HasChanges().ShouldBeFalse();
        }

        [Fact]
        public async Task no_delta()
        {
            await CreateSchemaObjectInDatabase(theTable);

            var delta = await theTable.FindDelta(theConnection);

            delta.HasChanges().ShouldBeFalse();
        }

        [Fact]
        public async Task missing_column()
        {
            await CreateSchemaObjectInDatabase(theTable);

            theTable.AddColumn<DateTimeOffset>("birth_day");
            var delta = await theTable.FindDelta(theConnection);
            delta.HasChanges().ShouldBeTrue();
            
            delta.Columns.Missing.Single().Name.ShouldBe("birth_day");
            
            delta.Columns.Extras.Any().ShouldBeFalse();
            delta.Columns.Different.Any().ShouldBeFalse();

            await AssertNoDeltasAfterPatching();
        }
        
        [Fact]
        public async Task extra_column()
        {
            theTable.AddColumn<DateTime>("birth_day");
            await CreateSchemaObjectInDatabase(theTable);

            theTable.RemoveColumn("birth_day");
            
            var delta = await theTable.FindDelta(theConnection);
            delta.HasChanges().ShouldBeTrue();
            
            delta.Columns.Extras.Single().Name.ShouldBe("birth_day");
            
            delta.Columns.Missing.Any().ShouldBeFalse();
            delta.Columns.Different.Any().ShouldBeFalse();
            
            await AssertNoDeltasAfterPatching();
        }
        

        [Fact]
        public async Task detect_new_index()
        {
            await CreateSchemaObjectInDatabase(theTable);

            theTable.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);
            
            var delta = await theTable.FindDelta(theConnection);
            delta.HasChanges().ShouldBeTrue();
            
            delta.Indexes.Missing.Single()
                .Name.ShouldBe("idx_people_user_name");
            
            await AssertNoDeltasAfterPatching();

        }
        
        [Fact]
        public async Task detect_matched_index()
        {
            theTable.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);

            await CreateSchemaObjectInDatabase(theTable);


            var delta = await theTable.FindDelta(theConnection);
            delta.HasChanges().ShouldBeFalse();
            
            delta.Indexes.Matched.Single()
                .Name.ShouldBe("idx_people_user_name");

            await AssertNoDeltasAfterPatching();
        }
        
        [Fact]
        public async Task detect_different_index()
        {
            theTable.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);

            await CreateSchemaObjectInDatabase(theTable);

            theTable.Indexes.Single().As<IndexDefinition>()
                .Method = IndexMethod.hash;

            var delta = await theTable.FindDelta(theConnection);
            delta.HasChanges().ShouldBeTrue();
            
            delta.Indexes.Different.Single()
                .Expected
                .Name.ShouldBe("idx_people_user_name");

            await AssertNoDeltasAfterPatching();
        }

        [Fact]
        public async Task detect_extra_index()
        {

            theTable.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);
            await CreateSchemaObjectInDatabase(theTable);

            theTable.Indexes.Clear();
            
            var delta = await theTable.FindDelta(theConnection);
            delta.HasChanges().ShouldBeTrue();
            
            delta.Indexes.Extras.Single().Name
                .ShouldBe("idx_people_user_name");
            
            await AssertNoDeltasAfterPatching();
        }


        [Theory]
        [MemberData(nameof(IndexTestData))]
        public async Task matching_index_ddl(string description, Action<Table> configure)
        {
            configure(theTable);
            await CreateSchemaObjectInDatabase(theTable);

            var existing = await theTable.FetchExisting(theConnection);
            
            // The index DDL should match what the database thinks it is in order to match
            theTable.Indexes.Single().ToDDL(theTable)
                .Replace("INDEX CONCURRENTLY", "INDEX")
                
                .ShouldBe(existing.Indexes.Single().ToDDL(theTable));

            // And no deltas
            var delta = await theTable.FindDelta(theConnection);
            delta.Indexes.Matched.Count.ShouldBe(1);

        }

        public static IEnumerable<object[]> IndexTestData()
        {
            foreach (var (description, action) in IndexConfigs())
            {
                yield return new object[] {description, action};
            }
        }

        private static IEnumerable<(string, Action<Table>)> IndexConfigs()
        {
            yield return ("Simple btree", t => t.ModifyColumn("user_name").AddIndex());
            yield return ("Simple btree with expression", t => t.ModifyColumn("user_name").AddIndex(i => i.Expression = "(lower(user_name))"));
            yield return ("Simple btree with expression and predicate", t => t.ModifyColumn("user_name").AddIndex(i =>
            {
                i.Expression = "(lower(user_name))";
                i.Predicate = "id > 5";
            }));
            
            
            
            yield return ("Simple btree + desc", t => t.ModifyColumn("user_name").AddIndex(i => i.SortOrder = SortOrder.Desc));
            yield return ("btree + unique", t => t.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true));
            yield return ("btree + concurrent", t => t.ModifyColumn("user_name").AddIndex(i =>
            {
                i.IsConcurrent = true;

            }));
            
            yield return ("btree + concurrent + unique", t => t.ModifyColumn("user_name").AddIndex(i =>
            {
                i.IsUnique = true;
                i.IsConcurrent = true;
            }));
            
            yield return ("Simple brin", t => t.ModifyColumn("user_name").AddIndex(i => i.Method = IndexMethod.brin));
            yield return ("Simple gin", t => t.ModifyColumn("data").AddIndex(i => i.Method = IndexMethod.gin));
            yield return ("Simple gist", t => t.AddColumn("data2", "tsvector").AddIndex(i => i.Method = IndexMethod.gist));
            yield return ("Simple hash", t => t.ModifyColumn("user_name").AddIndex(i => i.Method = IndexMethod.hash));
            
            
        }


        [Fact]
        public async Task detect_all_new_foreign_key()
        {
            var states = new Table("deltas.states");
            states.AddColumn<int>("id").AsPrimaryKey();
            
            await CreateSchemaObjectInDatabase(states);
            
            
            var table = new Table("deltas.people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");

            await CreateSchemaObjectInDatabase(table);
            
            table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

            var delta = await table.FindDelta(theConnection);
            
            delta.HasChanges().ShouldBeTrue();
            
            delta.ForeignKeys.Missing.Single()
                .ShouldBeSameAs(table.ForeignKeys.Single());

            await AssertNoDeltasAfterPatching();
        }
        
        [Fact]
        public async Task detect_extra_foreign_key()
        {
            var states = new Table("deltas.states");
            states.AddColumn<int>("id").AsPrimaryKey();
            
            await CreateSchemaObjectInDatabase(states);
            
            
            var table = new Table("deltas.people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");
            
            table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");


            await CreateSchemaObjectInDatabase(table);
            
            table.ForeignKeys.Clear();
            
            var delta = await table.FindDelta(theConnection);
            
            delta.HasChanges().ShouldBeTrue();
            
            delta.ForeignKeys.Extras.Single().Name
                .ShouldBe("fkey_people_state_id");

            await AssertNoDeltasAfterPatching();
        }
        
                
        [Fact]
        public async Task match_foreign_key()
        {
            var states = new Table("deltas.states");
            states.AddColumn<int>("id").AsPrimaryKey();
            
            await CreateSchemaObjectInDatabase(states);
            
            
            var table = new Table("deltas.people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");
            
            table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");


            await CreateSchemaObjectInDatabase(table);
            

            var delta = await table.FindDelta(theConnection);
            
            delta.HasChanges().ShouldBeFalse();
            
            delta.ForeignKeys.Matched.Single().Name
                .ShouldBe("fkey_people_state_id");

            await AssertNoDeltasAfterPatching();
        }
        
                
                
        [Fact]
        public async Task different_foreign_key()
        {
            var states = new Table("deltas.states");
            states.AddColumn<int>("id").AsPrimaryKey();
            
            await CreateSchemaObjectInDatabase(states);
            
            
            var table = new Table("deltas.people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");
            
            table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");


            await CreateSchemaObjectInDatabase(table);

            table.ForeignKeys.Single().OnDelete = CascadeAction.Cascade;

            var delta = await table.FindDelta(theConnection);
            
            delta.HasChanges().ShouldBeTrue();
            
            delta.ForeignKeys.Different.Single().Actual.Name
                .ShouldBe("fkey_people_state_id");

            await AssertNoDeltasAfterPatching();
        }

        
        

    }
}