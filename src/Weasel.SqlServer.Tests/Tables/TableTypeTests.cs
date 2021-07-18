using System;
using System.Threading.Tasks;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables
{
    [Collection("table_types")]
    public class TableTypeTests : IntegrationContext
    {
        public TableTypeTests() : base("table_types")
        {
        }

        [Fact]
        public async Task create_table_type()
        {
            await ResetSchema();

            var type = new TableType(new DbObjectName("table_types", "EnvelopeIdList"));
            type.AddColumn<Guid>("ID");

            await type.Create(theConnection);
        }

        [Fact]
        public async Task fetch_existing_when_it_does_not_exist()
        {
            await ResetSchema();

            var dbObjectName = new DbObjectName("table_types", "EnvelopeIdList");
            var type = new TableType(dbObjectName);
            type.AddColumn<Guid>("ID");

            var existing = await type.FetchExisting(theConnection);
            existing.ShouldBeNull();
        }
        
        [Fact]
        public async Task fetch_existing_when_it_does_exist()
        {
            await ResetSchema();

            var dbObjectName = new DbObjectName("table_types", "EnvelopeIdList");
            var type = new TableType(dbObjectName);
            type.AddColumn<Guid>("ID");
            
            await type.Create(theConnection);

            var existing = await type.FetchExisting(theConnection);
            existing.ShouldNotBeNull();
            existing.Columns.Count.ShouldBe(1);
            existing.Columns[0].Name.ShouldBe("ID");
        }
        
        [Fact]
        public async Task fetch_delta_when_does_not_exist()
        {
            await ResetSchema();

            var dbObjectName = new DbObjectName("table_types", "EnvelopeIdList");
            var type = new TableType(dbObjectName);
            type.AddColumn<Guid>("ID");
            
            var delta = await type.FindDelta(theConnection);
            delta.Difference.ShouldBe(SchemaPatchDifference.Create);
        }
        
        
        [Fact]
        public async Task apply_new_delta()
        {
            await ResetSchema();

            var dbObjectName = new DbObjectName("table_types", "EnvelopeIdList");
            var type = new TableType(dbObjectName);
            type.AddColumn<Guid>("ID");

            await type.ApplyChanges(theConnection);
            
            var delta = await type.FindDelta(theConnection);
            delta.Difference.ShouldBe(SchemaPatchDifference.None);
        }
        
        

        
        [Fact]
        public async Task fetch_delta_with_no_differences()
        {
            await ResetSchema();

            var dbObjectName = new DbObjectName("table_types", "EnvelopeIdList");
            var type = new TableType(dbObjectName);
            type.AddColumn<Guid>("ID");
            
            await type.Create(theConnection);

            var delta = await type.FindDelta(theConnection);
            delta.Difference.ShouldBe(SchemaPatchDifference.None);
        }
        
        [Fact]
        public async Task fetch_delta_with_no_differences_2()
        {
            await ResetSchema();

            var dbObjectName = new DbObjectName("table_types", "EnvelopeIdList");
            var type = new TableType(dbObjectName);
            type.AddColumn<Guid>("ID");
            type.AddColumn("name", "varchar");
            
            await type.Create(theConnection);

            var delta = await type.FindDelta(theConnection);
            delta.Difference.ShouldBe(SchemaPatchDifference.None);
        }


        [Fact]
        public async Task drop_table_type()
        {
            await ResetSchema();

            var dbObjectName = new DbObjectName("table_types", "EnvelopeIdList");
            var type = new TableType(dbObjectName);
            type.AddColumn<Guid>("ID");
            
            await type.Create(theConnection);

            await type.Drop(theConnection);
            
            var delta = await type.FindDelta(theConnection);
            delta.Difference.ShouldBe(SchemaPatchDifference.Create);
        }
        
        
        [Fact]
        public async Task fetch_delta_with_different_columns()
        {
            await ResetSchema();

            var dbObjectName = new DbObjectName("table_types", "EnvelopeIdList");
            var type = new TableType(dbObjectName);
            type.AddColumn<Guid>("ID");
            
            await type.Create(theConnection);

            type.Columns[0].DatabaseType = "varchar";

            var delta = await type.FindDelta(theConnection);
            delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        }
        
                
        [Fact]
        public async Task fetch_delta_with_different_columns_2()
        {
            await ResetSchema();

            var dbObjectName = new DbObjectName("table_types", "EnvelopeIdList");
            var type = new TableType(dbObjectName);
            type.AddColumn<Guid>("ID");
            
            await type.Create(theConnection);

            type.Columns[0].AllowNulls = !type.Columns[0].AllowNulls;

            var delta = await type.FindDelta(theConnection);
            delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        }
        
                
        [Fact]
        public async Task fetch_delta_with_different_columns_3()
        {
            await ResetSchema();

            var dbObjectName = new DbObjectName("table_types", "EnvelopeIdList");
            var type = new TableType(dbObjectName);
            type.AddColumn<Guid>("ID");
            
            
            await type.Create(theConnection);
            
            type.AddColumn<string>("name");

            var delta = await type.FindDelta(theConnection);
            delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        }
        
                
        [Fact]
        public async Task apply_update_delta()
        {
            await ResetSchema();

            var dbObjectName = new DbObjectName("table_types", "EnvelopeIdList");
            var type = new TableType(dbObjectName);
            type.AddColumn<Guid>("ID");
            
            
            await type.Create(theConnection);
            
            type.AddColumn("name", "varchar");

            
            await type.ApplyChanges(theConnection);
            
            var delta = await type.FindDelta(theConnection);
            
            delta.Difference.ShouldBe(SchemaPatchDifference.None);
        }
    }
}