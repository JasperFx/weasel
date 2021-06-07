using System.Threading.Tasks;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;
using Xunit.Sdk;

namespace Weasel.Postgresql.Tests.Tables
{
    [Collection("defaults")]
    public class using_default_values_in_tables : IntegrationContext
    {
        public using_default_values_in_tables() : base("defaults")
        {
        }

        [Fact]
        public async Task default_string_value()
        {
            await ResetSchema();
            
            var states = new Table("defaults.states");
            states.AddColumn<int>("id").AsPrimaryKey().Serial();
            states.AddColumn<string>("name").NotNull();
            states.AddColumn<string>("abbr").NotNull().DefaultValueByString("XX");

            await CreateSchemaObjectInDatabase(states);

            await theConnection.CreateCommand("insert into defaults.states (name) values ('Texas')")
                .ExecuteNonQueryAsync();

            var abbr = await theConnection.CreateCommand("select abbr from defaults.states limit 1").FetchOne<string>();

            abbr.ShouldBe("XX");
        }
        
        [Fact]
        public async Task default_int_value()
        {
            await ResetSchema();
            
            var states = new Table("defaults.states");
            states.AddColumn<int>("id").AsPrimaryKey().Serial();
            states.AddColumn<string>("name").NotNull();
            states.AddColumn<int>("number").NotNull().DefaultValue(5);

            await CreateSchemaObjectInDatabase(states);

            await theConnection.CreateCommand("insert into defaults.states (name) values ('Texas')")
                .ExecuteNonQueryAsync();

            var abbr = await theConnection.CreateCommand("select number from defaults.states limit 1").FetchOne<int>();

            abbr.ShouldBe(5);
        }
        
        [Fact]
        public async Task default_long_value()
        {
            await ResetSchema();
            
            var states = new Table("defaults.states");
            states.AddColumn<int>("id").AsPrimaryKey().Serial();
            states.AddColumn<string>("name").NotNull();
            states.AddColumn<long>("number").NotNull().DefaultValue(5L);

            await CreateSchemaObjectInDatabase(states);

            await theConnection.CreateCommand("insert into defaults.states (name) values ('Texas')")
                .ExecuteNonQueryAsync();

            var abbr = await theConnection.CreateCommand("select number from defaults.states limit 1").FetchOne<long>();

            abbr.ShouldBe(5);
        }
        
        [Fact]
        public async Task default_double_value()
        {
            await ResetSchema();
            
            var states = new Table("defaults.states");
            states.AddColumn<int>("id").AsPrimaryKey().Serial();
            states.AddColumn<string>("name").NotNull();
            states.AddColumn<double>("number").NotNull().DefaultValue(5.5);

            await CreateSchemaObjectInDatabase(states);

            await theConnection.CreateCommand("insert into defaults.states (name) values ('Texas')")
                .ExecuteNonQueryAsync();

            var abbr = await theConnection.CreateCommand("select number from defaults.states limit 1")
                .FetchOne<double>();

            abbr.ShouldBe(5.5);
        }
        

        [Fact]
        public async Task default_sequence_value()
        {
            await ResetSchema();
            
            var sequence = new Sequence("defaults.seq1");
            await CreateSchemaObjectInDatabase(sequence);
            
            var states = new Table("defaults.states");
            states.AddColumn<int>("id").AsPrimaryKey().Serial();
            states.AddColumn<string>("name").NotNull();
            states.AddColumn<int>("number").NotNull().DefaultValueFromSequence(sequence);

            await CreateSchemaObjectInDatabase(states);
            
            await theConnection.CreateCommand("insert into defaults.states (name) values ('Texas')")
                .ExecuteNonQueryAsync();

            var abbr = await theConnection.CreateCommand("select number from defaults.states limit 1").FetchOne<int>();

            abbr.ShouldBe(1);
        }


    }
}