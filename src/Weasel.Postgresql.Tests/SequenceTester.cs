using System.IO;
using System.Threading.Tasks;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    [Collection("sequences")]
    public class SequenceTester : IntegrationContext
    {
        private readonly Sequence theSequence = new(new DbObjectName("sequences", "mysequence"));

        public SequenceTester() : base("sequences")
        {
        }


        [Fact]
        public async Task can_create_sequence_without_blowing_up()
        {
            await theConnection.OpenAsync();
            await theConnection.ResetSchema("sequences");

            await theSequence.Create(theConnection);
        }
        
        
        [Fact]
        public async Task can_create_with_startup_sequence_without_blowing_up()
        {
            var sequence = new Sequence(new DbObjectName("sequences", "seq1"), 5);
            
            await theConnection.OpenAsync();
            await theConnection.ResetSchema("sequences");

            await sequence.Create(theConnection);
        }


        [Fact]
        public async Task determine_that_it_is_missing()
        {
            await theConnection.OpenAsync();
            await theConnection.ResetSchema("sequences");

            var patch = await theSequence.CreatePatch(theConnection);

            patch.Difference.ShouldBe(SchemaPatchDifference.Create);
        }

        [Fact]
        public async Task determine_that_it_is_already_there()
        {
            await can_create_sequence_without_blowing_up();

            var patch = await theSequence.CreatePatch(theConnection);

            patch.Difference.ShouldBe(SchemaPatchDifference.None);
        }

        [Fact]
        public async Task create_and_drop()
        {
            await theConnection.OpenAsync();
            await theConnection.ResetSchema("sequences");

            await theSequence.Create(theConnection);
            
            var patch = await theSequence.CreatePatch(theConnection);
            patch.Difference.ShouldBe(SchemaPatchDifference.None);

            await theSequence.Drop(theConnection);
            
            patch = await theSequence.CreatePatch(theConnection);

            patch.Difference.ShouldBe(SchemaPatchDifference.Create);
        }

    }
}