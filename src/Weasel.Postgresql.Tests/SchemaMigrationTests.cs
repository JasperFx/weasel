using System.IO;
using System.Linq;
using NSubstitute;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    public class SchemaMigrationTests
    {
        private ISchemaObjectDelta deltaFor(SchemaPatchDifference difference)
        {
            var delta = Substitute.For<ISchemaObjectDelta>();
            delta.Difference.Returns(difference);

            var schemaObject = Substitute.For<ISchemaObject>();
            delta.SchemaObject.Returns(schemaObject);

            return delta;
        }

        private SchemaMigration migrationFor(params SchemaPatchDifference[] differences)
        {
            var deltas = differences.Select(deltaFor).ToList();
            return new SchemaMigration(deltas);
        }


        [Theory]
        [InlineData(AutoCreate.All, new []{SchemaPatchDifference.None, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.All, new []{SchemaPatchDifference.Invalid, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.All, new []{SchemaPatchDifference.Create, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.All, new []{SchemaPatchDifference.Update, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.None, new []{SchemaPatchDifference.None, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.None, new []{SchemaPatchDifference.Invalid, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.None, new []{SchemaPatchDifference.Create, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.None, new []{SchemaPatchDifference.Update, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.CreateOnly, new []{SchemaPatchDifference.None, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.CreateOnly, new []{SchemaPatchDifference.Create, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.CreateOnly, new []{SchemaPatchDifference.Create, SchemaPatchDifference.Create, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.CreateOrUpdate, new []{SchemaPatchDifference.None, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.CreateOrUpdate, new []{SchemaPatchDifference.Create, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.CreateOrUpdate, new []{SchemaPatchDifference.Update, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.CreateOrUpdate, new []{SchemaPatchDifference.Update, SchemaPatchDifference.Create, SchemaPatchDifference.None})]
        public void valid_asserts(AutoCreate autoCreate, SchemaPatchDifference[] differences)
        {
            var migration = migrationFor(differences); 
            migration.AssertPatchingIsValid(autoCreate); // nothing thrown
        }
        
        [Theory]
        [InlineData(AutoCreate.CreateOnly, new []{SchemaPatchDifference.Update, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.CreateOnly, new []{SchemaPatchDifference.Invalid, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.CreateOrUpdate, new []{SchemaPatchDifference.Invalid, SchemaPatchDifference.None, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.CreateOrUpdate, new []{SchemaPatchDifference.Create, SchemaPatchDifference.Invalid, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.CreateOrUpdate, new []{SchemaPatchDifference.Update, SchemaPatchDifference.Invalid, SchemaPatchDifference.None})]
        [InlineData(AutoCreate.CreateOrUpdate, new []{SchemaPatchDifference.Update, SchemaPatchDifference.Create, SchemaPatchDifference.Invalid})]
        public void invalid_asserts(AutoCreate autoCreate, SchemaPatchDifference[] differences)
        {
            var migration = migrationFor(differences);
            Should.Throw<SchemaMigrationException>(() =>
            {
                migration.AssertPatchingIsValid(autoCreate);
            });

        }
        
        
        
    }


}