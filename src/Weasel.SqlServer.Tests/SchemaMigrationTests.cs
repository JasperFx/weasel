using System.IO;
using System.Linq;
using NSubstitute;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests
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

        [Fact]
        public void difference_is_none_by_default()
        {
            var migration = migrationFor(); // no objects
            migration.Difference.ShouldBe(SchemaPatchDifference.None);
        }

        [Fact]
        public void none_if_no_changes_detected()
        {
            var migration = migrationFor(SchemaPatchDifference.None, SchemaPatchDifference.None);
            migration.Difference.ShouldBe(SchemaPatchDifference.None);
        }


        [Fact]
        public void translates_the_file_name()
        {
            SchemaMigration.ToDropFileName("update.sql").ShouldBe("update.drop.sql");
            SchemaMigration.ToDropFileName("1.update.sql").ShouldBe("1.update.drop.sql");
            SchemaMigration.ToDropFileName("folder\\1.update.sql").ShouldBe("folder\\1.update.drop.sql");
        }


        [Fact]
        public void writing_out_updates_when_schema_object_is_invalid()
        {
            var migration = migrationFor(SchemaPatchDifference.Invalid);

            var writer = new StringWriter();
            var delta = migration.Deltas.Single();

            var rules = new SqlServerMigrator();
            migration.WriteAllUpdates(writer, rules, AutoCreate.All);

            delta.DidNotReceive().WriteUpdate(rules, writer);
            delta.SchemaObject.Received().WriteDropStatement(rules, writer);
            delta.SchemaObject.Received().WriteCreateStatement(rules, writer);
        }

        [Fact]
        public void writing_out_updates_when_schema_object_is_create()
        {
            var migration = migrationFor(SchemaPatchDifference.Create);

            var writer = new StringWriter();
            var delta = migration.Deltas.Single();

            var rules = new SqlServerMigrator();
            migration.WriteAllUpdates(writer, rules, AutoCreate.All);

            delta.DidNotReceive().WriteUpdate(rules, writer);
            delta.SchemaObject.DidNotReceive().WriteDropStatement(rules, writer);
            delta.SchemaObject.Received().WriteCreateStatement(rules, writer);
        }

        [Fact]
        public void writing_out_updates_when_schema_object_is_update()
        {
            var migration = migrationFor(SchemaPatchDifference.Update);

            var writer = new StringWriter();
            var delta = migration.Deltas.Single();

            var rules = new SqlServerMigrator();
            migration.WriteAllUpdates(writer, rules, AutoCreate.All);

            delta.Received().WriteUpdate(rules, writer);
            delta.SchemaObject.DidNotReceive().WriteDropStatement(rules, writer);
            delta.SchemaObject.DidNotReceive().WriteCreateStatement(rules, writer);
        }


    }


}
