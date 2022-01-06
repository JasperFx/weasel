using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Baseline;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Migrations
{
    [Collection("migrations")]
    public class SchemaMigrationTests : IntegrationContext, IAsyncLifetime
    {
        private readonly DatabaseWithTables theDatabase;

        public SchemaMigrationTests() : base("migrations")
        {
            theDatabase = new DatabaseWithTables(AutoCreate.None, "Migrations");
        }

        public override Task InitializeAsync()
        {
            return ResetSchema();
        }

        [Fact]
        public async Task assert_valid_change_with_only_creation_deltas()
        {
            theDatabase.Features["One"].AddTable(SchemaName, "one");
            await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

            // Add a new table here.
            theDatabase.Features["One"].AddTable(SchemaName, "two");

            var migration = await theDatabase.CreateMigrationAsync();
            migration.Difference.ShouldBe(SchemaPatchDifference.Create);

            // Always good!
            migration.AssertPatchingIsValid(AutoCreate.All);
            migration.AssertPatchingIsValid(AutoCreate.None); // Even though nothing would ever happen

            // Also good
            migration.AssertPatchingIsValid(AutoCreate.CreateOnly);
            migration.AssertPatchingIsValid(AutoCreate.CreateOrUpdate);


        }

        [Fact]
        public async Task assert_valid_change_with_an_update_deltas()
        {
            var table = theDatabase.Features["One"].AddTable(SchemaName, "one");
            await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

            // Add a new table here.
            table.AddColumn<string>("description");

            var migration = await theDatabase.CreateMigrationAsync();
            migration.Difference.ShouldBe(SchemaPatchDifference.Update);

            // Always good!
            migration.AssertPatchingIsValid(AutoCreate.All);
            migration.AssertPatchingIsValid(AutoCreate.None); // Even though nothing would ever happen

            // Also good
            migration.AssertPatchingIsValid(AutoCreate.CreateOrUpdate);

            // Not good!
            Should.Throw<SchemaMigrationException>(() => migration.AssertPatchingIsValid(AutoCreate.CreateOnly));
        }
    }

    public class NamedTable: Table
    {
        public NamedTable(DbObjectName name) : base(name)
        {
            AddColumn<Guid>("id").AsPrimaryKey();
            AddColumn<string>("name");
        }
    }

    public class NamedTableFeature: FeatureSchemaBase
    {
        public Dictionary<string, Table> Tables { get; } = new();

        public NamedTableFeature(string identifier, Migrator migrator) : base(identifier, migrator)
        {
        }

        public Table AddTable(string schemaName, string tableName)
        {
            var table = new NamedTable(new DbObjectName(schemaName, tableName));
            Tables[table.Identifier.QualifiedName] = table;

            return table;
        }

        protected override IEnumerable<ISchemaObject> schemaObjects()
        {
            return Tables.Values;
        }
    }

    public class DatabaseWithTables: PostgresqlDatabase
    {
        public DatabaseWithTables(AutoCreate autoCreate, string identifier) : base(new DefaultMigrationLogger(), autoCreate, new PostgresqlMigrator(), identifier, ConnectionSource.ConnectionString)
        {
        }

        public LightweightCache<string, NamedTableFeature> Features { get; } =
            new LightweightCache<string, NamedTableFeature>(name =>
                new NamedTableFeature(name, new PostgresqlMigrator()));

        public override IFeatureSchema[] BuildFeatureSchemas()
        {
            return Features.GetAll().OfType<IFeatureSchema>().ToArray();
        }
    }
}
