using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Tables;
using Xunit;
using JasperFx.Core;
using static System.Threading.Tasks.Task;

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
        public async Task calling_apply_will_override_AutoCreate_none()
        {
            // Just expressing the pre-condition
            theDatabase.AutoCreate.ShouldBe(AutoCreate.None);

            theDatabase.Features["One"].AddTable(SchemaName, "one");
            await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

            await theDatabase.AssertDatabaseMatchesConfigurationAsync();
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

        [Fact]
        public async Task assert_reconnection_succeeds()
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
        public static DatabaseWithTables ForConnectionString(string connectionString)
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionString);
            var identifier = builder.Database;

            return new DatabaseWithTables(identifier, connectionString);
        }

        public DatabaseWithTables(AutoCreate autoCreate, string identifier)
            : base(new DefaultMigrationLogger(), autoCreate, new PostgresqlMigrator(), identifier, ConnectionSource.ConnectionString)
        {
        }

        public DatabaseWithTables(string identifier, string connectionString)
            : base(new DefaultMigrationLogger(), AutoCreate.All, new PostgresqlMigrator(), identifier, connectionString)
        {
        }

        public LightweightCache<string, NamedTableFeature> Features { get; } =
            new LightweightCache<string, NamedTableFeature>(name =>
                new NamedTableFeature(name, new PostgresqlMigrator()));

        public override IFeatureSchema[] BuildFeatureSchemas()
        {
            return Features.OfType<IFeatureSchema>().ToArray();
        }
    }

    public class FlakyConnectionGlobalLock: IGlobalLock<NpgsqlConnection>
    {
        public bool failedAlread = false;
        public int ApplyChangesLockId = 4004;

        public async Task<AttainLockResult> TryAttainLock(NpgsqlConnection conn, CancellationToken ct = default)
        {
            if (!failedAlread)
            {
                // this will make AdminShutdown exception while trying to get global lock
                await conn.CreateCommand($"SELECT pg_terminate_backend({conn.ProcessID})").ExecuteNonQueryAsync(ct);
            }

            var result = await conn.TryGetGlobalLock(ApplyChangesLockId, cancellation: ct).ConfigureAwait(false);

            if (result != AttainLockResult.Failure)
                return result;

            await Delay(50, ct).ConfigureAwait(false);
            result = await conn.TryGetGlobalLock(ApplyChangesLockId, cancellation: ct).ConfigureAwait(false);

            if (result != AttainLockResult.Failure)
                return result;

            await Delay(100, ct).ConfigureAwait(false);
            result = await conn.TryGetGlobalLock(ApplyChangesLockId, cancellation: ct).ConfigureAwait(false);

            if (result != AttainLockResult.Failure)
                return result;

            await Delay(250, ct).ConfigureAwait(false);
            result = await conn.TryGetGlobalLock(ApplyChangesLockId, cancellation: ct).ConfigureAwait(false);

            return result;
        }

        public Task ReleaseLock(NpgsqlConnection conn, CancellationToken ct = default)
        {
            throw new NotImplementedException();
        }
    }
}
