using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
/// Tests for GitHub issue #45: PK columns with FK constraints don't migrate.
/// When a table's primary key is dropped and recreated (CASCADE drops referencing FKs),
/// the migration must recreate the FKs from other tables afterwards.
/// </summary>
[Collection("pk_fk_migration")]
public class pk_migration_with_referencing_fks : IntegrationContext
{
    public pk_migration_with_referencing_fks() : base("pk_fk_migration")
    {
    }

    public override async Task InitializeAsync()
    {
        await ResetSchema();
    }

    [Fact]
    public async Task can_change_pk_columns_when_fk_references_all_pk_columns()
    {
        // Parent with single-column PK
        var parent = new Table(new PostgresqlObjectName(SchemaName, "accounts"));
        parent.AddColumn<int>("id").AsPrimaryKey();
        parent.AddColumn<string>("name").NotNull();

        // Child FK references parent's full PK
        var child = new Table(new PostgresqlObjectName(SchemaName, "transactions"));
        child.AddColumn<int>("id").AsPrimaryKey();
        child.AddColumn<int>("account_id").NotNull().ForeignKeyTo(parent, "id");
        child.AddColumn<decimal>("amount");

        var migrator = new PostgresqlMigrator();
        if (theConnection.State != System.Data.ConnectionState.Open) await theConnection.OpenAsync();
        await theConnection.EnsureSchemaExists(SchemaName);
        var migration1 = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, parent, child);
        await migrator.ApplyAllAsync(theConnection, migration1, AutoCreate.CreateOrUpdate);

        // Now change parent PK to (id, name) — composite
        // The child FK stays on (id) alone, so we need a unique constraint on id
        // to satisfy PostgreSQL. But the PK change forces a DROP CASCADE + recreate.
        var parentV2 = new Table(new PostgresqlObjectName(SchemaName, "accounts"));
        parentV2.AddColumn<int>("id").AsPrimaryKey();
        parentV2.AddColumn<string>("name").AsPrimaryKey(); // added to PK

        // Add a unique index on id alone so the FK can still reference it
        var uniqueIdx = new IndexDefinition("ix_accounts_id") { IsUnique = true };
        uniqueIdx.Columns = new[] { "id" };
        parentV2.Indexes.Add(uniqueIdx);

        var childV2 = new Table(new PostgresqlObjectName(SchemaName, "transactions"));
        childV2.AddColumn<int>("id").AsPrimaryKey();
        childV2.AddColumn<int>("account_id").NotNull().ForeignKeyTo(parentV2, "id");
        childV2.AddColumn<decimal>("amount");

        // This should succeed — PK change + FK recreation
        var migration2 = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, parentV2, childV2);
        migration2.Difference.ShouldNotBe(SchemaPatchDifference.None);
        await migrator.ApplyAllAsync(theConnection, migration2, AutoCreate.CreateOrUpdate);

        // Verify clean state
        var finalMigration = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, parentV2, childV2);
        finalMigration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task can_rename_pk_constraint_with_referencing_fk()
    {
        var parent = new Table(new PostgresqlObjectName(SchemaName, "parent_rename"));
        parent.AddColumn<int>("id").AsPrimaryKey();
        parent.AddColumn<string>("name").NotNull();
        parent.PrimaryKeyName = "pk_parent_old";

        var child = new Table(new PostgresqlObjectName(SchemaName, "child_rename"));
        child.AddColumn<int>("id").AsPrimaryKey();
        child.AddColumn<int>("parent_id").NotNull().ForeignKeyTo(parent, "id");

        var migrator = new PostgresqlMigrator();
        if (theConnection.State != System.Data.ConnectionState.Open) await theConnection.OpenAsync();
        await theConnection.EnsureSchemaExists(SchemaName);
        var migration1 = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, parent, child);
        await migrator.ApplyAllAsync(theConnection, migration1, AutoCreate.CreateOrUpdate);

        // Change only the PK name
        var parentV2 = new Table(new PostgresqlObjectName(SchemaName, "parent_rename"));
        parentV2.AddColumn<int>("id").AsPrimaryKey();
        parentV2.AddColumn<string>("name").NotNull();
        parentV2.PrimaryKeyName = "pk_parent_new";

        var childV2 = new Table(new PostgresqlObjectName(SchemaName, "child_rename"));
        childV2.AddColumn<int>("id").AsPrimaryKey();
        childV2.AddColumn<int>("parent_id").NotNull().ForeignKeyTo(parentV2, "id");

        var migration2 = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, parentV2, childV2);
        await migrator.ApplyAllAsync(theConnection, migration2, AutoCreate.CreateOrUpdate);

        var finalMigration = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, parentV2, childV2);
        finalMigration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task ddl_output_includes_fk_recreation_after_pk_change()
    {
        var parent = new Table(new PostgresqlObjectName(SchemaName, "ddl_parent"));
        parent.AddColumn<int>("id").AsPrimaryKey();
        parent.AddColumn<string>("code");

        var child = new Table(new PostgresqlObjectName(SchemaName, "ddl_child"));
        child.AddColumn<int>("id").AsPrimaryKey();
        child.AddColumn<int>("parent_id").NotNull().ForeignKeyTo(parent, "id");

        var migrator = new PostgresqlMigrator();
        if (theConnection.State != System.Data.ConnectionState.Open) await theConnection.OpenAsync();
        await theConnection.EnsureSchemaExists(SchemaName);
        var migration1 = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, parent, child);
        await migrator.ApplyAllAsync(theConnection, migration1, AutoCreate.CreateOrUpdate);

        // Change PK — keep same column but add unique index so FK still works
        var parentV2 = new Table(new PostgresqlObjectName(SchemaName, "ddl_parent"));
        parentV2.AddColumn<int>("id").AsPrimaryKey();
        parentV2.AddColumn<string>("code").AsPrimaryKey(); // added to PK
        var uniqueIdx = new IndexDefinition("ix_ddl_parent_id") { IsUnique = true };
        uniqueIdx.Columns = new[] { "id" };
        parentV2.Indexes.Add(uniqueIdx);

        var childV2 = new Table(new PostgresqlObjectName(SchemaName, "ddl_child"));
        childV2.AddColumn<int>("id").AsPrimaryKey();
        childV2.AddColumn<int>("parent_id").NotNull().ForeignKeyTo(parentV2, "id");

        var migration2 = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, parentV2, childV2);

        var writer = new StringWriter();
        migration2.WriteAllUpdates(writer, migrator, AutoCreate.CreateOrUpdate);
        var ddl = writer.ToString();

        // Should contain: PK drop, PK add, FK recreation
        ddl.ShouldContain("drop constraint");
        ddl.ShouldContain("PRIMARY KEY");
        ddl.ShouldContain("FOREIGN KEY");
    }
}
