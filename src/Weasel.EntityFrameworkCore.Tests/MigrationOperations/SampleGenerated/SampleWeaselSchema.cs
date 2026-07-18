using Weasel.Core;
using Weasel.EntityFrameworkCore;
using Weasel.Postgresql;
using PgTable = Weasel.Postgresql.Tables.Table;
using PgSequence = Weasel.Postgresql.Sequence;
using PgIndex = Weasel.Postgresql.Tables.IndexDefinition;

namespace Weasel.EntityFrameworkCore.Tests.MigrationOperations.SampleGenerated;

/// <summary>
///     The Weasel schema whose generated migration + stub context are checked in
///     next to this file. The emitter drift-guard test regenerates both and
///     compares byte-for-byte; the integration test applies the checked-in
///     migration through the EF runtime and round-trips it against Weasel's own
///     delta detection.
/// </summary>
public static class SampleWeaselSchema
{
    public const string SchemaName = "efgen";
    public const string MigrationName = "WeaselSampleSchema";
    public const string ContextTypeName = "WeaselSampleDbContext";

    public static readonly DateTime Timestamp = new(2026, 7, 18, 12, 0, 0, DateTimeKind.Utc);

    public static ISchemaObject[] Objects()
    {
        var numbers = new PgSequence(new DbObjectName(SchemaName, "order_numbers"), 1000) { IncrementBy = 10 };

        var customers = new PgTable($"{SchemaName}.customers");
        customers.AddColumn<int>("id").AsPrimaryKey();
        customers.ColumnFor("id")!.IsAutoNumber = true;
        customers.AddColumn<string>("name").NotNull();
        customers.ColumnFor("name")!.DefaultExpression = "'unknown'";
        ((ITable)customers).AddCheckConstraint("ck_customers_name", "length(name) > 0");

        var orders = new PgTable($"{SchemaName}.orders");
        orders.AddColumn<Guid>("id").AsPrimaryKey();
        orders.AddColumn<int>("customer_id").NotNull();
        orders.AddColumn("payload", "jsonb");
        orders.AddColumn<string>("status").NotNull();
        orders.ColumnFor("status")!.DefaultExpression = "'pending'";
        ((ITable)orders).AddForeignKey("fk_orders_customer",
            new DbObjectName(SchemaName, "customers"), new[] { "customer_id" }, new[] { "id" })
            .DeleteAction = Weasel.Core.CascadeAction.Cascade;

        var statusIndex = new PgIndex("idx_orders_status")
        {
            Columns = new[] { "status" }, Predicate = "status <> 'archived'"
        };
        orders.Indexes.Add(statusIndex);

        return new ISchemaObject[] { numbers, customers, orders };
    }

    public static MigrationOperationTranslationOptions TranslationOptions()
        => new(EfMigrationProvider.PostgreSql) { Migrator = new PostgresqlMigrator() };

    public static EfMigrationEmissionOptions EmissionOptions()
        => new(ContextTypeName)
        {
            Namespace = "Weasel.EntityFrameworkCore.Tests.MigrationOperations.SampleGenerated",
            TimestampUtc = Timestamp
        };

    public static EfMigrationFile GenerateMigration()
    {
        var objects = Objects();
        var options = TranslationOptions();
        return EfMigrationFileEmitter.EmitMigration(
            MigrationName,
            objects.ToMigrationOperations(options),
            objects.ToDropMigrationOperations(options),
            EmissionOptions());
    }

    public static string GenerateStubContext()
        => EfMigrationFileEmitter.EmitStubContext(
            EfMigrationProvider.PostgreSql, EmissionOptions(), SchemaName);
}
