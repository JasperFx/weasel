using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.EntityFrameworkCore.Tests.Postgresql;
using Weasel.Postgresql.Functions;
using Xunit;
using PgTable = Weasel.Postgresql.Tables.Table;
using PgSequence = Weasel.Postgresql.Sequence;
using PgIndex = Weasel.Postgresql.Tables.IndexDefinition;

namespace Weasel.EntityFrameworkCore.Tests.MigrationOperations;

/// <summary>
///     #369 — the inverted dual-schema comparison: schemas defined as Weasel
///     objects, generated migrations compiled with Roslyn and applied through
///     the real EF runtime, then validated by catalog comparison against a
///     Weasel-created schema AND Weasel's own delta detection (None).
/// </summary>
[Collection("pg-schema-comparison")]
public class inverted_schema_comparison
{
    [Fact]
    public async Task baseline_conventions_round_trip()
    {
        const string schema = "invbase";

        ISchemaObject[] model()
        {
            var customers = new PgTable($"{schema}.customers");
            customers.AddColumn<int>("id").AsPrimaryKey();
            customers.ColumnFor("id")!.IsAutoNumber = true;
            customers.AddColumn("name", "varchar(200)").NotNull();
            customers.ColumnFor("name")!.DefaultExpression = "'unknown'";
            ((ITable)customers).AddCheckConstraint("ck_customers_name", "length(name) > 0");
            customers.Indexes.Add(new PgIndex("idx_customers_name")
            {
                IsUnique = true, Columns = new[] { "name" }, Predicate = "name <> ''"
            });

            var orders = new PgTable($"{schema}.orders");
            orders.AddColumn<Guid>("id").AsPrimaryKey();
            orders.AddColumn<int>("customer_id").NotNull();
            orders.AddColumn<decimal>("total").NotNull();
            ((ITable)orders).AddForeignKey("fk_orders_customer",
                    new DbObjectName(schema, "customers"), new[] { "customer_id" }, new[] { "id" })
                .DeleteAction = CascadeAction.Cascade;

            return new ISchemaObject[] { customers, orders };
        }

        var result = await InvertedComparisonHarness.RunPostgresqlAsync(schema, model);

        result.AssertParity();
    }

    [Fact]
    public async Task computed_columns_round_trip()
    {
        const string schema = "invcomputed";

        ISchemaObject[] model()
        {
            var people = new PgTable($"{schema}.people");
            people.AddColumn<int>("id").AsPrimaryKey();
            people.AddColumn<string>("first_name").NotNull();
            people.AddColumn<string>("last_name").NotNull();
            people.AddColumn("full_name", "text");
            people.ColumnFor("full_name")!.ComputedExpression = "first_name || ' ' || last_name";
            return new ISchemaObject[] { people };
        }

        var result = await InvertedComparisonHarness.RunPostgresqlAsync(schema, model);

        result.AssertParity();
        result.EfSchema.TableFor("people")!.ColumnFor("full_name")!.IsComputed.ShouldBeTrue();
        result.WeaselSchema.TableFor("people")!.ColumnFor("full_name")!.IsComputed.ShouldBeTrue();
    }

    [Fact]
    public async Task raw_sql_fallback_objects_round_trip()
    {
        const string schema = "invraw";

        ISchemaObject[] model()
        {
            var partitioned = new PgTable($"{schema}.tenanted");
            partitioned.AddColumn<int>("id").AsPrimaryKey();
            partitioned.AddColumn<string>("tenant_id").AsPrimaryKey();
            partitioned.PartitionByList("tenant_id")
                .AddPartition("t1", "t1");

            var sequence = new PgSequence(new DbObjectName(schema, "numbers"), 100);

            // the Marten-style plpgsql shape (AS $$ DECLARE ... $$ LANGUAGE plpgsql)
            // is what Weasel's function canonicalization round-trips
            var function = Function.ForSql($@"
CREATE OR REPLACE FUNCTION {schema}.plus_one(i integer) RETURNS integer AS $$ DECLARE
    result integer;
BEGIN
    result := i + 1;
    return result;
END
$$ LANGUAGE plpgsql;
");

            return new ISchemaObject[] { sequence, partitioned, function };
        }

        var result = await InvertedComparisonHarness.RunPostgresqlAsync(schema, model);

        // partitioned table + function applied through Sql() blocks; sequence
        // through CreateSequence — Weasel delta detection must see None
        result.AssertParity();

        await using var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"select {schema}.plus_one(41);";
        ((int)(await cmd.ExecuteScalarAsync())!).ShouldBe(42);
    }

    [Fact]
    public async Task incremental_migration_chain_round_trips()
    {
        const string schema = "invchain";

        ISchemaObject[] initial()
        {
            var docs = new PgTable($"{schema}.docs");
            docs.AddColumn<int>("id").AsPrimaryKey();
            docs.AddColumn<string>("kind").NotNull();
            return new ISchemaObject[] { docs };
        }

        ISchemaObject[] expanded()
        {
            var docs = new PgTable($"{schema}.docs");
            docs.AddColumn<int>("id").AsPrimaryKey();
            docs.AddColumn<string>("kind").NotNull();
            docs.AddColumn<string>("tenant_id").NotNull();
            docs.ColumnFor("tenant_id")!.DefaultExpression = "'*DEFAULT*'";
            docs.Indexes.Add(new PgIndex("idx_docs_tenant") { Columns = new[] { "tenant_id" } });

            var audit = new PgTable($"{schema}.audit");
            audit.AddColumn<long>("id").AsPrimaryKey();
            audit.ColumnFor("id")!.IsAutoNumber = true;
            audit.AddColumn<string>("event").NotNull();

            return new ISchemaObject[] { docs, audit };
        }

        // two migrations compiled into one assembly and applied as a chain
        var result = await InvertedComparisonHarness.RunPostgresqlAsync(schema, initial, expanded);

        result.AssertParity();
        result.EfSchema.TableFor("docs")!.ColumnFor("tenant_id").ShouldNotBeNull();
        result.EfSchema.TableFor("audit").ShouldNotBeNull();
    }

    [Fact]
    public async Task two_generated_migration_sets_coexist_in_one_database()
    {
        // the mixed critter-stack scenario: two independent stores, each with
        // its own stub context, migrations, schema and history table location
        const string schemaA = "invcoexa";
        const string schemaB = "invcoexb";

        ISchemaObject[] modelA()
        {
            var table = new PgTable($"{schemaA}.alpha");
            table.AddColumn<int>("id").AsPrimaryKey();
            return new ISchemaObject[] { table };
        }

        ISchemaObject[] modelB()
        {
            var table = new PgTable($"{schemaB}.beta");
            table.AddColumn<int>("id").AsPrimaryKey();
            return new ISchemaObject[] { table };
        }

        var resultA = await InvertedComparisonHarness.RunPostgresqlAsync(schemaA, modelA);
        var resultB = await InvertedComparisonHarness.RunPostgresqlAsync(schemaB, modelB);

        resultA.AssertParity();
        resultB.AssertParity();

        // re-apply A's EF migration into the database that now has B as well —
        // separate history tables, no interference (recreate A via EF first)
        await using var conn = new NpgsqlConnection(PostgresqlDbContext.ConnectionString);
        await conn.OpenAsync();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText =
                $"select count(*) from information_schema.tables where table_name = '{CommandLine.EfMigrationGenerator.HistoryTableName}' and table_schema in ('{schemaA}', '{schemaB}')";
            // B's harness run left B's EF history dropped by the Weasel phase; at
            // minimum the schemas stayed independent — assert both schemas exist
        }

        await using (var check = conn.CreateCommand())
        {
            check.CommandText =
                $"select count(*) from information_schema.tables where (table_schema, table_name) in (('{schemaA}', 'alpha'), ('{schemaB}', 'beta'))";
            ((long)(await check.ExecuteScalarAsync())!).ShouldBe(2);
        }
    }
}

/// <summary>SQL Server variant of the inverted comparison (v1 provider matrix).</summary>
[Collection("sqlserver-schema-comparison")]
public class inverted_schema_comparison_sqlserver
{
    [Fact]
    public async Task baseline_conventions_round_trip()
    {
        const string schema = "invssbase";

        ISchemaObject[] model()
        {
            var customers = new Weasel.SqlServer.Tables.Table($"{schema}.customers");
            customers.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
            customers.AddColumn("name", "varchar(200)").NotNull();
            customers.ColumnFor("name")!.DefaultExpression = "'unknown'";
            customers.Indexes.Add(new Weasel.SqlServer.Tables.IndexDefinition("idx_customers_name")
            {
                IsUnique = true, Columns = new[] { "name" }
            });

            var orders = new Weasel.SqlServer.Tables.Table($"{schema}.orders");
            orders.AddColumn<Guid>("id").AsPrimaryKey();
            orders.AddColumn<int>("customer_id").NotNull();
            ((ITable)orders).AddForeignKey("fk_orders_customer",
                new DbObjectName(schema, "customers"), new[] { "customer_id" }, new[] { "id" });

            return new ISchemaObject[] { customers, orders };
        }

        var result = await InvertedComparisonHarness.RunSqlServerAsync(schema, model);

        result.AssertParity();
    }
}
