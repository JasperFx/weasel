using JasperFx;
using JasperFx.Descriptors;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Core.Tests.Migrations;

/// <summary>
///     The seams a provider whose scripts and schemas differ from the rest -- Firebird's -- needs, held
///     here so the other providers are seen to keep what they always wrote.
/// </summary>
public class schema_script_hooks
{
    /// <summary>
    ///     The fingerprint table is <c>{DefaultSchemaName}.weasel_schema_fingerprints</c> for every
    ///     provider but Firebird, whose database has no schema to qualify it with.
    /// </summary>
    [Theory]
    [InlineData("Postgresql", "public.weasel_schema_fingerprints")]
    [InlineData("SqlServer", "dbo.weasel_schema_fingerprints")]
    [InlineData("Sqlite", "main.weasel_schema_fingerprints")]
    [InlineData("MySql", "public.weasel_schema_fingerprints")]
    [InlineData("Oracle", "WEASEL.weasel_schema_fingerprints")]
    [InlineData("Firebird", "weasel_schema_fingerprints")]
    public void the_fingerprint_table_is_named_by_the_migrator(string provider, string expected)
    {
        Migrator migrator = provider switch
        {
            "Postgresql" => new Postgresql.PostgresqlMigrator(),
            "SqlServer" => new SqlServer.SqlServerMigrator(),
            "Sqlite" => new Sqlite.SqliteMigrator(),
            "MySql" => new MySql.MySqlMigrator(),
            "Oracle" => new Oracle.OracleMigrator(),
            "Firebird" => new Firebird.FirebirdMigrator(),
            _ => throw new ArgumentOutOfRangeException(nameof(provider))
        };

        migrator.FingerprintTableName("weasel_schema_fingerprints").ShouldBe(expected);
    }

    /// <summary>
    ///     A rollback, and a migration file's drop script, undo the deltas in the order the migrator
    ///     asks for. Every provider but Firebird keeps the order they were applied in; Firebird will not
    ///     drop a table a view still uses, so it undoes them last to first.
    /// </summary>
    [Theory]
    [InlineData("Postgresql", "DROP first;DROP second;")]
    [InlineData("SqlServer", "DROP first;DROP second;")]
    [InlineData("Sqlite", "DROP first;DROP second;")]
    [InlineData("MySql", "DROP first;DROP second;")]
    [InlineData("Oracle", "DROP first;DROP second;")]
    [InlineData("Firebird", "DROP second;DROP first;")]
    public void rollbacks_are_written_in_the_order_the_migrator_asks_for(string provider, string expected)
    {
        Migrator migrator = provider switch
        {
            "Postgresql" => new Postgresql.PostgresqlMigrator(),
            "SqlServer" => new SqlServer.SqlServerMigrator(),
            "Sqlite" => new Sqlite.SqliteMigrator(),
            "MySql" => new MySql.MySqlMigrator(),
            "Oracle" => new Oracle.OracleMigrator(),
            "Firebird" => new Firebird.FirebirdMigrator(),
            _ => throw new ArgumentOutOfRangeException(nameof(provider))
        };

        var migration = new SchemaMigration([
            new SchemaObjectDelta(new Dropped("first"), SchemaPatchDifference.Create),
            new SchemaObjectDelta(new Dropped("second"), SchemaPatchDifference.Create)
        ]);

        var writer = new StringWriter();
        migration.WriteAllRollbacks(writer, migrator);

        writer.ToString().ShouldBe(expected);
    }

    /// <summary>
    ///     An object whose drop statement names it, and nothing else.
    /// </summary>
    private sealed class Dropped(string name): ISchemaObject
    {
        public DbObjectName Identifier { get; } = new("public", name);

        public void WriteCreateStatement(Migrator migrator, TextWriter writer) => writer.Write($"CREATE {name};");

        public void WriteDropStatement(Migrator rules, TextWriter writer) => writer.Write($"DROP {name};");

        public void ConfigureQueryCommand(DbCommandBuilder builder) => throw new NotSupportedException();

        public Task<ISchemaObjectDelta> CreateDeltaAsync(System.Data.Common.DbDataReader reader,
            CancellationToken ct = default) => throw new NotSupportedException();

        public IEnumerable<DbObjectName> AllNames() => [Identifier];
    }

    /// <summary>
    ///     A migrator shapes a script through the writer it hands the step. <c>ToDatabaseScript</c> used
    ///     to write straight past it into the outer writer, which left Firebird's script without the
    ///     COMMIT it needs after every statement.
    /// </summary>
    [Fact]
    public void a_database_script_is_written_through_the_writer_the_migrator_hands_the_step()
    {
        var script = new ScriptedDatabase().ToDatabaseScript();

        script.ShouldStartWith("<<");
        script.TrimEnd().ShouldEndWith(">>");
        script.ShouldContain("CREATE TABLE");
        script.IndexOf("CREATE TABLE", StringComparison.Ordinal).ShouldBeGreaterThan(script.IndexOf("<<", StringComparison.Ordinal));
    }

    /// <summary>
    ///     A migrator that passes its step a writer of its own and brackets what comes back.
    /// </summary>
    private sealed class BracketingMigrator: Sqlite.SqliteMigrator
    {
        public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
        {
            var inner = new StringWriter();
            writeStep(this, inner);

            writer.WriteLine("<<");
            writer.Write(inner.ToString());
            writer.WriteLine(">>");
        }
    }

    private sealed class ScriptedDatabase(): DatabaseBase<SqliteConnection>(new DefaultMigrationLogger(),
        AutoCreate.None, new BracketingMigrator(), "scripted", "Data Source=:memory:")
    {
        public override IFeatureSchema[] BuildFeatureSchemas()
        {
            var table = new Sqlite.Tables.Table("things");
            table.AddColumn<int>("id").AsPrimaryKey();

            return [new Feature(Migrator, table)];
        }

        public override DatabaseDescriptor Describe() => new() { Engine = "test", DatabaseName = "scripted" };
    }

    private sealed class Feature(Migrator migrator, ISchemaObject schemaObject): FeatureSchemaBase("things", migrator)
    {
        protected override IEnumerable<ISchemaObject> schemaObjects() => [schemaObject];
    }
}
