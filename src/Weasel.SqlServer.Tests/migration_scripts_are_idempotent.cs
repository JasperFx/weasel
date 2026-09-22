using System.Text.RegularExpressions;
using JasperFx;
using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer.Procedures;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     The issue as reported (weasel#593), end to end: a whole migration script -- schema, tables,
///     indexes, a foreign key, a table type and a stored procedure -- rendered into one file, has to
///     run as written, and running it a second time has to leave the database exactly as the first
///     run did rather than raising "there is already an object named ...".
/// </summary>
public class migration_scripts_are_idempotent: IntegrationContext
{
    private const string SchemaName = "gh593";

    public migration_scripts_are_idempotent(): base(SchemaName)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await ResetSchema();
    }

    [Fact]
    public async Task the_rendered_script_runs_twice_and_matches_the_configuration()
    {
        var database = buildDatabase();

        var migration = await database.CreateMigrationAsync();
        migration.Difference.ShouldBe(SchemaPatchDifference.Create);

        var script = renderScript(database, migration);

        await assertMigrationFileMatches(database, migration, script);
        assertProcedureIsBatchSeparated(script);

        await executeInBatches(script);
        await executeInBatches(script);

        await database.AssertDatabaseMatchesConfigurationAsync();

        var afterwards = await database.CreateMigrationAsync();
        afterwards.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The negative control for the splitter. <c>GO</c> is a sqlcmd directive rather than T-SQL,
    ///     so the same script handed to SqlClient whole is rejected before anything runs. This is the
    ///     failure the issue reported and the reason every rendered-DDL executor splits.
    /// </summary>
    [Fact]
    public async Task the_same_script_is_rejected_when_it_is_not_split_into_batches()
    {
        var database = buildDatabase();

        var migration = await database.CreateMigrationAsync();
        var script = renderScript(database, migration);

        var exception = await Should.ThrowAsync<SqlException>(
            async () => await theConnection.CreateCommand(script).ExecuteNonQueryAsync());

        exception.Message.ShouldContain("GO");
    }

    private static MigrationDatabase buildDatabase()
    {
        var database = new MigrationDatabase("gh593", ConnectionSource.ConnectionString);

        var parent = new Table(new SqlServerObjectName(SchemaName, "parent"));
        parent.AddColumn<Guid>("id").AsPrimaryKey();
        parent.AddColumn<string>("name").NotNull();
        database.AddTable(parent);

        var child = new Table(new SqlServerObjectName(SchemaName, "child"));
        child.AddColumn<Guid>("id").AsPrimaryKey();
        child.AddColumn<string>("email").NotNull().AddIndex(x =>
        {
            x.Name = "idx_child_email";
            x.IsUnique = true;
        });
        child.AddColumn<int>("status").AddIndex(x =>
        {
            x.Name = "idx_child_open_status";
            x.Predicate = "[status]>5";
        });
        child.AddColumn<Guid>("parent_id").ForeignKeyTo(parent, "id");
        database.AddTable(child);

        var idList = new TableType(new SqlServerObjectName(SchemaName, "ChildIdList"));
        idList.AddColumn<Guid>("ID");
        database.Add(idList);

        database.Add(new StoredProcedure(new SqlServerObjectName(SchemaName, "uspDeleteChildren"), @"
CREATE OR ALTER PROCEDURE gh593.uspDeleteChildren
    @IDLIST gh593.ChildIdList READONLY
AS

    DELETE FROM gh593.child WHERE id IN (SELECT ID FROM @IDLIST);
"));

        return database;
    }

    /// <summary>
    ///     Renders exactly what <c>Migrator.WriteMigrationFileAsync</c> writes, in the same order:
    ///     schema creation first, then every update.
    /// </summary>
    private static string renderScript(MigrationDatabase database, SchemaMigration migration)
    {
        var writer = new StringWriter();

        database.Migrator.WriteScript(writer, (rules, w) =>
        {
            if (migration.Schemas.Any())
            {
                rules.WriteSchemaCreationSql(migration.Schemas, w);
            }

            migration.WriteAllUpdates(w, rules, AutoCreate.All);
        });

        return writer.ToString();
    }

    private static async Task assertMigrationFileMatches(
        MigrationDatabase database,
        SchemaMigration migration,
        string script
    )
    {
        var directory = Path.Combine(Path.GetTempPath(), "weasel-gh593-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(directory);

        try
        {
            var filename = Path.Combine(directory, "migration.sql");
            await database.Migrator.WriteMigrationFileAsync(filename, migration);

            var written = await File.ReadAllTextAsync(filename);
            written.ShouldBe(script);
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }

    /// <summary>
    ///     The procedure definition has to be a batch of its own: SQL Server requires
    ///     <c>CREATE OR ALTER PROCEDURE</c> to be the first statement of its batch, and the whole
    ///     point of <c>OR ALTER</c> is that a bare <c>CREATE PROCEDURE</c> cannot run twice.
    /// </summary>
    private static void assertProcedureIsBatchSeparated(string script)
    {
        var lines = script.ReadLines().Select(x => x.Trim()).ToArray();

        var definition = Array.FindIndex(lines,
            x => x.StartsWith("CREATE OR ALTER PROCEDURE", StringComparison.OrdinalIgnoreCase));
        definition.ShouldBeGreaterThan(0);

        // The authored body opens with a newline, so "immediately before" means the nearest
        // preceding line that is not blank.
        var opening = Array.FindLastIndex(lines, definition - 1, x => x.IsNotEmpty());
        lines[opening].ShouldBe("GO");

        var closing = Array.FindIndex(lines, definition + 1, x => x.EqualsIgnoreCase("GO"));
        closing.ShouldBeGreaterThan(definition);

        // The closing separator comes after the whole definition, not after its first line.
        lines[definition..closing].Join(Environment.NewLine).ShouldContain("DELETE FROM gh593.child");

        Regex.IsMatch(script, @"CREATE\s+PROCEDURE", RegexOptions.IgnoreCase).ShouldBeFalse();
    }

    private async Task executeInBatches(string script)
    {
        foreach (var batch in SqlServerBatchSplitter.Split(script))
        {
            await theConnection.CreateCommand(batch).ExecuteNonQueryAsync();
        }
    }
}

/// <summary>
///     A <see cref="DatabaseWithTables" /> that also carries the schema objects that are not tables,
///     so one migration can cover a table type and a stored procedure alongside them.
/// </summary>
internal class MigrationDatabase: DatabaseWithTables
{
    private readonly List<ISchemaObject> _others = new();

    public MigrationDatabase(string identifier, string connectionString): base(identifier, connectionString)
    {
    }

    public void Add(ISchemaObject schemaObject)
    {
        _others.Add(schemaObject);
    }

    // The tables come first so the procedure and the type it takes are created against tables that
    // already exist.
    public override IFeatureSchema[] BuildFeatureSchemas()
        => [..base.BuildFeatureSchemas(), new OtherObjectsFeatureSchema(Migrator, _others)];

    private class OtherObjectsFeatureSchema(Migrator migrator, List<ISchemaObject> others)
        : FeatureSchemaBase("Others", migrator)
    {
        protected override IEnumerable<ISchemaObject> schemaObjects() => others;
    }
}
