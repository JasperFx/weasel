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

        // sqlcmd is the one client that leaves QUOTED_IDENTIFIER off, and a filtered index cannot be
        // created while it is. The header is what makes the file runnable there with no extra flags.
        script.ShouldStartWith("SET QUOTED_IDENTIFIER ON;");

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

    /// <summary>
    ///     The update path rather than the create path: an existing table gains a column and an index
    ///     on it, and the rendered update script has to run twice. Unguarded, the second run fails on
    ///     the column ("Column names in each table must be unique") before it reaches the index.
    /// </summary>
    [Fact]
    public async Task the_rendered_update_script_runs_twice_and_matches_the_configuration()
    {
        await buildDatabase().ApplyAllConfiguredChangesToDatabaseAsync();

        var database = buildDatabase(child =>
        {
            child.AddColumn<string>("nickname").AddIndex(x => x.Name = "idx_child_nickname");
            child.AddColumn<int>("rank").NotNull().DefaultValue(0);
        });

        var migration = await database.CreateMigrationAsync();
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        var script = renderScript(database, migration);
        script.ShouldContain("IF COL_LENGTH(N'gh593.child', N'nickname') IS NULL");

        await executeInBatches(script);
        await executeInBatches(script);

        await database.AssertDatabaseMatchesConfigurationAsync();
    }

    /// <summary>
    ///     The rest of the update path: a primary key added to a table that had none, a primary key
    ///     whose columns (and so its name) changed, a dropped column that carried a default, a new
    ///     check constraint and a new table type. Every one of these raised on a second run.
    /// </summary>
    [Fact]
    public async Task the_rendered_update_script_for_keys_and_constraints_runs_twice()
    {
        await buildKeysDatabase(updated: false).ApplyAllConfiguredChangesToDatabaseAsync();

        var database = buildKeysDatabase(updated: true);
        var migration = await database.CreateMigrationAsync();
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        var script = renderScript(database, migration);

        await executeInBatches(script);
        await executeInBatches(script);

        await database.AssertDatabaseMatchesConfigurationAsync();
    }

    /// <summary>
    ///     The same changes rolled back, twice: the actual primary keys go back on, the dropped column
    ///     is re-added, and the table type the update created is dropped again.
    /// </summary>
    [Fact]
    public async Task the_rendered_rollback_script_for_keys_and_constraints_runs_twice()
    {
        var initial = buildKeysDatabase(updated: false);
        await initial.ApplyAllConfiguredChangesToDatabaseAsync();

        var database = buildKeysDatabase(updated: true);
        var migration = await database.CreateMigrationAsync();

        await executeInBatches(renderScript(database, migration));

        var writer = new StringWriter();
        database.Migrator.WriteScript(writer, (rules, w) => migration.WriteAllRollbacks(w, rules));
        var rollback = writer.ToString();

        await executeInBatches(rollback);
        await executeInBatches(rollback);

        await initial.AssertDatabaseMatchesConfigurationAsync();
    }

    /// <summary>
    ///     The guards look the table up by name, so a name that needs delimiting has to be delimited
    ///     inside the literal too. Undelimited, <c>OBJECT_ID(N'gh593.a.b')</c> is NULL: the column
    ///     guard then fails open (and the second run raises), but the key-column nullability guard fails
    ///     closed, skipping the <c>NOT NULL</c> alter on the first run so the key add after it fails.
    /// </summary>
    [Fact]
    public async Task the_guards_find_a_table_whose_name_has_a_dot()
    {
        await buildDottedDatabase(updated: false).ApplyAllConfiguredChangesToDatabaseAsync();

        var database = buildDottedDatabase(updated: true);
        var migration = await database.CreateMigrationAsync();
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        var script = renderScript(database, migration);

        await executeInBatches(script);
        await executeInBatches(script);

        await database.AssertDatabaseMatchesConfigurationAsync();
    }

    private static MigrationDatabase buildDottedDatabase(bool updated)
    {
        var database = new MigrationDatabase("gh593", ConnectionSource.ConnectionString);

        var dotted = new Table(new SqlServerObjectName(SchemaName, "a.b"));
        var id = dotted.AddColumn<int>("id");
        if (updated)
        {
            id.AsPrimaryKey();
            dotted.AddColumn<string>("nickname");
        }

        database.AddTable(dotted);

        return database;
    }

    private static MigrationDatabase buildKeysDatabase(bool updated)
    {
        var database = new MigrationDatabase("gh593", ConnectionSource.ConnectionString);

        // no primary key at first, then one
        var keyless = new Table(new SqlServerObjectName(SchemaName, "keyless"));
        var keylessId = keyless.AddColumn<int>("id");
        if (updated) keylessId.AsPrimaryKey();
        database.AddTable(keyless);

        // the key widens to a second column, which renames it
        var widened = new Table(new SqlServerObjectName(SchemaName, "widened"));
        widened.AddColumn<int>("id").AsPrimaryKey();
        var code = widened.AddColumn<int>("code");
        if (updated) code.AsPrimaryKey();
        database.AddTable(widened);

        var trimmed = new Table(new SqlServerObjectName(SchemaName, "trimmed"));
        trimmed.AddColumn<int>("id").AsPrimaryKey();
        if (updated)
        {
            trimmed.CheckConstraints.Add(new TableCheckConstraint("ck_trimmed_id", "id > 0"));
        }
        else
        {
            trimmed.AddColumn<int>("legacy").DefaultValue(5);
        }

        database.AddTable(trimmed);

        if (updated)
        {
            var idList = new TableType(new SqlServerObjectName(SchemaName, "TrimmedIdList"));
            idList.AddColumn<int>("ID");
            database.Add(idList);
        }

        return database;
    }

    private static MigrationDatabase buildDatabase(Action<Table>? alterChild = null)
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
        alterChild?.Invoke(child);
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
    ///     creation of the changed schemas first, then every update.
    /// </summary>
    private static string renderScript(MigrationDatabase database, SchemaMigration migration)
    {
        var writer = new StringWriter();

        database.Migrator.WriteScript(writer, (rules, w) =>
        {
            if (migration.ChangedSchemas.Any())
            {
                rules.WriteSchemaCreationSql(migration.ChangedSchemas, w);
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
