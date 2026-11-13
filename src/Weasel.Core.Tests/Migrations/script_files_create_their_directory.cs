using JasperFx;
using JasperFx.Descriptors;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Core.Tests.Migrations;

/// <summary>
///     <c>db-patch</c> and <c>db-dump</c> are handed a path on the command line, and a path that names
///     a folder nobody has made yet is the usual case, not the odd one. Each writer makes the folders
///     its file needs rather than failing with a <see cref="DirectoryNotFoundException" />.
/// </summary>
public class script_files_create_their_directory: IDisposable
{
    private readonly string _root =
        Path.Combine(Path.GetTempPath(), "weasel-script-files", Guid.NewGuid().ToString("N"));

    /// <summary>
    ///     A relative folder one level deep, as in <c>db-patch migrations/patch.sql</c>. Its directory
    ///     part has no separator in it, which is the case <c>FileSystem.CreateDirectoryIfNotExists</c>
    ///     silently skips, so it needs a test of its own: an absolute path always has a separator.
    /// </summary>
    private readonly string _relative = "weasel-script-files-" + Guid.NewGuid().ToString("N");

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, true);
        }

        if (Directory.Exists(_relative))
        {
            Directory.Delete(_relative, true);
        }
    }

    /// <summary>
    ///     <c>db-patch</c> itself roots a relative path at <see cref="AppContext.BaseDirectory" /> before it
    ///     gets here, but <see cref="Migrator.WriteTemplatedFile" /> is public and is what
    ///     <c>db-dump --by-feature</c> writes through, so it takes a relative path as given.
    /// </summary>
    [Fact]
    public async Task a_templated_file_creates_a_relative_directory()
    {
        var filename = Path.Combine(_relative, "patch.sql");

        await new Sqlite.SqliteMigrator().WriteTemplatedFile(filename, (_, w) => w.WriteLine("select 1;"));

        File.Exists(filename).ShouldBeTrue();
    }

    [Fact]
    public async Task a_creation_script_creates_a_relative_directory()
    {
        var filename = Path.Combine(_relative, "dump.sql");

        await new ScriptedDatabase().WriteCreationScriptToFileAsync(filename);

        File.ReadAllText(filename).ShouldContain("CREATE TABLE");
    }

    [Fact]
    public async Task scripts_by_type_create_a_relative_directory()
    {
        await new ScriptedDatabase().WriteScriptsByTypeAsync(_relative);

        File.Exists(Path.Combine(_relative, "things.sql")).ShouldBeTrue();
        File.Exists(Path.Combine(_relative, "all.sql")).ShouldBeTrue();
    }

    /// <summary>
    ///     What <c>db-patch</c> writes: the patch, and the drop file beside it.
    /// </summary>
    [Fact]
    public async Task a_migration_file_creates_its_directory()
    {
        var filename = Path.Combine(_root, "nested", "deeper", "patch.sql");
        var migration = new SchemaMigration([
            new SchemaObjectDelta(new Things(), SchemaPatchDifference.Create)
        ]);

        await new Sqlite.SqliteMigrator().WriteMigrationFileAsync(filename, migration);

        File.Exists(filename).ShouldBeTrue();
        File.Exists(SchemaMigration.ToDropFileName(filename)).ShouldBeTrue();
    }

    /// <summary>
    ///     What <c>db-dump</c> writes by default: one file for the whole database.
    /// </summary>
    [Fact]
    public async Task a_creation_script_creates_its_directory()
    {
        var filename = Path.Combine(_root, "nested", "deeper", "dump.sql");

        await new ScriptedDatabase().WriteCreationScriptToFileAsync(filename);

        File.ReadAllText(filename).ShouldContain("CREATE TABLE");
    }

    /// <summary>
    ///     What <c>db-dump --by-feature</c> writes: a folder of files, one per feature.
    /// </summary>
    [Fact]
    public async Task scripts_by_type_create_their_directory()
    {
        var directory = Path.Combine(_root, "nested", "deeper");

        await new ScriptedDatabase().WriteScriptsByTypeAsync(directory);

        File.Exists(Path.Combine(directory, "things.sql")).ShouldBeTrue();
        File.Exists(Path.Combine(directory, "all.sql")).ShouldBeTrue();
    }

    private sealed class Things: ISchemaObject
    {
        public DbObjectName Identifier { get; } = new("main", "things");

        public void WriteCreateStatement(Migrator migrator, TextWriter writer) =>
            writer.WriteLine("CREATE TABLE things (id INTEGER);");

        public void WriteDropStatement(Migrator rules, TextWriter writer) =>
            writer.WriteLine("DROP TABLE IF EXISTS things;");

        public void ConfigureQueryCommand(DbCommandBuilder builder) => throw new NotSupportedException();

        public Task<ISchemaObjectDelta> CreateDeltaAsync(System.Data.Common.DbDataReader reader,
            CancellationToken ct = default) => throw new NotSupportedException();

        public IEnumerable<DbObjectName> AllNames() => [Identifier];
    }

    private sealed class ScriptedDatabase(): DatabaseBase<SqliteConnection>(new DefaultMigrationLogger(),
        AutoCreate.None, new Sqlite.SqliteMigrator(), "scripted", "Data Source=:memory:")
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
