using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using Weasel.Core.Migrations;

namespace Weasel.Core;

/// <summary>
///     Governs the rules and formatting for generating SQL exports of schema objects
/// </summary>
public abstract class Migrator
{
    public static readonly string SCHEMA = "%SCHEMA%";
    public static readonly string TABLENAME = "%TABLENAME%";
    public static readonly string FUNCTION = "%FUNCTION%";
    public static readonly string SIGNATURE = "%SIGNATURE%";
    public static readonly string COLUMNS = "%COLUMNS%";
    public static readonly string NON_ID_COLUMNS = "%NON_ID_COLUMNS%";
    public static readonly string METADATA_COLUMNS = "%METADATA_COLUMNS%";

    public readonly LightweightCache<string, SqlTemplate> Templates = new(name => new SqlTemplate(name));

    protected Migrator(string defaultSchemaName)
    {
        DefaultSchemaName = defaultSchemaName;
    }

    /// <summary>
    ///     Should all generated DDL files be written with transactional semantics
    ///     so that everything succeeds or everything fails together
    /// </summary>
    public bool IsTransactional { get; set; } = true;

    public SqlFormatting Formatting { get; set; } = SqlFormatting.Pretty;

    /// <summary>
    ///     Alters the syntax used to create tables in DDL
    /// </summary>
    public CreationStyle TableCreation { get; set; } = CreationStyle.DropThenCreate;

    /// <summary>
    ///     Alters the user rights for the upsert functions in DDL
    /// </summary>
    public SecurityRights UpsertRights { get; set; } = SecurityRights.Invoker;

    /// <summary>
    ///     Option to use this database role during DDL scripts
    /// </summary>
    public string? Role { get; set; }

    public string DefaultSchemaName { get; }

    /// <summary>
    ///     Read [name].table and [name].function files from the named directory
    ///     to serve as templates for extra DDL (GRANT's probably)
    /// </summary>
    /// <param name="directory"></param>
    public async Task ReadTemplatesAsync(string directory, CancellationToken ct = default)
    {
        foreach (var file in FileSystem.FindFiles(directory, FileSet.Shallow("*.function")))
        {
            var name = Path.GetFileNameWithoutExtension(file).ToLower();
            Templates[name].FunctionCreation = await File.ReadAllTextAsync(file, ct).ConfigureAwait(false);
        }

        foreach (var file in FileSystem.FindFiles(directory, FileSet.Shallow("*.table")))
        {
            var name = Path.GetFileNameWithoutExtension(file).ToLower();

            Templates[name].TableCreation = await File.ReadAllTextAsync(file, ct).ConfigureAwait(false);
        }
    }


    /// <summary>
    ///     Write templated SQL to the supplied file name
    /// </summary>
    /// <param name="filename"></param>
    /// <param name="writeStep"></param>
    public async Task WriteTemplatedFile(string filename, Action<Migrator, TextWriter> writeStep,
        CancellationToken ct = default)
    {
        await using var stream = new FileStream(filename, FileMode.Create);
        var writer = new StreamWriter(stream) { AutoFlush = true };

        WriteScript(writer, writeStep);

        await stream.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     Write out a templated SQL script with all rules
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="writeStep">A continuation to write the inner SQL</param>
    public abstract void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep);

    /// <summary>
    ///     Writes the necessary SQL to create the supplied schema names
    /// </summary>
    /// <param name="schemaNames"></param>
    /// <param name="writer"></param>
    public abstract void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer);

    /// <summary>
    ///     Apply all the differences in the supplied SchemaMigration to the supplied database connection
    /// </summary>
    /// <param name="conn"></param>
    /// <param name="migration"></param>
    /// <param name="autoCreate"></param>
    /// <param name="logger"></param>
    public Task ApplyAllAsync(
        DbConnection conn,
        SchemaMigration migration,
        AutoCreate autoCreate,
        IMigrationLogger? logger = null,
        CancellationToken ct = default
    )
    {
        if (autoCreate == AutoCreate.None)
        {
            return Task.CompletedTask;
        }

        if (migration.Difference == SchemaPatchDifference.None)
        {
            return Task.CompletedTask;
        }

        if (!migration.Deltas.Any())
        {
            return Task.CompletedTask;
        }

        migration.AssertPatchingIsValid(autoCreate);

        logger ??= new DefaultMigrationLogger();
        return executeDelta(migration, conn, autoCreate, logger, ct);
    }

    protected abstract Task executeDelta(
        SchemaMigration migration,
        DbConnection conn,
        AutoCreate autoCreate,
        IMigrationLogger logger,
        CancellationToken ct = default
    );

    /// <summary>
    ///     Write the SQL updates for a single delta object to the TextWriter
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="delta"></param>
    /// <returns></returns>
    public bool WriteUpdate(TextWriter writer, ISchemaObjectDelta delta)
    {
        switch (delta.Difference)
        {
            case SchemaPatchDifference.None:
                return false;

            case SchemaPatchDifference.Create:
                delta.SchemaObject.WriteCreateStatement(this, writer);
                return true;

            case SchemaPatchDifference.Update:
                delta.WriteUpdate(this, writer);
                return true;

            case SchemaPatchDifference.Invalid:
                delta.SchemaObject.WriteDropStatement(this, writer);
                delta.SchemaObject.WriteCreateStatement(this, writer);
                return true;
        }

        return false;
    }

    /// <summary>
    ///     Create a line in a SQL statement to execute the given script file name. Used in
    ///     generating database creation SQL by feature
    /// </summary>
    /// <param name="scriptName"></param>
    /// <returns></returns>
    public abstract string ToExecuteScriptLine(string scriptName);

    /// <summary>
    ///     Write the delta "up" and matching "down" file at the same location
    /// </summary>
    /// <param name="filename"></param>
    /// <param name="migration"></param>
    public async Task WriteMigrationFileAsync(string filename, SchemaMigration patch, CancellationToken ct = default)
    {
        if (!Path.IsPathRooted(filename))
        {
            filename = AppContext.BaseDirectory.AppendPath(filename);
        }

        await WriteTemplatedFile(filename, (r, w) =>
        {
            patch.WriteAllUpdates(w, r, AutoCreate.All);
        }, ct).ConfigureAwait(false);

        var dropFile = SchemaMigration.ToDropFileName(filename);
        await WriteTemplatedFile(dropFile, (r, w) =>
        {
            patch.WriteAllRollbacks(w, r);
        }, ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     Throw an exception if the supplied name is an invalid identifier for
    ///     this database engine
    /// </summary>
    /// <param name="name"></param>
    public abstract void AssertValidIdentifier(string name);
}
