using System.Data.Common;
using JasperFx.Core;

namespace Weasel.Core;

/// <summary>
///     A detected change between desired database configuration
///     and the actual state of the database
/// </summary>
public class SchemaMigration
{
    private readonly List<ISchemaObjectDelta> _deltas;

    public SchemaMigration(IEnumerable<ISchemaObjectDelta> deltas)
    {
        _deltas = new List<ISchemaObjectDelta>(deltas);
        Schemas = _deltas.SelectMany(x => x.SchemaObject.AllNames())
            .Select(x => x.Schema)
            .Where(x => x != "public")
            .Distinct().ToArray();

        if (_deltas.Any())
        {
            Difference = _deltas.Min(x => x.Difference);
        }
    }

    public SchemaMigration(ISchemaObjectDelta delta): this(new[] { delta })
    {
    }

    public IReadOnlyList<ISchemaObjectDelta> Deltas => _deltas;

    /// <summary>
    ///     The unique schemas part of this migration
    /// </summary>
    public string[] Schemas { get; }

    /// <summary>
    ///     The detected difference between configuration and the actual database
    /// </summary>
    public SchemaPatchDifference Difference { get; } = SchemaPatchDifference.None;

    /// <summary>
    ///     Create a SchemaMigration for the supplied connection and array of schema
    ///     objects
    /// </summary>
    /// <param name="conn"></param>
    /// <param name="schemaObjects"></param>
    /// <returns></returns>
    public static Task<SchemaMigration> DetermineAsync(
        DbConnection conn,
        params ISchemaObject[] schemaObjects
    ) =>
        DetermineAsync(conn, default, schemaObjects);

    /// <summary>
    ///     Create a SchemaMigration for the supplied connection and array of schema
    ///     objects
    /// </summary>
    /// <param name="conn"></param>
    /// <param name="schemaObjects"></param>
    /// <returns></returns>
    public static async Task<SchemaMigration> DetermineAsync(
        DbConnection conn,
        CancellationToken ct,
        params ISchemaObject[] schemaObjects
    )
    {
        var deltas = new List<ISchemaObjectDelta>();

        if (!schemaObjects.Any())
        {
            return new SchemaMigration(deltas);
        }

        var builder = new DbCommandBuilder(conn);

        foreach (var schemaObject in schemaObjects) schemaObject.ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);

        deltas.Add(await schemaObjects[0].CreateDeltaAsync(reader, ct).ConfigureAwait(false));

        for (var i = 1; i < schemaObjects.Length; i++)
        {
            await reader.NextResultAsync(ct).ConfigureAwait(false);
            deltas.Add(await schemaObjects[i].CreateDeltaAsync(reader, ct).ConfigureAwait(false));
        }

        try
        {
            await reader.CloseAsync().ConfigureAwait(false);
            await reader.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // It's aggravating, but there's an issue w/ postgresql's metadata query on partitions that can throw here
        }

        foreach (var postProcessing in deltas.OfType<ISchemaObjectDeltaWithPostProcessing>().ToArray())
        {
            postProcessing.PostProcess(deltas);
        }

        return new SchemaMigration(deltas);
    }


    /// <summary>
    ///     Writes all the necessary SQL statements to update the actual database to the expected configuration
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="rules"></param>
    /// <param name="autoCreate"></param>
    public void WriteAllUpdates(TextWriter writer, Migrator rules, AutoCreate autoCreate)
    {
        AssertPatchingIsValid(autoCreate);
        foreach (var delta in _deltas)
        {
            switch (delta.Difference)
            {
                case SchemaPatchDifference.None:
                    break;

                case SchemaPatchDifference.Create:
                    delta.SchemaObject.WriteCreateStatement(rules, writer);
                    break;

                case SchemaPatchDifference.Update:
                    delta.WriteUpdate(rules, writer);
                    break;

                case SchemaPatchDifference.Invalid:
                    delta.SchemaObject.WriteDropStatement(rules, writer);
                    delta.SchemaObject.WriteCreateStatement(rules, writer);
                    break;
            }
        }
    }

    /// <summary>
    ///     Writes all the necessary SQL statements to rollback the actual database to the initial state if
    ///     this migration has already been applied
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="rules"></param>
    public void WriteAllRollbacks(TextWriter writer, Migrator rules)
    {
        foreach (var delta in _deltas)
        {
            switch (delta.Difference)
            {
                case SchemaPatchDifference.None:
                    continue;

                case SchemaPatchDifference.Create:
                    delta.SchemaObject.WriteDropStatement(rules, writer);
                    break;

                case SchemaPatchDifference.Update:
                    delta.WriteRollback(rules, writer);
                    break;

                case SchemaPatchDifference.Invalid:
                    delta.SchemaObject.WriteDropStatement(rules, writer);
                    delta.WriteRestorationOfPreviousState(rules, writer);
                    break;
            }
        }
    }

    public static string ToDropFileName(string updateFile)
    {
        var containingFolder = updateFile.ParentDirectory();
        var rawFileName = Path.GetFileNameWithoutExtension(updateFile);
        var ext = Path.GetExtension(updateFile);

        var dropFile = $"{rawFileName}.drop{ext}";

        return containingFolder.IsEmpty() ? dropFile : containingFolder.AppendPath(dropFile);
    }

    /// <summary>
    ///     Assert that this migration can be applied based on the supplied
    ///     autoCreate threshold
    /// </summary>
    /// <param name="autoCreate"></param>
    /// <exception cref="SchemaMigrationException"></exception>
    public void AssertPatchingIsValid(AutoCreate autoCreate)
    {
        if (autoCreate == AutoCreate.All)
        {
            return;
        }

        if (Difference == SchemaPatchDifference.None)
        {
            return;
        }

        if (Difference == SchemaPatchDifference.Invalid)
        {
            var invalids = _deltas.Where(x => x.Difference == SchemaPatchDifference.Invalid);
            throw new SchemaMigrationException(autoCreate, invalids);
        }

        switch (autoCreate)
        {
            case AutoCreate.None:
            case AutoCreate.CreateOrUpdate:
                return;

            case AutoCreate.CreateOnly:
                if (Difference != SchemaPatchDifference.Create)
                {
                    var invalids = _deltas.Where(x => x.Difference < SchemaPatchDifference.Create);
                    throw new SchemaMigrationException(autoCreate, invalids);
                }

                break;
        }
    }

    /// <summary>
    ///     Apply all the rollback steps from this migration to the supplied database connection
    /// </summary>
    /// <param name="conn"></param>
    /// <param name="rules"></param>
    /// <returns></returns>
    public Task RollbackAllAsync(DbConnection conn, Migrator rules, CancellationToken ct = default)
    {
        var writer = new StringWriter();
        WriteAllRollbacks(writer, rules);

        return conn
            .CreateCommand(writer.ToString())
            .ExecuteNonQueryAsync(ct);
    }
}
