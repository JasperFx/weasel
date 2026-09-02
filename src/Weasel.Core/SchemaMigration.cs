using System.Data.Common;
using JasperFx;
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

        deferForeignKeysToTablesCreatedLater();
    }

    private void deferForeignKeysToTablesCreatedLater()
    {
        if (!_deltas.Any(x => x is ISchemaObjectDeltaWithDeferrableForeignKeys))
        {
            return;
        }

        // Keyed on QualifiedName with an OrdinalIgnoreCase comparer rather than on DbObjectName itself:
        // DbObjectName.Equals compares QualifiedName ignoring case, but GetHashCode hashes it ordinally,
        // so two names differing only in case are equal yet land in different buckets.
        var creations = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < _deltas.Count; i++)
        {
            if (_deltas[i].Difference != SchemaPatchDifference.Create)
            {
                continue;
            }

            var identifier = _deltas[i].SchemaObject?.Identifier?.QualifiedName;
            if (identifier != null)
            {
                creations.TryAdd(identifier, i);
            }
        }

        if (creations.Count == 0)
        {
            return;
        }

        for (var i = 0; i < _deltas.Count; i++)
        {
            if (_deltas[i] is not ISchemaObjectDeltaWithDeferrableForeignKeys delta)
            {
                continue;
            }

            foreach (var (name, linkedTable) in delta.ForeignKeysToCreate)
            {
                if (creations.TryGetValue(linkedTable.QualifiedName, out var created) && created > i)
                {
                    delta.DeferForeignKey(name);
                }
            }
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
    ///     The budget assumed for a <see cref="Migrator" /> that reports no limit of its own. SQL
    ///     Server's hard limit of 2100 is the tightest of the five, so it is what an unknown provider
    ///     is assumed to have.
    /// </summary>
    public const int DefaultParameterBudget = 2000;

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
    /// <remarks>
    ///     One command, as it has always been. Without a <see cref="Migrator" /> there is no way to
    ///     know the provider's parameter limit, and guessing at one would change how many round trips
    ///     every existing caller makes. Pass the migrator, or an explicit budget, to batch.
    /// </remarks>
    public static Task<SchemaMigration> DetermineAsync(
        DbConnection conn,
        CancellationToken ct,
        params ISchemaObject[] schemaObjects
    ) => DetermineAsync(conn, new DbCommandBuilder(conn), ct, schemaObjects);

    /// <summary>
    ///     Create a SchemaMigration, batching the introspection queries so that no single command
    ///     binds more than <paramref name="parameterBudget" /> parameters.
    /// </summary>
    /// <param name="parameterBudget">
    ///     The most parameters one introspection command may carry. Use
    ///     <see cref="Migrator.MaxParametersPerCommand" /> when a migrator is available.
    /// </param>
    public static Task<SchemaMigration> DetermineAsync(
        DbConnection conn,
        CancellationToken ct,
        int parameterBudget,
        params ISchemaObject[] schemaObjects
    ) => determineBatchedAsync(conn, () => new DbCommandBuilder(conn), parameterBudget, ct, schemaObjects);

    /// <summary>
    ///     Create a SchemaMigration using the dialect's own command builder and parameter limit.
    /// </summary>
    /// <remarks>
    ///     This is what the migration path uses. It is the only overload that gets both halves
    ///     right at once: the builder from <see cref="Migrator.CreateCommandBuilder" />, which is
    ///     what lets Oracle chain a reader across split statements, and the batching limit from
    ///     <see cref="Migrator.MaxParametersPerCommand" />, so a database with more objects than one
    ///     command can bind is inspected over several round trips instead of failing.
    /// </remarks>
    public static Task<SchemaMigration> DetermineAsync(
        DbConnection conn,
        Migrator migrator,
        CancellationToken ct,
        params ISchemaObject[] schemaObjects
    ) => determineBatchedAsync(conn, () => migrator.CreateCommandBuilder(conn),
        migrator.MaxParametersPerCommand, ct, schemaObjects);

    /// <summary>
    ///     Create a SchemaMigration using a command builder the caller supplies.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Only Oracle needs this: ODP.NET will not execute several statements from a single
    ///         command, so <c>OracleDbCommandBuilder</c> splits the batch and the reader chains across
    ///         the pieces. Without it every Oracle schema object was limited to one introspection query,
    ///         which is why index, foreign key and primary key drift was invisible to the whole
    ///         migration path (weasel#474). Callers reach it through
    ///         <see cref="Migrator.CreateCommandBuilder" />.
    ///     </para>
    ///     <para>
    ///         One builder is one command, so this overload cannot batch — every object's parameters
    ///         land in the builder that was handed in. Pass the <see cref="Migrator" /> instead when
    ///         the object count is unbounded.
    ///     </para>
    /// </remarks>
    public static async Task<SchemaMigration> DetermineAsync(
        DbConnection conn,
        DbCommandBuilder builder,
        CancellationToken ct,
        params ISchemaObject[] schemaObjects
    )
    {
        var deltas = new List<ISchemaObjectDelta>();

        if (!schemaObjects.Any())
        {
            return new SchemaMigration(deltas);
        }

        await determineBatchAsync(conn, builder, schemaObjects, deltas, ct).ConfigureAwait(false);

        postProcess(deltas);

        return new SchemaMigration(deltas);
    }

    private static async Task<SchemaMigration> determineBatchedAsync(
        DbConnection conn,
        Func<DbCommandBuilder> newBuilder,
        int parameterBudget,
        CancellationToken ct,
        ISchemaObject[] schemaObjects
    )
    {
        var deltas = new List<ISchemaObjectDelta>();

        if (!schemaObjects.Any())
        {
            return new SchemaMigration(deltas);
        }

        // Priced against a throwaway builder, which costs no round trip: the parameter count an
        // object binds is a property of its query, not of the builder it is handed.
        int cost(ISchemaObject schemaObject)
        {
            var probe = new DbCommandBuilder(conn);
            schemaObject.ConfigureQueryCommand(probe);
            using var command = probe.Command;
            return command.Parameters.Count;
        }

        foreach (var batch in BatchByParameterBudget(schemaObjects, cost, parameterBudget))
        {
            await determineBatchAsync(conn, newBuilder(), batch, deltas, ct).ConfigureAwait(false);
        }

        // Once over the complete delta set rather than per batch, so a delta can still look across
        // objects that landed in different batches.
        postProcess(deltas);

        return new SchemaMigration(deltas);
    }

    /// <summary>
    ///     Group the objects into batches whose combined parameter count stays within the budget.
    /// </summary>
    /// <remarks>
    ///     Every provider but Oracle concatenates a whole batch into one command, so a large enough
    ///     database binds more parameters than the driver will accept. SQL Server refuses a request
    ///     carrying more than 2100, and a table's query binds two, so past about a thousand tables
    ///     migration failed outright with "The incoming request has too many parameters" before any
    ///     comparison happened.
    /// </remarks>
    /// <param name="parameterCost">
    ///     What one object binds. Separate from the batching itself so the arithmetic can be
    ///     exercised without a database.
    /// </param>
    internal static IEnumerable<ISchemaObject[]> BatchByParameterBudget(
        ISchemaObject[] schemaObjects,
        Func<ISchemaObject, int> parameterCost,
        int parameterBudget
    )
    {
        // A Migrator subclass that does not answer -- a test double, most often -- would otherwise
        // put every object in a batch of its own and turn one round trip into hundreds.
        var budget = parameterBudget > 0 ? parameterBudget : DefaultParameterBudget;

        var current = new List<ISchemaObject>();
        var used = 0;

        foreach (var schemaObject in schemaObjects)
        {
            var cost = parameterCost(schemaObject);

            // current.Count is what keeps an object costing more than the entire budget from
            // yielding an empty batch forever; it goes into a batch of its own instead.
            if (current.Count > 0 && used + cost > budget)
            {
                yield return current.ToArray();
                current.Clear();
                used = 0;
            }

            current.Add(schemaObject);
            used += cost;
        }

        if (current.Count > 0)
        {
            yield return current.ToArray();
        }
    }

    private static async Task determineBatchAsync(
        DbConnection conn,
        DbCommandBuilder builder,
        ISchemaObject[] schemaObjects,
        List<ISchemaObjectDelta> deltas,
        CancellationToken ct
    )
    {
        for (var i = 0; i < schemaObjects.Length; i++)
        {
            // Between objects, not before the first: a builder that splits on this boundary would
            // otherwise open with an empty statement.
            if (i > 0) builder.StartNewCommand();

            schemaObjects[i].ConfigureQueryCommand(builder);
        }

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
    }

    private static void postProcess(List<ISchemaObjectDelta> deltas)
    {
        foreach (var postProcessing in deltas.OfType<ISchemaObjectDeltaWithPostProcessing>().ToArray())
        {
            postProcessing.PostProcess(deltas);
        }
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
            rules.WriteUpdate(writer, delta);
        }

        WriteDeferredForeignKeys(writer, rules);
    }

    public void WriteDeferredForeignKeys(TextWriter writer, Migrator rules)
    {
        foreach (var delta in _deltas.OfType<ISchemaObjectDeltaWithDeferrableForeignKeys>())
        {
            if (!delta.HasDeferredForeignKeys)
            {
                continue;
            }

            delta.WriteDeferredForeignKeys(rules, writer);
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
            // Only the deltas that are genuinely stuck. A delta that can rebuild in place is not:
            // both apply paths below -- WriteAllUpdates and Migrator.WriteUpdate -- answer such a
            // delta by calling its WriteUpdate rather than dropping and recreating (weasel#477), so
            // refusing it here rejects a migration the machinery would have carried out correctly
            // and with the data intact (weasel#538).
            //
            // It is still Invalid, so it still fails the CreateOnly check below. A rebuild recreates
            // an object that is already there, which is an update whichever way you look at it.
            var invalids = _deltas
                .Where(x => x.Difference == SchemaPatchDifference.Invalid)
                .Where(x => x is not ISchemaObjectDeltaWithRebuild { CanRebuildInPlace: true })
                .ToArray();

            if (invalids.Any())
            {
                throw new SchemaMigrationException(autoCreate, invalids);
            }
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
