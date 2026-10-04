namespace Weasel.Core.Migrations;

/// <summary>
///     Schema objects that reference something a creation script also has to create first, where
///     the dependency is not a foreign key. Foreign keys are read off <see cref="ITable" /> and need
///     no opt-in.
/// </summary>
/// <remarks>
///     weasel#677. A trigger whose body writes to another table, a view selecting from one, a
///     function referencing one — none of these is a foreign key, and all of them have to exist in
///     dependency order in a script that runs top to bottom. Declaring the edge here is what keeps
///     the ordering out of the consumer's yield order.
/// </remarks>
public interface ISchemaObjectWithDependencies : ISchemaObject
{
    /// <summary>
    ///     Names of objects this one has to be created after. A name this script does not create is
    ///     ignored, so declaring a dependency on something external is harmless.
    /// </summary>
    IEnumerable<DbObjectName> DependsOn { get; }
}

/// <summary>
///     Orders schema objects and features so that a creation script written top to bottom creates
///     referenced objects before the objects referencing them.
/// </summary>
/// <remarks>
///     <para>
///         weasel#677. The migration path has resolved this for the caller since
///         <see cref="SchemaMigration" /> began deferring foreign keys to tables created later in
///         the same migration. The script path wrote objects in yield order, so the same model could
///         migrate cleanly and produce a creation script that failed on its first constraint —
///         <c>Foreign key '…' references invalid table '…'</c>. Consumers were left to re-derive the
///         ordering themselves (JasperFx/polecat#684), and nothing warned them that yield order was
///         load-bearing in one path and not the other.
///     </para>
///     <para>
///         <b>A cycle is left in encounter order rather than refused.</b> Two tables referencing each
///         other have no valid creation order at all, so there is nothing to sort them into. Refusing
///         would reject a configuration the migration path handles fine, by deferring one of the keys
///         — and on SQLite even that is impossible, since it has no
///         <c>ALTER TABLE … ADD CONSTRAINT</c> and a foreign key can only be declared inline at
///         table creation. So the sort does what it can and leaves the rest where it found it, which
///         is the same deliberate non-handling Polecat's own sort documents.
///     </para>
/// </remarks>
public static class SchemaObjectOrdering
{
    /// <summary>
    ///     The same objects, reordered so that each one follows everything in the set it depends on.
    ///     Stable: objects with no dependency relationship keep their original relative order, so a
    ///     script's shape only changes where it had to.
    /// </summary>
    public static IReadOnlyList<ISchemaObject> InDependencyOrder(IReadOnlyList<ISchemaObject> objects)
    {
        if (objects.Count < 2)
        {
            return objects;
        }

        // Keyed on QualifiedName with an OrdinalIgnoreCase comparer rather than on DbObjectName
        // itself: DbObjectName.Equals compares QualifiedName ignoring case, but GetHashCode hashes
        // it ordinally, so two names differing only in case are equal yet land in different buckets.
        var byName = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < objects.Count; i++)
        {
            var name = objects[i].Identifier?.QualifiedName;
            if (name != null)
            {
                byName.TryAdd(name, i);
            }
        }

        var ordered = new List<ISchemaObject>(objects.Count);

        // 0 = untouched, 1 = on the current path, 2 = already placed. The on-the-path state is what
        // detects a cycle: re-entering a node still being visited means following an edge back into
        // the path, and that edge is the one we drop.
        var state = new byte[objects.Count];

        for (var i = 0; i < objects.Count; i++)
        {
            visit(i, objects, byName, state, ordered);
        }

        return ordered;
    }

    private static void visit(int index, IReadOnlyList<ISchemaObject> objects, Dictionary<string, int> byName,
        byte[] state, List<ISchemaObject> ordered)
    {
        if (state[index] != 0)
        {
            return;
        }

        state[index] = 1;

        foreach (var dependency in dependenciesOf(objects[index]))
        {
            if (byName.TryGetValue(dependency.QualifiedName, out var target) && target != index)
            {
                visit(target, objects, byName, state, ordered);
            }
        }

        state[index] = 2;
        ordered.Add(objects[index]);
    }

    /// <summary>
    ///     What this object has to be created after. A table's foreign keys need no opt-in; anything
    ///     else declares itself through <see cref="ISchemaObjectWithDependencies" />.
    /// </summary>
    private static IEnumerable<DbObjectName> dependenciesOf(ISchemaObject schemaObject)
    {
        if (schemaObject is ITable table)
        {
            foreach (var fk in table.ForeignKeys)
            {
                // A self-referencing key is satisfied by the table's own creation, so it is not an
                // ordering constraint and must not look like one
                if (fk.LinkedTable != null && !fk.LinkedTable.Equals(table.Identifier))
                {
                    yield return fk.LinkedTable;
                }
            }
        }

        if (schemaObject is ISchemaObjectWithDependencies declared)
        {
            foreach (var name in declared.DependsOn)
            {
                yield return name;
            }
        }
    }

    /// <summary>
    ///     The same features, reordered so that each one follows every feature it depends on.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Needed as well as the per-object sort because both script paths are written a feature
    ///         at a time, and one of them —
    ///         <c>WriteScriptsByTypeAsync</c> — puts each feature in its own file. Sorting inside a
    ///         feature cannot help a table whose referenced table belongs to a different one.
    ///     </para>
    ///     <para>
    ///         Edges come from where the objects actually point, not only from the declared
    ///         <see cref="IFeatureSchema.DependentTypes" />: that collection is documented as
    ///         controlling "the proper ordering of object creation or scripting" but was only ever
    ///         read by the lazy per-feature storage path, and a consumer who never declared it had
    ///         nothing but dictionary order. Declared types are honoured too, so a dependency that is
    ///         not expressible as an object reference still orders.
    ///     </para>
    /// </remarks>
    public static IReadOnlyList<IFeatureSchema> InDependencyOrder(IReadOnlyList<IFeatureSchema> features)
    {
        if (features.Count < 2)
        {
            return features;
        }

        var featureOfObject = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var featureOfType = new Dictionary<Type, int>();

        for (var i = 0; i < features.Count; i++)
        {
            featureOfType.TryAdd(features[i].StorageType, i);

            foreach (var schemaObject in features[i].Objects)
            {
                var name = schemaObject.Identifier?.QualifiedName;
                if (name != null)
                {
                    featureOfObject.TryAdd(name, i);
                }
            }
        }

        var ordered = new List<IFeatureSchema>(features.Count);
        var state = new byte[features.Count];

        for (var i = 0; i < features.Count; i++)
        {
            visitFeature(i, features, featureOfObject, featureOfType, state, ordered);
        }

        return ordered;
    }

    private static void visitFeature(int index, IReadOnlyList<IFeatureSchema> features,
        Dictionary<string, int> featureOfObject, Dictionary<Type, int> featureOfType, byte[] state,
        List<IFeatureSchema> ordered)
    {
        if (state[index] != 0)
        {
            return;
        }

        state[index] = 1;

        foreach (var target in dependentFeatures(features[index], featureOfObject, featureOfType))
        {
            if (target != index)
            {
                visitFeature(target, features, featureOfObject, featureOfType, state, ordered);
            }
        }

        state[index] = 2;
        ordered.Add(features[index]);
    }

    private static IEnumerable<int> dependentFeatures(IFeatureSchema feature,
        Dictionary<string, int> featureOfObject, Dictionary<Type, int> featureOfType)
    {
        foreach (var schemaObject in feature.Objects)
        {
            foreach (var dependency in dependenciesOf(schemaObject))
            {
                if (featureOfObject.TryGetValue(dependency.QualifiedName, out var target))
                {
                    yield return target;
                }
            }
        }

        foreach (var dependentType in feature.DependentTypes())
        {
            if (featureOfType.TryGetValue(dependentType, out var target))
            {
                yield return target;
            }
        }
    }
}
