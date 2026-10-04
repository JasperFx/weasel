namespace Weasel.Core.Migrations;

public static class FeatureSchemaExtensions
{
    /// <summary>
    ///     Write the creation SQL for an entire feature, in dependency order.
    /// </summary>
    /// <remarks>
    ///     weasel#677. This used to write <see cref="IFeatureSchema.Objects" /> in yield order, which
    ///     made the order a feature happened to return its objects in load-bearing: a table yielded
    ///     before the table its foreign key references emitted a constraint against something that
    ///     did not exist yet, and the script failed on its first constraint. The migration path has
    ///     never had that problem, so nothing warned the caller that the two paths disagreed. See
    ///     <see cref="SchemaObjectOrdering" />, including why a mutual reference is still left alone.
    /// </remarks>
    /// <param name="schema"></param>
    /// <param name="rules"></param>
    /// <param name="writer"></param>
    public static void WriteFeatureCreation(this IFeatureSchema schema, Migrator rules, TextWriter writer)
    {
        foreach (var schemaObject in SchemaObjectOrdering.InDependencyOrder(schema.Objects))
            schemaObject.WriteCreateStatement(rules, writer);

        schema.WritePermissions(rules, writer);
    }
}
