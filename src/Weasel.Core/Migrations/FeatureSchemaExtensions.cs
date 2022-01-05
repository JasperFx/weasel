using System.IO;

namespace Weasel.Core.Migrations
{
    public static class FeatureSchemaExtensions
    {
        /// <summary>
        /// Write the creation SQL for an entire feature
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="rules"></param>
        /// <param name="writer"></param>
        public static void WriteFeatureCreation(this IFeatureSchema schema, Migrator rules, TextWriter writer)
        {
            foreach (var schemaObject in schema.Objects) schemaObject.WriteCreateStatement(rules, writer);

            schema.WritePermissions(rules, writer);
        }
    }
}
