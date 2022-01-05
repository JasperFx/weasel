using System.IO;

namespace Weasel.Core
{
    /// <summary>
    /// Models the difference between a configured ISchemaObject and the actual
    /// database version of that object
    /// </summary>
    public interface ISchemaObjectDelta
    {
        /// <summary>
        /// The subject of this delta
        /// </summary>
        ISchemaObject SchemaObject { get; }
        SchemaPatchDifference Difference { get; }

        /// <summary>
        /// Write the SQL to make incremental changes to the existing object
        /// in the database to make it match the as desired configuration
        /// </summary>
        /// <param name="rules"></param>
        /// <param name="writer"></param>
        void WriteUpdate(Migrator rules, TextWriter writer);

        /// <summary>
        /// Write the necessary SQL to rollback any incremental changes to the
        /// existing object in this delta
        /// </summary>
        /// <param name="rules"></param>
        /// <param name="writer"></param>
        void WriteRollback(Migrator rules, TextWriter writer);

        /// <summary>
        /// Only used to express the current state in the database for an object when
        /// Weasel is unable to execute the detected changes
        /// </summary>
        /// <param name="rules"></param>
        /// <param name="writer"></param>
        void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer);
    }
}
