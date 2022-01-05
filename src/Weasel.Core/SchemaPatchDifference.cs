namespace Weasel.Core
{
    public enum SchemaPatchDifference
    {
        /// <summary>
        /// No differences between expected and actual database structure
        /// </summary>
        None = 3,

        /// <summary>
        /// Configured schema objects are missing in the existing database structure
        /// </summary>
        Create = 2,

        /// <summary>
        /// There are detected differences between the expected configuration of a schema object and
        /// the existing database
        /// </summary>
        Update = 1,

        /// <summary>
        /// The existing database object in incompatible somehow with the expected configuration and Weasel
        /// cannot determine the update SQL to bridge the difference
        /// </summary>
        Invalid = 0
    }
}
