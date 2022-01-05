using System;

namespace Weasel.Core.Migrations
{
    /// <summary>
    /// Thrown when the actual database configuration does not match the expected configuration
    /// </summary>
    public class DatabaseValidationException: Exception
    {
        public DatabaseValidationException(string ddl)
            : base("Configuration to Schema Validation Failed! These changes detected:\n\n" + ddl)
        {
        }

        public DatabaseValidationException(string databaseName, string ddl)
            : base($"Configuration to Schema Validation for Database '{databaseName}' Failed! These changes detected:\n\n" + ddl)
        {
        }

#if SERIALIZE
        protected DatabaseValidationException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
#endif
    }
}
